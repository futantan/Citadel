@testable import Citadel
import Crypto
import NIO
import NIOConcurrencyHelpers
import NIOEmbedded
@preconcurrency import NIOSSH
import XCTest

final class AuthenticationLifecycleTests: XCTestCase {
    func testSettingsExposeAuthenticationLifecycleConfiguration() {
        let callback: @Sendable (String, String) -> Void = { _, _ in }
        var settings = makeSettings(port: 22)

        XCTAssertEqual(settings.authenticationTimeout, .seconds(10))
        XCTAssertNil(settings.onUserAuthenticationBanner)

        settings.authenticationTimeout = .minutes(5)
        settings.onUserAuthenticationBanner = callback

        XCTAssertEqual(settings.authenticationTimeout, .minutes(5))
        XCTAssertNotNil(settings.onUserAuthenticationBanner)
    }

    func testHandshakeForwardsBannerAndSuccessEvents() throws {
        final class EventRecorder: ChannelInboundHandler {
            typealias InboundIn = Any
            var events: [Any] = []

            func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
                events.append(event)
                context.fireUserInboundEventTriggered(event)
            }
        }

        let channel = EmbeddedChannel()
        let recorder = EventRecorder()
        let banners = NIOLockedValueBox<[(String, String)]>([])
        let handler = ClientHandshakeHandler(
            eventLoop: channel.eventLoop,
            authenticationTimeout: .seconds(10),
            onUserAuthenticationBanner: { message, languageTag in
                banners.withLockedValue { $0.append((message, languageTag)) }
            }
        )
        try channel.pipeline.addHandlers(handler, recorder).wait()

        channel.pipeline.fireUserInboundEventTriggered(
            NIOUserAuthBannerEvent(message: "check", languageTag: "en")
        )
        channel.pipeline.fireUserInboundEventTriggered(UserAuthSuccessEvent())
        try handler.authenticated.wait()

        XCTAssertEqual(banners.withLockedValue { $0.map(\.0) }, ["check"])
        XCTAssertEqual(banners.withLockedValue { $0.map(\.1) }, ["en"])
        XCTAssertEqual(recorder.events.count, 2)
        XCTAssertTrue(recorder.events[0] is NIOUserAuthBannerEvent)
        XCTAssertTrue(recorder.events[1] is UserAuthSuccessEvent)
        XCTAssertNoThrow(try channel.finish())
    }

    func testHandshakeCompletesOnlyOnceWhenErrorRacesWithSuccess() throws {
        struct TestError: Error, Equatable {}
        final class ErrorSink: ChannelInboundHandler {
            typealias InboundIn = Any
            func errorCaught(context: ChannelHandlerContext, error: any Error) {}
        }

        let channel = EmbeddedChannel()
        let handler = ClientHandshakeHandler(
            eventLoop: channel.eventLoop,
            authenticationTimeout: .seconds(10),
            onUserAuthenticationBanner: nil
        )
        try channel.pipeline.addHandlers(handler, ErrorSink()).wait()

        channel.pipeline.fireErrorCaught(TestError())
        channel.pipeline.fireUserInboundEventTriggered(UserAuthSuccessEvent())

        XCTAssertThrowsError(try handler.authenticated.wait()) { error in
            XCTAssertEqual(error as? TestError, TestError())
        }
        XCTAssertNoThrow(try channel.finish())
    }

    func testHandshakeFailsWhenChannelBecomesInactive() throws {
        let channel = EmbeddedChannel()
        let handler = ClientHandshakeHandler(
            eventLoop: channel.eventLoop,
            authenticationTimeout: .seconds(10),
            onUserAuthenticationBanner: nil
        )
        try channel.pipeline.addHandler(handler).wait()

        try channel.close().wait()

        XCTAssertThrowsError(try handler.authenticated.wait()) { error in
            XCTAssertEqual(error as? ChannelError, .eof)
        }
        XCTAssertThrowsError(try channel.finish())
    }

    func testBannerArrivesBeforeSuccessfulConnectAndTimeoutIsCancelled() async throws {
        let server = try await BannerSSHServer.start()
        let banners = NIOLockedValueBox<[(String, String)]>([])
        var settings = makeSettings(port: server.port)
        settings.authenticationTimeout = .milliseconds(50)
        settings.onUserAuthenticationBanner = { message, languageTag in
            banners.withLockedValue { $0.append((message, languageTag)) }
        }

        let client = try await SSHClient.connect(to: settings)

        XCTAssertEqual(banners.withLockedValue { $0.map(\.0) }, ["check-url"])
        XCTAssertEqual(banners.withLockedValue { $0.map(\.1) }, ["en"])
        try await Task.sleep(for: .milliseconds(100))
        XCTAssertTrue(client.isConnected)
        XCTAssertEqual(server.acceptedCount.withLockedValue { $0 }, 1)

        try await client.close()
        try await server.close()
    }

    func testConfiguredAuthenticationTimeoutClosesConnection() async throws {
        let server = try await HangingTCPServer.start()
        var settings = makeSettings(port: server.port)
        settings.authenticationTimeout = .milliseconds(50)

        do {
            _ = try await SSHClient.connect(to: settings)
            XCTFail("Connection should time out during authentication")
        } catch {
            XCTAssertEqual(error as? ChannelError, .connectTimeout(.milliseconds(50)))
        }

        let accepted = try await server.accepted.get()
        XCTAssertFalse(accepted.isActive)
        try await accepted.closeFuture.get()
        try await server.close()
    }

    func testCancellationClosesConnectionAndThrowsCancellationError() async throws {
        let server = try await HangingTCPServer.start()
        var settings = makeSettings(port: server.port)
        settings.authenticationTimeout = .seconds(30)

        let task = Task { try await SSHClient.connect(to: settings) }
        let accepted = try await server.accepted.get()
        task.cancel()

        do {
            _ = try await task.value
            XCTFail("Cancelled connection should not succeed")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }

        XCTAssertFalse(accepted.isActive)
        try await accepted.closeFuture.get()
        try await server.close()
    }

    private func makeSettings(port: Int) -> SSHClientSettings {
        SSHClientSettings(
            host: "127.0.0.1",
            port: port,
            authenticationMethod: {
                .passwordBased(username: "citadel", password: "test")
            },
            hostKeyValidator: .acceptAnything()
        )
    }
}

private struct BannerSSHServer {
    struct AuthenticationDelegate: NIOSSHServerUserAuthenticationDelegate {
        let supportedAuthenticationMethods: NIOSSHAvailableUserAuthenticationMethods = .password

        func requestReceived(
            request: NIOSSHUserAuthenticationRequest,
            responsePromise: EventLoopPromise<NIOSSHUserAuthenticationOutcome>
        ) {
            responsePromise.succeed(.success)
        }
    }

    let channel: Channel
    let group: MultiThreadedEventLoopGroup
    let acceptedCount: NIOLockedValueBox<Int>

    var port: Int { channel.localAddress!.port! }

    static func start() async throws -> Self {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let acceptedCount = NIOLockedValueBox(0)
        let hostKey = NIOSSHPrivateKey(p521Key: .init())
        let channel = try await ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                acceptedCount.withLockedValue { $0 += 1 }
                let configuration = SSHServerConfiguration(
                    hostKeys: [hostKey],
                    userAuthDelegate: AuthenticationDelegate(),
                    banner: .init(message: "check-url", languageTag: "en")
                )
                return channel.pipeline.addHandler(
                    NIOSSHHandler(
                        role: .server(configuration),
                        allocator: channel.allocator,
                        inboundChildChannelInitializer: nil
                    )
                )
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        return Self(channel: channel, group: group, acceptedCount: acceptedCount)
    }

    func close() async throws {
        try await channel.close().get()
        try await shutdown(group)
    }
}

private struct HangingTCPServer {
    let channel: Channel
    let accepted: EventLoopFuture<Channel>
    let group: MultiThreadedEventLoopGroup

    var port: Int { channel.localAddress!.port! }

    static func start() async throws -> Self {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let accepted = group.next().makePromise(of: Channel.self)
        let channel = try await ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                accepted.succeed(channel)
                return channel.eventLoop.makeSucceededVoidFuture()
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        return Self(
            channel: channel,
            accepted: accepted.futureResult,
            group: group
        )
    }

    func close() async throws {
        try await channel.close().get()
        try await shutdown(group)
    }
}

private func shutdown(_ group: MultiThreadedEventLoopGroup) async throws {
    try await withCheckedThrowingContinuation {
        (continuation: CheckedContinuation<Void, any Error>) in
        group.shutdownGracefully { error in
            if let error {
                continuation.resume(throwing: error)
            } else {
                continuation.resume()
            }
        }
    }
}
