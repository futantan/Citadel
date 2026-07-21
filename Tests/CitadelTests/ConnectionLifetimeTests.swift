@testable import Citadel
import NIO
import NIOEmbedded
import XCTest

final class ConnectionLifetimeTests: XCTestCase {
    func testCancellationBeforePublicationClosesLaterChannel() throws {
        let lifetime = ConnectionLifetime()
        let channel = try makeActiveChannel()

        lifetime.cancel()

        XCTAssertFalse(lifetime.publish(channel))
        runClose(on: channel)
        XCTAssertFalse(channel.isActive)
    }

    func testCancellationVisibleAtHandoffClosesChannel() throws {
        let lifetime = ConnectionLifetime()
        let channel = try makeActiveChannel()
        XCTAssertTrue(lifetime.publish(channel))

        XCTAssertFalse(lifetime.handOff(unlessCancelled: { true }))
        lifetime.cancel()

        runClose(on: channel)
        XCTAssertFalse(channel.isActive)
    }

    func testSuccessfulHandoffKeepsChannelOpenAfterLateCancellation() throws {
        let lifetime = ConnectionLifetime()
        let channel = try makeActiveChannel()
        XCTAssertTrue(lifetime.publish(channel))

        XCTAssertTrue(lifetime.handOff(unlessCancelled: { false }))
        lifetime.cancel()
        channel.embeddedEventLoop.run()

        XCTAssertTrue(channel.isActive)
        XCTAssertNoThrow(try channel.finish())
    }

    private func runClose(on channel: EmbeddedChannel) {
        channel.embeddedEventLoop.run()
        XCTAssertNoThrow(try channel.closeFuture.wait())
    }

    private func makeActiveChannel() throws -> EmbeddedChannel {
        let channel = EmbeddedChannel()
        channel.connect(
            to: try SocketAddress(ipAddress: "127.0.0.1", port: 22),
            promise: nil
        )
        channel.embeddedEventLoop.run()
        XCTAssertTrue(channel.isActive)
        return channel
    }
}
