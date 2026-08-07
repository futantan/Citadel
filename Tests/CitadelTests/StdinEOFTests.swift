@testable import Citadel
import NIO
@preconcurrency import NIOSSH
import XCTest
import Foundation

/// Covers `TTYStdinWriter.sendEOF()`, the half-close that lets a client finish
/// stdin while the exec channel stays open for the command's answer.
@available(macOS 15.0, *)
final class StdinEOFTests: XCTestCase {

    // MARK: - Helpers

    private struct TestTimeout: Error {}

    /// A server-side command that answers only after the client half-closes stdin.
    ///
    /// This is the shape that hangs without EOF: the delegate collects stdin and
    /// writes nothing until `inputClosed()` arrives.
    private final class EOFAwaitingExec: ExecDelegate, @unchecked Sendable {
        private let lock = NSLock()
        private var _inputClosedCount = 0
        private var _stdin = Data()

        var inputClosedCount: Int { lock.withLock { _inputClosedCount } }
        var stdin: Data { lock.withLock { _stdin } }

        final class Ctx: ExecCommandContext {
            let owner: EOFAwaitingExec
            let outputHandler: ExecOutputHandler

            init(owner: EOFAwaitingExec, outputHandler: ExecOutputHandler) {
                self.owner = owner
                self.outputHandler = outputHandler
            }

            func terminate() async throws {}

            func inputClosed() async throws {
                let isFirst = owner.lock.withLock { () -> Bool in
                    owner._inputClosedCount += 1
                    return owner._inputClosedCount == 1
                }
                guard isFirst else { return }

                let handler = outputHandler
                DispatchQueue.global().async {
                    let output = handler.stdoutPipe.fileHandleForWriting
                    output.write(Data("answered after EOF".utf8))
                    try? output.close()
                    // Let NIO drain the pipe before succeed triggers pipeChannel.close
                    Thread.sleep(forTimeInterval: 0.3)
                    handler.succeed(exitCode: 0)
                }
            }
        }

        func setEnvironmentValue(_ value: String, forKey key: String) async throws {}

        func start(command: String, outputHandler: ExecOutputHandler) async throws -> ExecCommandContext {
            let input = outputHandler.stdinPipe.fileHandleForReading
            input.readabilityHandler = { [weak self] handle in
                let data = handle.availableData
                guard !data.isEmpty, let self else { return }
                self.lock.withLock { self._stdin.append(data) }
            }
            return Ctx(owner: self, outputHandler: outputHandler)
        }
    }

    private func runTest(
        timeout: Duration = .seconds(5),
        perform: @escaping (SSHServer, SSHClient) async throws -> Void
    ) async throws {
        let authDelegate = AuthDelegate(supportedAuthenticationMethods: .password) { request, promise in
            switch request.request {
            case .password(.init(password: "test")) where request.username == "citadel":
                promise.succeed(.success)
            default:
                promise.succeed(.failure)
            }
        }
        let server = try await SSHServer.host(
            host: "localhost",
            port: 0,
            hostKeys: [NIOSSHPrivateKey(p521Key: .init())],
            authenticationDelegate: authDelegate
        )

        let port = try XCTUnwrap(server.channel.localAddress?.port)

        let client = try await SSHClient.connect(
            host: "localhost",
            port: port,
            authenticationMethod: .passwordBased(username: "citadel", password: "test"),
            hostKeyValidator: .acceptAnything(),
            reconnect: .never
        )

        defer {
            Task { try? await server.close() }
        }

        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await perform(server, client)
                }
                group.addTask {
                    try await Task.sleep(for: timeout)
                    throw TestTimeout()
                }
                try await group.next()
                group.cancelAll()
            }
        } catch is TestTimeout {
            XCTFail("Test timed out after \(timeout)")
        } catch let error as ChannelError where error == .alreadyClosed {
            // Server-initiated channel close can race with client close in withExec
        }

        do {
            try await client.close()
        } catch let error as ChannelError where error == .alreadyClosed {
            // Already cleaned up
        }
    }

    // MARK: - Tests

    /// The half-close reaches the server as `inputClosed()`, and the channel is
    /// still readable afterwards, so the command's answer arrives.
    func testSendEOFHalfClosesStdinAndKeepsReading() async throws {
        try await runTest { server, client in
            let execDelegate = EOFAwaitingExec()
            server.enableExec(withDelegate: execDelegate)

            try await client.withExec("await-eof") { inbound, outbound in
                try await outbound.write(ByteBuffer(string: "request payload"))
                try await outbound.sendEOF()

                var collected = ByteBuffer()
                for try await chunk in inbound {
                    if case .stdout(let buf) = chunk {
                        collected.writeImmutableBuffer(buf)
                    }
                }
                XCTAssertEqual(String(buffer: collected), "answered after EOF")
            }

            XCTAssertEqual(execDelegate.inputClosedCount, 1)
            XCTAssertEqual(String(data: execDelegate.stdin, encoding: .utf8), "request payload")
        }
    }

    /// Repeat calls are no-ops: RFC 4254 allows one EOF per channel, and a second
    /// frame on the wire is a protocol violation that would tear the session down.
    func testSendEOFIsIdempotent() async throws {
        try await runTest { server, client in
            let execDelegate = EOFAwaitingExec()
            server.enableExec(withDelegate: execDelegate)

            try await client.withExec("await-eof") { inbound, outbound in
                try await outbound.write(ByteBuffer(string: "once"))
                try await outbound.sendEOF()
                try await outbound.sendEOF()
                try await outbound.sendEOF()

                var collected = ByteBuffer()
                for try await chunk in inbound {
                    if case .stdout(let buf) = chunk {
                        collected.writeImmutableBuffer(buf)
                    }
                }
                XCTAssertEqual(String(buffer: collected), "answered after EOF")
            }

            XCTAssertEqual(execDelegate.inputClosedCount, 1)
        }
    }

    /// Writing after the half-close is a programmer error, reported rather than
    /// silently dropped.
    func testWriteAfterSendEOFFails() async throws {
        try await runTest { server, client in
            let execDelegate = EOFAwaitingExec()
            server.enableExec(withDelegate: execDelegate)

            try await client.withExec("await-eof") { inbound, outbound in
                try await outbound.sendEOF()

                do {
                    try await outbound.write(ByteBuffer(string: "too late"))
                    XCTFail("Expected the write after EOF to fail")
                } catch let error as ChannelError {
                    XCTAssertEqual(error, .outputClosed)
                }

                for try await _ in inbound {}
            }
        }
    }

    /// On a channel the remote already tore down there is nothing to half-close,
    /// and the caller is told so instead of believing stdin was finished.
    func testSendEOFOnClosedChannelThrows() async throws {
        final class ImmediateExec: ExecDelegate, @unchecked Sendable {
            struct Ctx: ExecCommandContext {
                func terminate() async throws {}
            }
            func setEnvironmentValue(_ value: String, forKey key: String) async throws {}
            func start(command: String, outputHandler: ExecOutputHandler) async throws -> ExecCommandContext {
                DispatchQueue.global().async {
                    try? outputHandler.stdoutPipe.fileHandleForWriting.close()
                    Thread.sleep(forTimeInterval: 0.3)
                    outputHandler.succeed(exitCode: 0)
                }
                return Ctx()
            }
        }

        try await runTest { server, client in
            server.enableExec(withDelegate: ImmediateExec())

            try await client.withExec("exit-now") { inbound, outbound in
                // Draining to the end means the remote has closed the channel.
                for try await _ in inbound {}

                do {
                    try await outbound.sendEOF()
                    XCTFail("Expected sendEOF on a closed channel to fail")
                } catch let error as ChannelError {
                    XCTAssertEqual(error, .alreadyClosed)
                }
            }
        }
    }
}
