@testable import Citadel
import Logging
import NIO
@preconcurrency import NIOSSH
import XCTest

/// Covers the termination classification of `_executeCommandStream`'s output
/// stream: how `.eof(nil)` (channel torn down without a transport error) ends the
/// stream depending on whether an exit-status arrived and on the command mode.
///
/// The handler under test is built by `SSHClient.makeExecOutputHandler` — the
/// exact production closure `_executeCommandStream` installs — and is driven
/// through an `EmbeddedChannel`, so no live SSH session is needed.
@available(macOS 15.0, *)
final class PTYTerminationSemanticsTests: XCTestCase {

    // MARK: - Helpers

    private enum StreamEnd {
        case clean
        case failed(Error)
    }

    private static func ptyMode() -> SSHClient.CommandMode {
        .pty(
            SSHChannelRequestEvent.PseudoTerminalRequest(
                wantReply: true,
                term: "xterm",
                terminalCharacterWidth: 80,
                terminalRowHeight: 24,
                terminalPixelWidth: 0,
                terminalPixelHeight: 0,
                terminalModes: .init([:])
            ),
            command: nil
        )
    }

    /// Runs the production output handler on an `EmbeddedChannel`, fires the
    /// given exit-status (if any), closes the channel — which delivers
    /// `.eof(nil)` via `handlerRemoved`, the shape of a connection-level
    /// termination without a transport error — and reports how the stream ended.
    private func endOfStream(
        mode: SSHClient.CommandMode,
        exitStatus: Int?
    ) async throws -> StreamEnd {
        let (stream, continuation) = AsyncThrowingStream<ExecCommandOutput, Error>.makeStream()
        let handler = SSHClient.makeExecOutputHandler(
            logger: Logger(label: "citadel.tests.pty-termination"),
            mode: mode,
            continuation: continuation
        )

        let channel = EmbeddedChannel(handler: handler)
        if let exitStatus {
            channel.pipeline.fireUserInboundEventTriggered(
                SSHChannelRequestEvent.ExitStatus(exitStatus: exitStatus)
            )
        }
        _ = try channel.finish()

        do {
            for try await _ in stream {}
            return .clean
        } catch {
            return .failed(error)
        }
    }

    // MARK: - Tests

    func testPTYStreamEndingWithoutExitStatusThrowsNamedError() async throws {
        let end = try await endOfStream(mode: Self.ptyMode(), exitStatus: nil)

        guard case .failed(let error) = end else {
            XCTFail("PTY stream that ended without an exit-status finished cleanly; expected SSHSessionEndedWithoutExitStatus")
            return
        }
        XCTAssert(
            error is SSHSessionEndedWithoutExitStatus,
            "Expected SSHSessionEndedWithoutExitStatus, got \(error)"
        )
    }

    func testPTYStreamAfterExitZeroFinishesCleanly() async throws {
        let end = try await endOfStream(mode: Self.ptyMode(), exitStatus: 0)

        guard case .clean = end else {
            XCTFail("PTY stream with exit-status 0 should finish cleanly, got \(end)")
            return
        }
    }

    func testPTYStreamAfterNonZeroExitThrowsCommandFailed() async throws {
        let end = try await endOfStream(mode: Self.ptyMode(), exitStatus: 1)

        guard case .failed(let error) = end else {
            XCTFail("PTY stream with exit-status 1 should throw CommandFailed")
            return
        }
        guard let commandFailed = error as? SSHClient.CommandFailed else {
            XCTFail("Expected CommandFailed, got \(error)")
            return
        }
        XCTAssertEqual(commandFailed.exitCode, 1)
    }

    func testExecCommandStreamWithoutExitStatusThrowsNamedError() async throws {
        // Issue #411: `.command` (exec) shares the `.pty` semantics — EOF without
        // an exit-status means the connection was cut, not a clean completion.
        let end = try await endOfStream(mode: .command("true"), exitStatus: nil)

        guard case .failed(let error) = end else {
            XCTFail("Exec-mode stream that ended without an exit-status finished cleanly; expected SSHSessionEndedWithoutExitStatus")
            return
        }
        XCTAssert(
            error is SSHSessionEndedWithoutExitStatus,
            "Expected SSHSessionEndedWithoutExitStatus, got \(error)"
        )
    }

    func testExecCommandStreamAfterExitZeroFinishesCleanly() async throws {
        let end = try await endOfStream(mode: .command("true"), exitStatus: 0)

        guard case .clean = end else {
            XCTFail("Exec-mode stream with exit-status 0 should finish cleanly, got \(end)")
            return
        }
    }

    func testExecCommandStreamAfterNonZeroExitThrowsCommandFailed() async throws {
        let end = try await endOfStream(mode: .command("false"), exitStatus: 1)

        guard case .failed(let error) = end, let commandFailed = error as? SSHClient.CommandFailed else {
            XCTFail("Exec-mode stream with exit-status 1 should throw CommandFailed, got \(end)")
            return
        }
        XCTAssertEqual(commandFailed.exitCode, 1)
    }

    func testTTYStreamWithoutExitStatusThrowsNamedError() async throws {
        let end = try await endOfStream(mode: .tty(command: "true"), exitStatus: nil)

        guard case .failed(let error) = end else {
            XCTFail("TTY-mode stream that ended without an exit-status finished cleanly; expected SSHSessionEndedWithoutExitStatus")
            return
        }
        XCTAssert(
            error is SSHSessionEndedWithoutExitStatus,
            "Expected SSHSessionEndedWithoutExitStatus, got \(error)"
        )
    }

    func testTTYStreamAfterExitZeroFinishesCleanly() async throws {
        let end = try await endOfStream(mode: .tty(command: "true"), exitStatus: 0)

        guard case .clean = end else {
            XCTFail("TTY-mode stream with exit-status 0 should finish cleanly, got \(end)")
            return
        }
    }

    func testTTYStreamAfterNonZeroExitThrowsCommandFailed() async throws {
        let end = try await endOfStream(mode: .tty(command: "false"), exitStatus: 1)

        guard case .failed(let error) = end, let commandFailed = error as? SSHClient.CommandFailed else {
            XCTFail("TTY-mode stream with exit-status 1 should throw CommandFailed, got \(end)")
            return
        }
        XCTAssertEqual(commandFailed.exitCode, 1)
    }
}
