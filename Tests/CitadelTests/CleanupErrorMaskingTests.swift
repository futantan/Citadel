@testable import Citadel
import XCTest

/// Covers the cleanup shape shared by `withPTY`, `withTTY`, and `withExec`
/// (`SSHClient.performThenClose`): a failure inside the body must be rethrown
/// as-is even when the cleanup `close()` also fails — on a dead connection
/// `channel.close()` throws `ChannelError.alreadyClosed`, which must not mask
/// the original stream-termination error (e.g. `SSHSessionEndedWithoutExitStatus`).
final class CleanupErrorMaskingTests: XCTestCase {
    private struct BodyError: Error {}
    private struct CloseError: Error {}

    func testBodyErrorNotMaskedByFailingCleanupClose() async {
        var closeCalls = 0
        do {
            try await SSHClient.performThenClose(
                perform: { throw BodyError() },
                close: {
                    closeCalls += 1
                    throw CloseError()
                }
            )
            XCTFail("performThenClose should rethrow the body error")
        } catch {
            XCTAssert(error is BodyError, "Original body error was masked by the cleanup close error; got \(error)")
        }
        XCTAssertEqual(closeCalls, 1, "Cleanup close should still be attempted exactly once")
    }

    func testBodyErrorRethrownWhenCloseSucceeds() async {
        var closeCalls = 0
        do {
            try await SSHClient.performThenClose(
                perform: { throw BodyError() },
                close: { closeCalls += 1 }
            )
            XCTFail("performThenClose should rethrow the body error")
        } catch {
            XCTAssert(error is BodyError, "Expected the body error, got \(error)")
        }
        XCTAssertEqual(closeCalls, 1)
    }

    func testSuccessPathStillSurfacesCloseError() async {
        // On the success path close() is not cleanup after a failure;
        // its error must keep surfacing to the caller.
        do {
            try await SSHClient.performThenClose(
                perform: {},
                close: { throw CloseError() }
            )
            XCTFail("close error on the success path must surface")
        } catch {
            XCTAssert(error is CloseError, "Expected the close error, got \(error)")
        }
    }
}
