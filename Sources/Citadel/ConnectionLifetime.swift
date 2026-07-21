import NIO
import NIOConcurrencyHelpers

final class ConnectionLifetime: @unchecked Sendable {
    private enum State {
        case pending(Channel?)
        case handedOff
        case finished(Channel?)
    }

    private let state = NIOLockedValueBox<State>(.pending(nil))

    func publish(_ channel: Channel) -> Bool {
        let accepted = state.withLockedValue { state in
            switch state {
            case .pending(nil):
                state = .pending(channel)
                return true
            case .pending(.some), .handedOff:
                preconditionFailure("A connection lifetime may publish only one channel")
            case .finished(nil):
                state = .finished(channel)
                return false
            case .finished(.some):
                return false
            }
        }
        if !accepted {
            channel.close(promise: nil)
        }
        return accepted
    }

    func handOff(unlessCancelled isCancelled: () -> Bool) -> Bool {
        state.withLockedValue { state in
            switch state {
            case .pending(let channel?):
                guard !isCancelled() else {
                    state = .finished(channel)
                    return false
                }
                state = .handedOff
                return true
            case .finished:
                return false
            case .pending(nil), .handedOff:
                preconditionFailure("A connection requires one published channel before handoff")
            }
        }
    }

    func finish() -> Channel? {
        state.withLockedValue { state in
            switch state {
            case .pending(let channel):
                state = .finished(channel)
                return channel
            case .finished(let channel):
                return channel
            case .handedOff:
                return nil
            }
        }
    }

    func cancel() {
        finish()?.close(promise: nil)
    }
}
