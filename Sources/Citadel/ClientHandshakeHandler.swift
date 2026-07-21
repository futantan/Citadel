import NIO
import NIOConcurrencyHelpers
@preconcurrency import NIOSSH

final class ClientHandshakeHandler: ChannelInboundHandler, Sendable {
    typealias InboundIn = Any

    private let completion: AuthenticationCompletion
    private let authenticationTimeout: TimeAmount
    private let onUserAuthenticationBanner:
        (@Sendable (_ message: String, _ languageTag: String) -> Void)?

    var authenticated: EventLoopFuture<Void> {
        completion.futureResult
    }

    init(
        eventLoop: EventLoop,
        authenticationTimeout: TimeAmount,
        onUserAuthenticationBanner:
            (@Sendable (_ message: String, _ languageTag: String) -> Void)?
    ) {
        self.completion = AuthenticationCompletion(eventLoop: eventLoop)
        self.authenticationTimeout = authenticationTimeout
        self.onUserAuthenticationBanner = onUserAuthenticationBanner
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive {
            completion.scheduleTimeout(authenticationTimeout)
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        completion.scheduleTimeout(authenticationTimeout)
        context.fireChannelActive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let banner = event as? NIOUserAuthBannerEvent {
            onUserAuthenticationBanner?(banner.message, banner.languageTag)
        } else if event is UserAuthSuccessEvent {
            completion.succeed()
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        completion.fail(error)
        context.fireErrorCaught(error)
    }

    func channelInactive(context: ChannelHandlerContext) {
        completion.fail(ChannelError.eof)
        context.fireChannelInactive()
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        completion.fail(ChannelError.eof)
    }

    func cancelTimeout() {
        completion.cancelTimeout()
    }
}

private final class AuthenticationCompletion: @unchecked Sendable {
    private struct State {
        var isCompleted = false
        var hasScheduledTimeout = false
        var timeout: Scheduled<Void>?
    }

    private let promise: EventLoopPromise<Void>
    private let state = NIOLockedValueBox(State())

    var futureResult: EventLoopFuture<Void> {
        promise.futureResult
    }

    init(eventLoop: EventLoop) {
        self.promise = eventLoop.makePromise(of: Void.self)
    }

    func scheduleTimeout(_ timeout: TimeAmount) {
        let shouldSchedule = state.withLockedValue { state in
            guard !state.isCompleted, !state.hasScheduledTimeout else {
                return false
            }
            state.hasScheduledTimeout = true
            return true
        }
        guard shouldSchedule else { return }

        let scheduled = promise.futureResult.eventLoop.scheduleTask(
            deadline: .now() + timeout
        ) { [weak self] in
            self?.fail(ChannelError.connectTimeout(timeout))
            return ()
        }
        let shouldCancel = state.withLockedValue { state in
            if state.isCompleted {
                return true
            }
            state.timeout = scheduled
            return false
        }
        if shouldCancel { scheduled.cancel() }
    }

    func succeed() {
        complete(.success(()))
    }

    func fail(_ error: any Error) {
        complete(.failure(error))
    }

    func cancelTimeout() {
        let timeout = state.withLockedValue { state in
            let timeout = state.timeout
            state.timeout = nil
            return timeout
        }
        timeout?.cancel()
    }

    private func complete(_ result: Result<Void, any Error>) {
        let shouldComplete = state.withLockedValue { state in
            guard !state.isCompleted else { return false }
            state.isCompleted = true
            return true
        }
        guard shouldComplete else { return }

        cancelTimeout()
        promise.completeWith(result)
    }
}
