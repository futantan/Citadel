import NIO

public struct SSHClientSettings: Sendable {
    public var host: String
    public var port: Int
    public var authenticationMethod: @Sendable () -> SSHAuthenticationMethod
    public var hostKeyValidator: SSHHostKeyValidator
    public var algorithms: SSHAlgorithms = SSHAlgorithms()
    public var protocolOptions: Set<SSHProtocolOption> = []
    public var group: EventLoopGroup = MultiThreadedEventLoopGroup.singleton
    internal var channelHandlers: [ChannelHandler & Sendable] = []
    public var connectTimeout: TimeAmount = .seconds(30)
    public var authenticationTimeout: TimeAmount = .seconds(10)
    public var onUserAuthenticationBanner:
        (@Sendable (_ message: String, _ languageTag: String) -> Void)?

    public init(
        host: String,
        port: Int = 22,
        authenticationMethod: @Sendable @escaping () -> SSHAuthenticationMethod,
        hostKeyValidator: SSHHostKeyValidator
    ) {
        self.host = host
        self.port = port
        self.authenticationMethod = authenticationMethod
        self.hostKeyValidator = hostKeyValidator
    }
}
