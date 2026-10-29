///|
/// Connection options; see `Options::default` for the values used when a
/// field is not overridden.
pub(all) struct Options {
  host : String
  port : Int32
  /// Client name advertised to the server (monitoring only).
  name : String?
  user : String?
  pass : String?
  token : String?
  /// Establish TLS using the system trust store or the configured PEM root.
  tls : Bool
  /// Start TLS before reading the server INFO line (for handshake-first servers).
  tls_handshake_first : Bool
  /// Optional PEM file used as the exclusive trust root.
  tls_root_certificate : String?
  /// Handshake deadline used by the structured connection helpers, in
  /// milliseconds (INFO + CONNECT + PING/PONG).
  handshake_timeout_ms : Int32
  /// Delay between connection attempts made by ReconnectingClient.
  reconnect_delay_ms : Int32
  /// How often the client sends a keepalive PING.
  ping_interval_ms : Int32
  /// Consecutive unanswered PINGs before the client declares the server dead.
  max_outstanding_pings : Int32
  /// Request timeout when the caller does not pass one explicitly.
  default_request_timeout_ms : Int32
  /// First token of generated reply subjects.
  inbox_prefix : String
  /// Ask the server to acknowledge every command with +OK (debugging).
  verbose : Bool
} derive(Eq, Debug)

///|
pub fn Options::default() -> Options {
  {
    host: "127.0.0.1",
    port: 4222,
    name: None,
    user: None,
    pass: None,
    token: None,
    tls: false,
    tls_handshake_first: false,
    tls_root_certificate: None,
    handshake_timeout_ms: 10000,
    reconnect_delay_ms: 1000,
    ping_interval_ms: 60000,
    max_outstanding_pings: 2,
    default_request_timeout_ms: 2000,
    inbox_prefix: "_INBOX",
    verbose: false,
  }
}

///|
test "defaults follow nats conventions" {
  let o = Options::default()
  assert_eq(o.port, 4222)
  assert_eq(o.ping_interval_ms, 60000)
  assert_eq(o.max_outstanding_pings, 2)
  assert_eq(o.handshake_timeout_ms, 10000)
  assert_eq(o.reconnect_delay_ms, 1000)
  assert_eq(o.default_request_timeout_ms, 2000)
  assert_eq(o.inbox_prefix, "_INBOX")
  assert_eq(o.host, "127.0.0.1")
}
