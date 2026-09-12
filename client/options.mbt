///|
/// Connection options; see `Options::default` for the values used when a
/// field is not overridden.
pub(all) struct Options {
  host : String
  port : Int
  /// Client name advertised to the server (monitoring only).
  name : String?
  user : String?
  pass : String?
  token : String?
  /// Whole-handshake deadline in milliseconds (INFO + CONNECT + PING/PONG).
  handshake_timeout_ms : Int
  /// How often the client sends a keepalive PING.
  ping_interval_ms : Int
  /// Consecutive unanswered PINGs before the client declares the server dead.
  max_outstanding_pings : Int
  /// Request timeout when the caller does not pass one explicitly.
  default_request_timeout_ms : Int
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
    handshake_timeout_ms: 10000,
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
  assert_eq(o.default_request_timeout_ms, 2000)
  assert_eq(o.inbox_prefix, "_INBOX")
  assert_eq(o.host, "127.0.0.1")
}
