///|
/// Every error the client surfaces, in categories you can match on.
pub suberror NatsError {
  /// TCP connect failed, no INFO, malformed handshake, handshake timeout.
  HandshakeFailed(String)
  /// The server rejected the credentials.
  AuthFailed(String)
  /// The server denied a per-subject operation (connection stays usable).
  PermissionDenied(String)
  /// The server sent an unclassified -ERR.
  ProtocolError(String)
  /// A request or flush exceeded its deadline.
  Timeout(String)
  /// no_responders: nobody was subscribed to the requested subject.
  NoResponders(String)
  /// Locally rejected input: invalid subject, payload too large, wrong state.
  BadRequest(String)
  /// The connection is gone (closed locally, by the server, or keepalive
  /// timeout); in-flight operations are aborted with this.
  ConnectionClosed(String)
} derive(Eq, Debug)

///|
pub fn NatsError::message(self : NatsError) -> String {
  match self {
    HandshakeFailed(m) => "handshake failed: \{m}"
    AuthFailed(m) => "auth failed: \{m}"
    PermissionDenied(m) => "permission denied: \{m}"
    ProtocolError(m) => "protocol error: \{m}"
    Timeout(m) => "timeout: \{m}"
    NoResponders(m) => "no responders: \{m}"
    BadRequest(m) => "bad request: \{m}"
    ConnectionClosed(m) => "connection closed: \{m}"
  }
}

///|
test "nats error messages" {
  assert_eq(AuthFailed("bad creds").message(), "auth failed: bad creds")
  assert_eq(ConnectionClosed("eof").message(), "connection closed: eof")
}
