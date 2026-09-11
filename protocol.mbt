///|
/// A message delivered by the server via `MSG`.
pub(all) struct DeliveredMsg {
  subject : String
  sid : String
  reply : String?
  payload : Bytes
} derive(Eq, Show)

///|
/// A message delivered by the server via `HMSG`. The `headers` block is the
/// raw `NATS/1.0\r\nKey: Value\r\n\r\n` prefix of the body; splitting it into
/// key/value pairs is a separate concern.
pub(all) struct DeliveredHMsg {
  subject : String
  sid : String
  reply : String?
  headers : Bytes
  payload : Bytes
} derive(Eq, Show)

///|
/// A server-to-client operation on the NATS wire protocol.
///
/// The text protocol frames every operation as one line terminated by
/// `\r\n`; delivered messages additionally carry a binary body whose length
/// is declared in the header line.
pub(all) enum ServerOp {
  /// `INFO {json}` — the connect handshake banner and later cluster updates.
  /// Kept as raw JSON text here; the parsed form arrives with INFO support.
  Info(String)
  Msg(DeliveredMsg)
  HMsg(DeliveredHMsg)
  Ping
  Pong
  /// `+OK` — acknowledgement of a verbose command.
  Ok
  /// `-ERR '<description>'` — protocol or permission failure.
  ErrOp(String)
} derive(Eq, Show)

///|
/// Arguments of `PUB <subject> [reply-to] <#bytes>`.
pub(all) struct Publish {
  subject : String
  reply : String?
  payload : Bytes
} derive(Eq, Show)

///|
/// Arguments of `SUB <subject> [queue-group] <sid>`.
pub(all) struct Subscribe {
  subject : String
  sid : String
  queue_group : String?
} derive(Eq, Show)

///|
/// Arguments of `UNSUB <sid> [max-msgs]`.
pub(all) struct Unsubscribe {
  sid : String
  max_msgs : Int?
} derive(Eq, Show)

///|
/// A client-to-server command.
pub(all) enum ClientCommand {
  /// `CONNECT {json}` — the client handshake; serialized by `ConnectConfig`.
  Connect(String)
  Pub(Publish)
  Sub(Subscribe)
  Unsub(Unsubscribe)
  Ping
} derive(Eq, Show)
