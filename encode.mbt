///|
/// Encode a client command into the exact bytes to write to the wire.
///
/// Every command is one `\r\n`-terminated line; `Pub` appends the payload
/// followed by a second `\r\n` as the protocol requires. The header line is
/// ASCII by protocol construction, so the line-to-bytes mapping is lossless;
/// payload bytes are concatenated, never routed through a string.
pub fn encode_command(cmd : ClientCommand) -> Bytes {
  let line = StringBuilder()
  match cmd {
    Connect(json) => {
      line.write_string("CONNECT ")
      line.write_string(json)
    }
    Pub(p) => {
      line.write_string("PUB ")
      line.write_string(p.subject)
      append_optional(line, p.reply)
      line.write_char(' ')
      line.write_string(p.payload.length().to_string())
    }
    Sub(s) => {
      line.write_string("SUB ")
      line.write_string(s.subject)
      append_optional(line, s.queue_group)
      line.write_char(' ')
      line.write_string(s.sid)
    }
    Unsub(u) => {
      line.write_string("UNSUB ")
      line.write_string(u.sid)
      match u.max_msgs {
        Some(max) => {
          line.write_char(' ')
          line.write_string(max.to_string())
        }
        None => ()
      }
    }
    Ping => line.write_string("PING")
    Pong => line.write_string("PONG")
  }
  line.write_string("\r\n")
  let header = string_to_bytes(line.to_string())
  match cmd {
    Pub(p) => bytes_concat([header, p.payload, string_to_bytes("\r\n")])
    _ => header
  }
}

///|
/// Write ` value` when the optional protocol field is present; PUB, SUB and
/// UNSUB interleave optional fields positionally with required ones.
fn append_optional(line : StringBuilder, value : String?) -> Unit {
  match value {
    Some(v) => {
      line.write_char(' ')
      line.write_string(v)
    }
    None => ()
  }
}

///|
fn string_to_bytes(s : String) -> Bytes {
  let out : Array[Byte] = []
  for ch in s {
    out.push(ch.to_int().to_byte())
  }
  Bytes::from_array(out)
}

///|
fn bytes_concat(parts : Array[Bytes]) -> Bytes {
  let out : Array[Byte] = []
  for part in parts {
    for b in part {
      out.push(b)
    }
  }
  Bytes::from_array(out)
}

///|
/// Keepalive PING command bytes.
pub fn ping_command() -> Bytes {
  encode_command(ClientCommand::Ping)
}

///|
/// Keepalive PONG command bytes (reply to a server PING).
pub fn pong_command() -> Bytes {
  encode_command(ClientCommand::Pong)
}
