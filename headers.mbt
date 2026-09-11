///|
/// One decoded header of an `HMSG` message.
pub(all) struct Header {
  name : String
  value : String
} derive(Eq, Show)

///|
/// The headers block of an `HMSG` body: a `NATS/1.0` line optionally
/// followed by an inline status, then CRLF-separated `Name: Value` pairs
/// ended by an empty line.
///
/// `NATS/1.0 100 Idle Heartbeat\r\nK: V\r\n\r\n`
pub(all) struct HeaderBlock {
  /// The protocol marker line without the `NATS/1.0` prefix (inline status
  /// code and description when present, e.g. `100 Idle Heartbeat`).
  version_line : String
  headers : Array[Header]
} derive(Eq, Show)

///|
/// Decode a raw headers block. Tolerates LF-only line endings, as nats-server
/// itself produces CRLF but user headers may be re-encoded by intermediaries.
pub fn parse_header_block(raw : Bytes) -> Result[HeaderBlock, String] {
  let text = ascii_text(raw)
  // Locate the end of the version line, CRLF first, bare LF as fallback.
  let version_end : (Int, Int) = match text.find("\r\n") {
    Some(i) => (i, 2)
    None =>
      match text.find("\n") {
        Some(i) => (i, 1)
        None => return Err("headers block missing version line")
      }
  }
  let version_line = text.substring(start=0, end=version_end.0)
  guard version_line.has_prefix("NATS/1.0") else {
    return Err("headers block does not start with NATS/1.0")
  }
  let headers : Array[Header] = []
  let body = text.substring(start=version_end.0 + version_end.1)
  for line in split_lines(body) {
    if line.is_empty() {
      continue
    }
    let colon = match line.find(":") {
      Some(c) if c > 0 => c
      _ => return Err("malformed header line: \"\{line}\"")
    }
    let name = line.substring(start=0, end=colon)
    let mut value = line.substring(start=colon + 1)
    // Single leading space after the colon is not part of the value.
    if value.has_prefix(" ") {
      value = value.substring(start=1)
    }
    headers.push({ name, value })
  }
  Ok({ version_line: version_line.substring(start=8), headers })
}

///|
fn ascii_text(raw : Bytes) -> String {
  let sb = StringBuilder::new()
  for b in raw {
    sb.write_char(Char::from_int(b.to_int() & 0x7F))
  }
  sb.to_string()
}

///|
fn split_lines(text : String) -> Array[String] {
  let lines : Array[String] = []
  let mut start = 0
  let len = text.length()
  let mut i = 0
  while i < len {
    if text[i] == u16('\n') {
      let mut end = i
      if end > start && text[end - 1] == u16('\r') {
        end -= 1
      }
      lines.push(text.substring(start~, end~))
      start = i + 1
    }
    i += 1
  }
  if start < len {
    lines.push(text.substring(start~))
  }
  lines
}
