///|
/// One decoded header of an `HMSG` message.
pub(all) struct Header {
  name : String
  value : String
} derive(Eq, Debug)

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
} derive(Eq, Debug)

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
  let version_line = text[:version_end.0].to_owned()
  guard version_line.has_prefix("NATS/1.0") else {
    return Err("headers block does not start with NATS/1.0")
  }
  let headers : Array[Header] = []
  let body = text[version_end.0 + version_end.1:].to_owned()
  for line in split_lines(body) {
    if line.is_empty() {
      continue
    }
    let colon = match line.find(":") {
      Some(c) if c > 0 => c
      _ => return Err("malformed header line: \"\{line}\"")
    }
    let name = line[:colon].to_owned()
    let mut value = line[colon + 1:].to_owned()
    // Single leading space after the colon is not part of the value.
    if value.has_prefix(" ") {
      value = value[1:].to_owned()
    }
    headers.push({ name, value, })
  }
  Ok({ version_line: version_line[8:].to_owned(), headers, })
}

///|
fn ascii_text(raw : Bytes) -> String {
  let sb = StringBuilder()
  for b in raw {
    sb.write_char((b.to_int() & 0x7F).unsafe_to_char())
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
      lines.push(text[start:end].to_owned())
      start = i + 1
    }
    i += 1
  }
  if start < len {
    lines.push(text[start:].to_owned())
  }
  lines
}
