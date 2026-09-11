///|
/// Result of trying to parse one operation from the buffered bytes.
pub(all) enum ParseResult {
  /// The buffer does not hold a complete frame yet.
  NeedMore
  /// A complete operation plus the number of buffer bytes it consumed.
  Op(ServerOp, Int)
  /// The buffered bytes can never form a valid frame at this position.
  Fail(String)
} derive(Eq, Debug)

///|
/// Incremental parser for the server side of the NATS wire protocol.
///
/// Bytes are appended with `feed` as TCP delivers them — possibly split in
/// the middle of a frame — and complete operations are pulled out with
/// `next_op`. The parser is a state machine over one growable buffer;
/// no allocation happens per fed chunk beyond the buffer growth itself.
pub struct Parser {
  priv mut buf : Array[Byte]
  priv mut pos : Int
} derive(Eq, Debug)

///|
pub fn Parser::new() -> Parser {
  { buf: [], pos: 0, }
}

///|
/// Append bytes delivered by the transport.
pub fn Parser::feed(self : Parser, data : Bytes) -> Unit {
  for b in data {
    self.buf.push(b)
  }
}

///|
/// Bytes currently buffered but not yet consumed.
pub fn Parser::buffered(self : Parser) -> Int {
  self.buf.length() - self.pos
}

///|
/// Try to parse the next complete operation. Consumes it on success;
/// leaves the buffer untouched on `NeedMore`.
pub fn Parser::next_op(self : Parser) -> ParseResult {
  match self.find_crlf(self.pos) {
    None => NeedMore
    Some(crlf) => {
      if crlf == self.pos {
        return Fail("empty protocol line")
      }
      let header = match self.ascii_range(self.pos, crlf) {
        Err(msg) => return Fail(msg)
        Ok(text) => text
      }
      let consumed = crlf + 2 - self.pos
      // The command word ends at the first space; everything after it is the
      // raw argument. -ERR descriptions and INFO JSON contain spaces, so they
      // must never be tokenized.
      let sp = first_space(header)
      let command = if sp < 0 { header } else { header[:sp].to_owned() }
      let rest = if sp < 0 { "" } else { header[sp + 1:].to_owned() }
      match command {
        "PING" => self.bare_op(consumed, rest, Ping)
        "PONG" => self.bare_op(consumed, rest, Pong)
        "+OK" => self.bare_op(consumed, rest, Ok)
        "-ERR" => {
          if rest.is_empty() {
            return Fail("malformed -ERR line")
          }
          self.op(consumed, ErrOp(strip_quotes(rest)))
        }
        "INFO" => {
          if rest.is_empty() {
            return Fail("malformed INFO line")
          }
          self.op(consumed, Info(rest))
        }
        "MSG" => self.parse_msg(split_tokens(header), crlf + 2)
        "HMSG" => self.parse_hmsg(split_tokens(header), crlf + 2)
        other => Fail("unknown protocol operation: \u{22}" + other + "\u{22}")
      }
    }
  }
}

///|
/// Drop consumed bytes so the buffer cannot grow without bound on
/// long-lived connections. Called by feed to piggyback compaction on the
/// next write.
pub fn Parser::feed_and_compact(self : Parser, data : Bytes) -> Unit {
  if self.pos > 0 && self.pos >= self.buf.length() {
    self.buf = []
    self.pos = 0
  } else if self.pos > 8192 {
    let rest : Array[Byte] = []
    let len = self.buf.length()
    for i in self.pos..<len {
      rest.push(self.buf[i])
    }
    self.buf = rest
    self.pos = 0
  }
  self.feed(data)
}

///|
/// Internal helpers below operate on buf[pos..] and must keep `pos` and the
/// buffer consistent with the ParseResult they return.
///

///|

///|
fn Parser::op(self : Parser, consumed : Int, result : ServerOp) -> ParseResult {
  self.pos += consumed
  Op(result, consumed)
}

///|
/// Line operations carry no arguments; trailing junk is a protocol error.
fn Parser::bare_op(
  self : Parser,
  consumed : Int,
  rest : String,
  op : ServerOp,
) -> ParseResult {
  if !rest.is_empty() {
    return Fail("unexpected argument for single-word operation")
  }
  self.op(consumed, op)
}

///|
fn Parser::find_crlf(self : Parser, from : Int) -> Int? {
  let len = self.buf.length()
  let mut i = from
  while i + 1 < len {
    if self.buf[i].to_int() == 13 && self.buf[i + 1].to_int() == 10 {
      return Some(i)
    }
    i += 1
  }
  None
}

///|
/// Decode ASCII bytes into a string; the text protocol lines are ASCII, and
/// anything above 127 means the stream is corrupt.
fn Parser::ascii_range(
  self : Parser,
  start : Int,
  end : Int,
) -> Result[String, String] {
  let sb = StringBuilder()
  for i in start..<end {
    let b = self.buf[i].to_int()
    if b < 0 || b > 127 {
      return Err("non-ASCII byte in protocol line")
    }
    sb.write_char(b.unsafe_to_char())
  }
  Ok(sb.to_string())
}

///|
/// Parse `MSG <subject> <sid> [reply-to] <#bytes>` and, once the payload and
/// its trailing CRLF are fully buffered, the delivered message.
fn Parser::parse_msg(
  self : Parser,
  tokens : Array[String],
  header_offset : Int,
) -> ParseResult {
  if tokens.length() != 4 && tokens.length() != 5 {
    return Fail("malformed MSG header")
  }
  let subject = tokens[1]
  let sid = tokens[2]
  let reply : String? = if tokens.length() == 5 {
    Some(tokens[3])
  } else {
    None
  }
  let size = match parse_usize(tokens[tokens.length() - 1]) {
    Err(msg) => return Fail(msg)
    Ok(n) => n
  }
  let body_end = header_offset + size + 2
  if self.buf.length() < body_end {
    return NeedMore
  }
  if self.buf[body_end - 2].to_int() != 13 ||
    self.buf[body_end - 1].to_int() != 10 {
    return Fail("payload not terminated by CRLF")
  }
  let payload = self.bytes_range(header_offset, header_offset + size)
  let consumed = body_end - self.pos
  self.op(consumed, Msg({ subject, sid, reply, payload, }))
}

///|
/// Parse `HMSG <subject> <sid> [reply-to] <hdr#> <total#>`; the body holds
/// the headers block followed by the payload, `total#` bytes together.
fn Parser::parse_hmsg(
  self : Parser,
  tokens : Array[String],
  header_offset : Int,
) -> ParseResult {
  if tokens.length() != 5 && tokens.length() != 6 {
    return Fail("malformed HMSG header")
  }
  let subject = tokens[1]
  let sid = tokens[2]
  let reply : String? = if tokens.length() == 6 {
    Some(tokens[3])
  } else {
    None
  }
  let hdr_size = match parse_usize(tokens[tokens.length() - 2]) {
    Err(msg) => return Fail(msg)
    Ok(n) => n
  }
  let total_size = match parse_usize(tokens[tokens.length() - 1]) {
    Err(msg) => return Fail(msg)
    Ok(n) => n
  }
  if hdr_size >= total_size {
    return Fail("HMSG headers size must be smaller than total size")
  }
  let body_end = header_offset + total_size + 2
  if self.buf.length() < body_end {
    return NeedMore
  }
  if self.buf[body_end - 2].to_int() != 13 ||
    self.buf[body_end - 1].to_int() != 10 {
    return Fail("payload not terminated by CRLF")
  }
  let headers = self.bytes_range(header_offset, header_offset + hdr_size)
  let payload = self.bytes_range(
    header_offset + hdr_size,
    header_offset + total_size,
  )
  let consumed = body_end - self.pos
  self.op(consumed, HMsg({ subject, sid, reply, headers, payload, }))
}

///|
fn Parser::bytes_range(self : Parser, start : Int, end : Int) -> Bytes {
  let out : Array[Byte] = []
  for i in start..<end {
    out.push(self.buf[i])
  }
  Bytes::from_array(out)
}

///|
/// Index of the first space in a protocol line, or -1.
fn first_space(line : String) -> Int {
  let mut i = 0
  let len = line.length()
  while i < len {
    if line[i] == u16(' ') {
      return i
    }
    i += 1
  }
  -1
}

///|
/// Split on single spaces; the protocol never repeats spaces, so consecutive
/// spaces produce an empty token that later validation rejects naturally.
fn split_tokens(line : String) -> Array[String] {
  let tokens : Array[String] = []
  let current = StringBuilder()
  for ch in line {
    if ch == ' ' {
      tokens.push(current.to_string())
      current.reset()
    } else {
      current.write_char(ch)
    }
  }
  tokens.push(current.to_string())
  tokens
}

///|
fn parse_usize(text : String) -> Result[Int, String] {
  if text.is_empty() {
    return Err("empty size field")
  }
  let mut value = 0
  for ch in text {
    if ch < '0' || ch > '9' {
      return Err("invalid size field: \u{22}" + text + "\u{22}")
    }
    value = value * 10 + (ch.to_int() - '0'.to_int())
    if value > 1073741824 {
      return Err("size field too large")
    }
  }
  Ok(value)
}

///|
fn strip_quotes(text : String) -> String {
  let len = text.length()
  if len >= 2 && text[0] == '\'' && text[len - 1] == '\'' {
    text[1:len - 1].to_owned()
  } else {
    text
  }
}
