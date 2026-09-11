///|
test "parses single-line operations" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("PING\r\n"))
  guard p.next_op() is Op(op, consumed)
  assert_eq(op, @moonnats.Ping)
  assert_eq(consumed, 6)
  assert_eq(p.buffered(), 0)
}

///|
test "parses pong, ok and err" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("PONG\r\n+OK\r\n-ERR 'Authorization Violation'\r\n"))
  guard p.next_op() is Op(op1, _)
  assert_eq(op1, @moonnats.Pong)
  guard p.next_op() is Op(op2, _)
  assert_eq(op2, @moonnats.Ok)
  guard p.next_op() is Op(op3, _)
  assert_eq(op3, @moonnats.ErrOp("Authorization Violation"))
  assert_eq(p.next_op(), @moonnats.NeedMore)
}

///|
test "parses info keeping the raw json" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("INFO {\"server_id\":\"NATS\",\"version\":\"2.10.0\"}\r\n"))
  guard p.next_op() is Op(op, _)
  guard op is @moonnats.Info(json)
  assert_eq(json, "{\"server_id\":\"NATS\",\"version\":\"2.10.0\"}")
}

///|
test "parses msg without reply subject" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("MSG foo 1 5\r\nhello\r\n"))
  guard p.next_op() is Op(op, _)
  assert_eq(
    op,
    @moonnats.Msg(@moonnats.DeliveredMsg::{
      subject: "foo",
      sid: "1",
      reply: None,
      payload: ascii("hello"),
    }),
  )
}

///|
test "parses msg with reply subject" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("MSG foo 1 _INBOX.x 2\r\nhi\r\n"))
  guard p.next_op() is Op(op, _)
  guard op is @moonnats.Msg(m)
  assert_eq(m.reply, Some("_INBOX.x"))
  assert_eq(m.payload, ascii("hi"))
}

///|
test "msg payload may contain crlf bytes" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("MSG bin 7 4\r\n"))
  p.feed(ascii("\r\n\r\n"))
  assert_eq(p.next_op(), @moonnats.NeedMore)
  p.feed(ascii("\r\n"))
  guard p.next_op() is Op(op, _)
  guard op is @moonnats.Msg(m)
  assert_eq(m.payload, ascii("\r\n\r\n"))
}

///|
test "hmsg splits headers and payload" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("HMSG foo 1 18 22\r\nNATS/1.0\r\nA: b\r\n\r\nrest\r\n"))
  guard p.next_op() is Op(op, _)
  guard op is @moonnats.HMsg(m)
  assert_eq(m.headers, ascii("NATS/1.0\r\nA: b\r\n\r\n"))
  assert_eq(m.payload, ascii("rest"))
}

///|
test "incomplete frames return need more and keep the buffer" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("MSG foo 1 11\r\nhel"))
  assert_eq(p.next_op(), @moonnats.NeedMore)
  assert_eq(p.buffered(), 17)
  p.feed(ascii("lo world\r\n"))
  guard p.next_op() is Op(op, _)
  guard op is @moonnats.Msg(m)
  assert_eq(m.payload, ascii("hello world"))
  assert_eq(p.buffered(), 0)
}

///|
test "headers split across feeds" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("PIN"))
  assert_eq(p.next_op(), @moonnats.NeedMore)
  p.feed(ascii("G\r\nMSG foo 1 2\r\nab"))
  guard p.next_op() is Op(op1, _)
  assert_eq(op1, @moonnats.Ping)
  assert_eq(p.next_op(), @moonnats.NeedMore)
  p.feed(ascii("\r\n"))
  guard p.next_op() is Op(op2, _)
  guard op2 is @moonnats.Msg(m)
  assert_eq(m.payload, ascii("ab"))
}

///|
test "malformed frames fail with a reason" {
  let p = @moonnats.Parser::new()
  p.feed(ascii("BOGUS x\r\n"))
  guard p.next_op() is Fail(reason)
  assert_true(reason.contains("unknown"))

  let p2 = @moonnats.Parser::new()
  p2.feed(ascii("MSG foo 1 abc\r\n"))
  guard p2.next_op() is Fail(reason2)
  assert_true(reason2.contains("size"))
}

///|
test "feed and compact keeps parsing across many frames" {
  let p = @moonnats.Parser::new()
  for _i in 0..<100 {
    p.feed_and_compact(ascii("PING\r\nPONG\r\n"))
    guard p.next_op() is Op(a, _)
    assert_eq(a, @moonnats.Ping)
    guard p.next_op() is Op(b, _)
    assert_eq(b, @moonnats.Pong)
  }
  assert_eq(p.buffered(), 0)
}
