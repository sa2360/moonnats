///|
test "encode pub without reply" {
  let bytes = @moonnats.encode_command(
    @moonnats.Pub(@moonnats.Publish::{
      subject: "foo.bar",
      reply: None,
      payload: ascii("hello"),
    }),
  )
  assert_eq(bytes, ascii("PUB foo.bar 5\r\nhello\r\n"))
}

///|
test "encode pub with reply subject" {
  let bytes = @moonnats.encode_command(
    @moonnats.Pub(@moonnats.Publish::{
      subject: "foo",
      reply: Some("_INBOX.x1"),
      payload: ascii("hi"),
    }),
  )
  assert_eq(bytes, ascii("PUB foo _INBOX.x1 2\r\nhi\r\n"))
}

///|
test "encode pub with empty payload" {
  let bytes = @moonnats.encode_command(
    @moonnats.Pub(@moonnats.Publish::{
      subject: "a",
      reply: None,
      payload: ascii(""),
    }),
  )
  assert_eq(bytes, ascii("PUB a 0\r\n\r\n"))
}

///|
test "encode pub preserves binary payload" {
  let payload = Bytes::from_array([
    (0).to_byte(),
    (13).to_byte(),
    (10).to_byte(),
    (255).to_byte(),
  ])
  let bytes = @moonnats.encode_command(
    @moonnats.Pub(@moonnats.Publish::{ subject: "bin", reply: None, payload, }),
  )
  let expected : Array[Byte] = []
  for b in ascii("PUB bin 4\r\n") {
    expected.push(b)
  }
  for b in payload {
    expected.push(b)
  }
  for b in ascii("\r\n") {
    expected.push(b)
  }
  assert_eq(bytes, Bytes::from_array(expected))
}

///|
test "encode sub with and without queue group" {
  assert_eq(
    @moonnats.encode_command(
      @moonnats.Sub(@moonnats.Subscribe::{
        subject: "foo.*",
        sid: "1",
        queue_group: None,
      }),
    ),
    ascii("SUB foo.* 1\r\n"),
  )
  assert_eq(
    @moonnats.encode_command(
      @moonnats.Sub(@moonnats.Subscribe::{
        subject: "help",
        sid: "2",
        queue_group: Some("workers"),
      }),
    ),
    ascii("SUB help workers 2\r\n"),
  )
}

///|
test "encode unsub with and without max msgs" {
  assert_eq(
    @moonnats.encode_command(
      @moonnats.Unsub(@moonnats.Unsubscribe::{ sid: "1", max_msgs: None, }),
    ),
    ascii("UNSUB 1\r\n"),
  )
  assert_eq(
    @moonnats.encode_command(
      @moonnats.Unsub(@moonnats.Unsubscribe::{ sid: "1", max_msgs: Some(5), }),
    ),
    ascii("UNSUB 1 5\r\n"),
  )
}

///|
test "encode ping and connect" {
  assert_eq(@moonnats.encode_command(@moonnats.Ping), ascii("PING\r\n"))
  assert_eq(
    @moonnats.encode_command(@moonnats.Connect("{\"a\":1}")),
    ascii("CONNECT {\"a\":1}\r\n"),
  )
}
