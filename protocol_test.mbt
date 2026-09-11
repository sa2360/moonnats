///|
/// Encode an ASCII string as Bytes for wire-protocol tests.
fn ascii(s : String) -> Bytes {
  let out : Array[Byte] = []
  for ch in s {
    out.push(ch.to_int().to_byte())
  }
  Bytes::from_array(out)
}

///|
test "server op model compares by value" {
  let msg = @moonnats.Msg(@moonnats.DeliveredMsg::{
    subject: "orders.new",
    sid: "1",
    reply: Some("_INBOX.abc"),
    payload: ascii("hello"),
  })
  let same = @moonnats.Msg(@moonnats.DeliveredMsg::{
    subject: "orders.new",
    sid: "1",
    reply: Some("_INBOX.abc"),
    payload: ascii("hello"),
  })
  let no_reply = @moonnats.Msg(@moonnats.DeliveredMsg::{
    subject: "orders.new",
    sid: "1",
    reply: None,
    payload: ascii("hello"),
  })
  assert_eq(msg, same)
  assert_not_eq(msg, no_reply)
  guard msg is @moonnats.Msg(delivered)
  assert_eq(delivered.subject, "orders.new")
  assert_eq(delivered.payload, ascii("hello"))
}

///|
test "client command model distinguishes variants" {
  let p = @moonnats.Pub(@moonnats.Publish::{
    subject: "foo",
    reply: None,
    payload: ascii("x"),
  })
  let s = @moonnats.Sub(@moonnats.Subscribe::{
    subject: "foo.*",
    sid: "42",
    queue_group: Some("workers"),
  })
  let u = @moonnats.Unsub(@moonnats.Unsubscribe::{
    sid: "42",
    max_msgs: Some(10),
  })
  assert_not_eq(p, s)
  assert_not_eq(s, u)
}
