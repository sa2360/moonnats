///|
test "readme examples" {
  // subject examples
  assert_true(@moonnats.is_valid_subject("foo.bar"))
  assert_false(@moonnats.is_valid_subject("foo.*"))
  assert_true(@moonnats.is_valid_subject("foo.*", wildcards=true))
  assert_true(@moonnats.is_valid_subject("foo.>", wildcards=true))
  assert_false(@moonnats.is_valid_subject("foo.>.bar", wildcards=true))
  assert_true(@moonnats.subject_matches("foo.*", "foo.bar"))
  assert_false(@moonnats.subject_matches("foo.*", "foo.bar.baz"))
  assert_true(@moonnats.subject_matches("foo.>", "foo.bar.baz"))
  assert_false(@moonnats.subject_matches("foo.>", "foo"))
  // encode example
  let cmd : @moonnats.ClientCommand = @moonnats.Pub(@moonnats.Publish::{
    subject: "events",
    reply: None,
    payload: b"hi",
  })
  ignore(@moonnats.encode_command(cmd))
  // connect config
  let cfg = @moonnats.ConnectConfig::default(name=Some("my-service"))
  ignore(cfg.encode())
  ignore(cfg.to_json())
  // parser example
  let parser = @moonnats.Parser::new()
  parser.feed(b"PING\r\n")
  ignore(parser.next_op())
  ignore(@moonnats.parse_server_info("{}"))
  ignore(@moonnats.parse_header_block(b"NATS/1.0\r\n\r\n"))
}
