///|
test "connect config serializes required fields" {
  let cfg = @moonnats.ConnectConfig::default()
  let json = cfg.to_json().stringify()
  assert_true(json.contains("\"lang\":\"moonbit\""))
  assert_true(json.contains("\"protocol\":1"))
  assert_true(json.contains("\"echo\":true"))
  assert_true(json.contains("\"headers\":true"))
  assert_true(json.contains("\"verbose\":false"))
  assert_true(json.contains("\"no_echo\":false"))
  assert_true(json.contains("\"version\":\"0.1.0\""))
}

///|
test "connect config omits unset credentials" {
  let json = @moonnats.ConnectConfig::default().to_json().stringify()
  assert_false(json.contains("user"))
  assert_false(json.contains("pass"))
  assert_false(json.contains("auth_token"))
  assert_false(json.contains("\"name\""))
}

///|
test "connect config emits credentials when set" {
  let cfg = @moonnats.ConnectConfig::default(name=Some("worker-1"))
  let json = cfg.to_json().stringify()
  assert_true(json.contains("\"name\":\"worker-1\""))
  let with_auth = { ..cfg, user: Some("alice"), pass: Some("s3cret"), }
  let json2 = with_auth.to_json().stringify()
  assert_true(json2.contains("\"user\":\"alice\""))
  assert_true(json2.contains("\"pass\":\"s3cret\""))
}

///|
test "connect config encodes to a full CONNECT command" {
  let bytes = @moonnats.ConnectConfig::default().encode()
  let text = bytes_to_ascii(bytes)
  assert_true(text.has_prefix("CONNECT {"))
  assert_true(text.has_suffix("}\r\n"))
}

///|
fn bytes_to_ascii(b : Bytes) -> String {
  let sb = StringBuilder()
  for byte in b {
    sb.write_char(byte.to_int().unsafe_to_char())
  }
  sb.to_string()
}
