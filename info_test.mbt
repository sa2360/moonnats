///|
test "parses a full server info" {
  let info = @moonnats.parse_server_info(
    "{\"server_id\":\"NDXAA\",\"server_name\":\"ndxaa\",\"version\":\"2.10.11\",\"host\":\"0.0.0.0\",\"port\":4222,\"headers\":true,\"proto\":1,\"max_payload\":1048576,\"auth_required\":true,\"cluster\":\"demo\"}",
  )
  guard info is Ok(parsed)
  assert_eq(parsed.server_id, "NDXAA")
  assert_eq(parsed.server_name, "ndxaa")
  assert_eq(parsed.version, "2.10.11")
  assert_eq(parsed.port, 4222)
  assert_true(parsed.headers)
  assert_true(parsed.auth_required)
  assert_eq(parsed.max_payload, 1048576)
  assert_eq(parsed.proto, 1)
}

///|
test "tolerates absent optional fields" {
  let info = @moonnats.parse_server_info(
    "{\"server_id\":\"NDX\",\"version\":\"2.10.0\",\"host\":\"localhost\",\"port\":4222}",
  )
  guard info is Ok(parsed)
  assert_false(parsed.headers)
  assert_false(parsed.auth_required)
  assert_eq(parsed.max_payload, 0)
}

///|
test "rejects malformed json and missing required fields" {
  guard @moonnats.parse_server_info("not json") is Err(_)
  guard @moonnats.parse_server_info("[]") is Err(_)
  guard @moonnats.parse_server_info("{\"port\":4222}") is Err(_)
}

///|
test "extracts info from a server op" {
  let op = @moonnats.Info(
    "{\"server_id\":\"S1\",\"version\":\"2\",\"host\":\"h\",\"port\":1}",
  )
  guard @moonnats.server_info_from_op(op) is Ok(info)
  assert_eq(info.server_id, "S1")
  guard @moonnats.server_info_from_op(@moonnats.Ping) is Err(_)
}
