///|
test "parses basic header block" {
  let block = @moonnats.parse_header_block(
    ascii("NATS/1.0\r\nK1: V1\r\nK2: V2\r\n\r\n"),
  )
  guard block is Ok(parsed)
  assert_eq(parsed.version_line, "")
  assert_eq(parsed.headers, [
    { name: "K1", value: "V1", },
    { name: "K2", value: "V2", },
  ])
}

///|
test "parses inline status in version line" {
  let block = @moonnats.parse_header_block(
    ascii("NATS/1.0 100 Idle Heartbeat\r\n\r\n"),
  )
  guard block is Ok(parsed)
  assert_eq(parsed.version_line, " 100 Idle Heartbeat")
  assert_eq(parsed.headers, [])
}

///|
test "tolerates lf-only line endings" {
  let block = @moonnats.parse_header_block(ascii("NATS/1.0\nA: 1\nB: 2\n\n"))
  guard block is Ok(parsed)
  assert_eq(parsed.headers.length(), 2)
  assert_eq(parsed.headers[1].name, "B")
}

///|
test "value keeps additional spaces after the separator" {
  let block = @moonnats.parse_header_block(
    ascii("NATS/1.0\r\nX:  two spaces\r\n\r\n"),
  )
  guard block is Ok(parsed)
  assert_eq(parsed.headers[0].value, " two spaces")
}

///|
test "rejects blocks without the version marker" {
  guard @moonnats.parse_header_block(ascii("HTTP/1.1 200 OK\r\n\r\n")) is Err(_)
  guard @moonnats.parse_header_block(ascii("")) is Err(_)
}

///|
test "rejects header lines without a colon" {
  guard @moonnats.parse_header_block(ascii("NATS/1.0\r\nno-colon-here\r\n\r\n"))
    is Err(_)
}
