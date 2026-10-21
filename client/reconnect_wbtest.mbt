///|
test "cluster endpoint parsing accepts schemes and IPv6" {
  assert_true(
    parse_endpoint("nats://node-a:4222")
    is Some({ host: "node-a", port: 4222, }),
  )
  assert_true(
    parse_endpoint("[::1]:4333") is Some({ host: "::1", port: 4333, }),
  )
  assert_true(parse_endpoint("node-a:not-a-port") is None)
}

///|
async test "reconnecting client rejects publishes while offline" {
  let client = ReconnectingClient::new()
  guard client.publish("offline", ascii_bytes("x"))
    is Err(ConnectionClosed("reconnecting"))
}

///|
async test "close before run prevents the initial dial" {
  let client = ReconnectingClient::new(options={
    ..Options::default(),
    port: 1,
  })
  client.close()
  @async.with_task_group(group => client.run(group))
  assert_false(client.is_connected())
}

///|
async test "reconnecting client restores subscriptions after disconnect" {
  let port = next_test_port()
  let discovered_port = next_test_port()
  let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:\{port}"))
  let discovered_server = @socket.TcpServer(
    @socket.Addr::parse("127.0.0.1:\{discovered_port}"),
  )
  let client = ReconnectingClient::new(options={
    ..Options::default(),
    port,
    handshake_timeout_ms: 1000,
    reconnect_delay_ms: 20,
    ping_interval_ms: 60000,
  })
  let restored : @async.Queue[Bool] = @async.Queue(kind=Unbounded)
  let close_done : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
  let restored_ok = [false]
  let finished = [false]
  let stage = ["starting"]
  let _ = client.subscribe("recover.me", _ => ())
  @async.with_task_group(group => {
    let _ = group.spawn(() => {
      for _ in 0..<500 {
        if finished[0] {
          return
        }
        @async.sleep(20)
      }
      abort("reconnect test timeout at " + stage[0])
    })
    let _ = group.spawn(() => {
      stage[0] = "accept first"
      let (first, _) = server.accept()
      stage[0] = "first handshake"
      first.write(
        ascii_bytes(
          "INFO {\"server_id\":\"ONE\",\"version\":\"2.10.0\",\"host\":\"127.0.0.1\",\"port\":4222}\r\n",
        ),
      )
      let _ = first.read_until("\r\n")
      let _ = first.read_until("\r\n")
      first.write(ascii_bytes("PONG\r\n"))
      stage[0] = "first SUB"
      let first_sub = first.read_until("\r\n")
      guard first_sub is Some(line) && line.contains("SUB recover.me") else {
        abort("first session did not subscribe")
      }
      first.write(
        ascii_bytes(
          "INFO {\"server_id\":\"ONE\",\"version\":\"2.10.0\",\"host\":\"127.0.0.1\",\"port\":4222,\"connect_urls\":[\"127.0.0.1:\{discovered_port}\"]}\r\n",
        ),
      )
      @async.sleep(100)
      first.close()

      stage[0] = "accept second"
      let (second, _) = discovered_server.accept()
      stage[0] = "second handshake"
      second.write(
        ascii_bytes(
          "INFO {\"server_id\":\"TWO\",\"version\":\"2.10.0\",\"host\":\"127.0.0.1\",\"port\":4222}\r\n",
        ),
      )
      let _ = second.read_until("\r\n")
      let _ = second.read_until("\r\n")
      second.write(ascii_bytes("PONG\r\n"))
      stage[0] = "second SUB"
      let second_sub = second.read_until("\r\n")
      let ok = match second_sub {
        Some(line) => line.contains("SUB recover.me")
        None => false
      }
      restored_ok[0] = ok
      restored.put(ok)
      stage[0] = "server close protocol"
      // Complete the supervisor's graceful close handshake.
      let _ = second.read_until("\r\n")
      let ping = second.read_until("\r\n")
      if ping is Some("PING\r\n") {
        second.write(ascii_bytes("PONG\r\n"))
      }
      second.close()
      server.close()
      discovered_server.close()
      stage[0] = "server done"
    })
    let _ = group.spawn(() => {
      let _ = restored.get()
      stage[0] = "client close"
      client.close()
      stage[0] = "client closed"
      close_done.put(())
    })
    client.run(group)
    stage[0] = "run returned"
    finished[0] = true
  })
  assert_true(close_done.try_get() is Some(_))
  assert_true(restored_ok[0])
}
