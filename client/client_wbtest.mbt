// Client behaviour tests against a real loopback TCP fake server.
//
// Ground rules learned the hard way (in-memory pipes had uglier semantics
// than sockets): the handshake runs in its own task opposite the server's
// acceptor script, and the writer/reader/keepalive loops are spawned by the
// task running the test scenario — never from inside another spawned task.

///|
/// Established client over a fresh server: dials, handshakes, starts loops.
async fn established(
  group : @async.TaskGroup[Unit],
  options? : Options = Options::default(),
) -> (Conn, FakeServer) {
  let (srv, port) = start_server(group)
  let ready : @async.Queue[Conn] = @async.Queue(kind=Unbounded)
  let _ = group.spawn(() => {
    let conn = client_dial(port, options~)
    let _ = conn.handshake()
    ready.put(conn)
  })
  let conn = ready.get()
  conn.spawn_loops(group)
  (conn, srv)
}

///|
async test "handshake succeeds and sends CONNECT with identity" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let captured : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let conn_box : @async.Queue[Conn] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      srv.send_info()
      guard srv.read_line() is Some(connect_line)
      captured.put(connect_line)
      let _ = srv.read_line()
      srv.put("PONG\r\n")
    })
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      let outcome = conn.handshake()
      result.put(
        match outcome {
          Ok(_) => "OK"
          Err(e) => e.message()
        },
      )
      conn_box.put(conn)
    })
    let connect_line = captured.get()
    assert_true(connect_line.has_prefix("CONNECT "))
    assert_true(connect_line.contains("\"lang\":\"moonbit\""))
    assert_eq(result.get(), "OK")
    let conn = conn_box.get()
    assert_true(conn.is_open())
    guard conn.server_info() is Some(info)
    assert_eq(info.server_id, "FAKE")
    let _ = group.spawn(() => conn.close(timeout_ms=200))
  })
}

///|
async test "handshake reports auth rejection" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      srv.send_info()
      let _ = srv.read_line()
      let _ = srv.read_line()
      srv.put("-ERR 'Authorization Violation'\r\n")
    })
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      let outcome = conn.handshake()
      result.put(
        match outcome {
          Ok(_) => "OK"
          Err(e) => e.message()
        },
      )
    })
    assert_true(result.get().contains("auth"))
  })
}

///|
async test "handshake times out when server is silent after INFO" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      srv.send_info()
      // never answers the PING
      @async.sleep(400)
    })
    let _ = group.spawn(() => {
      let conn = client_dial(port, options={
        ..Options::default(),
        handshake_timeout_ms: 80,
      })
      let outcome = conn.handshake()
      result.put(
        match outcome {
          Ok(_) => "OK"
          Err(e) => e.message()
        },
      )
    })
    assert_true(result.get().contains("timeout"))
  })
}

///|
async test "publish reaches the wire and validates locally" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let lines : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      srv.send_info()
      let _ = srv.finish_handshake()
      guard srv.read_line() is Some(line)
      lines.put(line)
    })
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      guard conn.publish("foo bar", ascii_bytes("x")) is Err(_)
      guard conn.publish("events", ascii_bytes("hello")) is Ok(_)
    })
    assert_eq(lines.get(), "PUB events 5")
  })
}

///|
async test "publish rejects payloads over server max_payload" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    srv.max_payload = 8
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      srv.send_info()
      let _ = srv.finish_handshake()
      @async.sleep(300)
    })
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let verdict = match conn.publish("big", ascii_bytes("123456789")) {
        Err(e) => e.message()
        Ok(_) => "sent"
      }
      result.put(verdict)
    })
    assert_true(result.get().contains("max_payload"))
  })
}

///|
async test "subscribe dispatches messages in order" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let received : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      guard conn.subscribe("orders.*", msg => {
          received.put(bytes_text(msg.payload))
        })
        is Ok(_)
      srv.put("MSG orders.new 1 3\r\nabc\r\n")
      srv.put("MSG orders.old 1 2\r\nde\r\n")
      @async.sleep(100)
      let _ = conn.close(timeout_ms=100)
    })
    assert_eq(received.get(), "abc")
    assert_eq(received.get(), "de")
  })
}

///|
async test "a panicking handler does not disturb other subscriptions" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let good_count : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let _ = conn.subscribe("boom.*", _ => raise Failure("handler exploded"))
      let _ = conn.subscribe("ok.*", _ => good_count.put(()))
      srv.put("MSG boom.x 1 1\r\nz\r\n")
      srv.put("MSG ok.y 2 1\r\nq\r\n")
      srv.put("MSG ok.z 2 1\r\nw\r\n")
      @async.sleep(120)
      let _ = conn.close(timeout_ms=100)
    })
    good_count.get()
    good_count.get()
  })
}

///|
async test "unsubscribe stops delivery" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let received : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      guard conn.subscribe("t.*", _ => received.put(())) is Ok(sid)
      srv.put("MSG t.a 1 1\r\nx\r\n")
      received.get()
      let _ = conn.unsubscribe(sid)
      srv.put("MSG t.b 1 1\r\nx\r\n")
      @async.sleep(80)
      assert_true(received.try_get() is None)
      let _ = conn.close(timeout_ms=100)
    })
    @async.sleep(250)
  })
}

///|
async test "request reply round trip" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      // The first request of the connection gets INBOX sequence 1.
      let _ = group.spawn(() => {
        defer_write(srv, 40, "MSG _INBOX.1 9 5\r\nreply\r\n")
      })
      let verdict = match
        conn.request("svc.echo", ascii_bytes("ping!"), timeout_ms=800) {
        Ok(reply) => bytes_text(reply)
        Err(e) => "ERR: \{e.message()}"
      }
      result.put(verdict)
      let _ = conn.close(timeout_ms=100)
    })
    assert_eq(result.get(), "reply")
  })
}

///|
async test "concurrent requests all pair correctly" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[Bool] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let results : Array[@async.Task[Result[Bytes, NatsError]]] = []
      for i in 0..<20 {
        let t = group.spawn(() => {
          conn.request("svc", ascii_bytes("m\{i}"), timeout_ms=2000)
        })
        results.push(t)
      }
      // Answer every INBOX out of order to prove matching is per-inbox.
      let _ = group.spawn(() => {
        @async.sleep(60)
        for j in 0..<20 {
          let payload = "r\{19 - j}"
          srv.put("MSG _INBOX.\{j + 1} 9 \{payload.length()}\r\n\{payload}\r\n")
        }
      })
      let mut all_ok = true
      for i, t in results {
        match t.wait() {
          Ok(reply) => if bytes_text(reply) != "r\{i}" { all_ok = false }
          Err(_) => all_ok = false
        }
      }
      result.put(all_ok)
      let _ = conn.close(timeout_ms=100)
    })
    assert_true(result.get())
  })
}

///|
async test "request without responders fails immediately" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      // 503 headers block "NATS/1.0 503\r\n\r\n" is 16 bytes.
      let _ = group.spawn(() => {
        defer_write(srv, 40, "MSG _INBOX.1 9 16 16\r\nNATS/1.0 503\r\n\r\n")
      })
      let verdict = match
        conn.request("nobody", ascii_bytes("x"), timeout_ms=3000) {
        Err(e) => e.message()
        Ok(_) => "got-response"
      }
      result.put(verdict)
      let _ = conn.close(timeout_ms=100)
    })
    assert_true(result.get().contains("no responders"))
  })
}

///|
async test "request times out when nothing answers" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let verdict = match conn.request("svc", ascii_bytes("x"), timeout_ms=80) {
        Err(e) => e.message()
        Ok(_) => "got-response"
      }
      result.put(verdict)
      let _ = conn.close(timeout_ms=100)
    })
    assert_true(result.get().contains("timeout"))
  })
}

///|
async test "server disconnect aborts in-flight requests" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let verdict = match
        conn.request("svc", ascii_bytes("x"), timeout_ms=5000) {
        Err(e) => e.message()
        Ok(_) => "got-response"
      }
      result.put(verdict)
      let _ = conn.close(timeout_ms=100)
    })
    @async.sleep(60)
    srv.sock.close()
    let text = result.get()
    assert_true(text.contains("connection closed") || text.contains("aborted"))
  })
}

///|
async test "unanswered pings eventually fail the connection" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    srv.answer_pings = false
    let _ = group.spawn(() => {
      let conn = client_dial(port, options={
        ..Options::default(),
        ping_interval_ms: 20,
        max_outstanding_pings: 1,
      })
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      @async.sleep(150)
      let open = conn.is_open()
      let verdict = match
        conn.request("svc", ascii_bytes("x"), timeout_ms=100) {
        Err(e) => "closed:\{open}"
        Ok(_) => "got-response"
      }
      result.put(verdict)
    })
    assert_true(result.get().contains("closed:true"))
  })
}

///|
async test "server pings are answered with pongs" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[Bool] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      srv.put("PING\r\n")
      let ok = match conn.flush(timeout_ms=400) {
        Ok(_) => true
        Err(_) => false
      }
      result.put(ok)
      let _ = conn.close(timeout_ms=100)
    })
    assert_true(result.get())
  })
}

///|
async test "graceful close unsubscribes and flushes before shutdown" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[Bool] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let _ = conn.subscribe("t.*", _ => ())
      let _ = conn.publish("t.a", ascii_bytes("last"))
      // Answer the flush PING once the close flow reaches it.
      let _ = group.spawn(() => defer_write(srv, 40, "PONG\r\n"))
      let first = match conn.close(timeout_ms=500) {
        Ok(_) => true
        Err(_) => false
      }
      let second = match conn.close(timeout_ms=50) {
        Ok(_) => true
        Err(_) => false
      }
      result.put(first && second && !conn.is_open())
    })
    assert_true(result.get())
  })
}

///|
async test "publish after close fails with connection closed" {
  @async.with_task_group(group => {
    let (srv, port) = start_server(group)
    let result : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(port)
      guard conn.handshake() is Ok(_)
      conn.spawn_loops(group)
      let _ = group.spawn(() => defer_write(srv, 20, "PONG\r\n"))
      let _ = conn.close(timeout_ms=100)
      let verdict = match conn.publish("x", ascii_bytes("y")) {
        Err(e) => e.message()
        Ok(_) => "sent"
      }
      result.put(verdict)
    })
    assert_true(result.get().contains("connection closed"))
  })
}
