// Integration tests against a real nats-server.
//
// Gated by the MOONNATS_INTEGRATION=1 environment variable so that plain
// `moon test` never requires a server. CI starts nats-server (no auth on
// 4222; auth server with user "test" / pass "pw" on 24223) and sets the
// variable. When the gate is off or no server answers, tests skip.

///|
let nats_host : String = "127.0.0.1"

///|
async fn integration_enabled() -> Bool {
  @env.get_env_var("MOONNATS_INTEGRATION") is Some("1")
}

///|
/// Probe a port with a short deadline; false when nothing answers.
async fn probe_connect(port : Int) -> Bool {
  try {
    let sock = @socket.Tcp::connect_to_host(nats_host, port~)
    sock.close()
    true
  } catch {
    _ => false
  }
}

///|
async fn port_up(port : Int) -> Bool {
  @async.with_task_group(group => {
    let done : @async.Queue[Bool] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => done.put(probe_connect(port)))
    @async.with_timeout_opt(1500, () => done.get()) is Some(true)
  })
}

///|
/// Dial + handshake + start loops; aborts the test on handshake failure.
async fn nats_established(
  group : @async.TaskGroup[Unit],
  port : Int,
  options? : Options = Options::default(),
) -> Conn {
  let ready : @async.Queue[Conn] = @async.Queue(kind=Unbounded)
  let _ = group.spawn(() => {
    let conn = client_dial(port, options~)
    match conn.handshake() {
      Ok(_) => ready.put(conn)
      Err(e) => abort("integration handshake failed: \{e.message()}")
    }
  })
  ready.get()
}

///|
async test "integration: wildcard subscribe and delivery" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let received : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("orders.*", msg => {
        received.put(bytes_text(msg.payload))
      })
      is Ok(_)
    let _ = conn.flush(timeout_ms=1000)
    let _ = conn.publish("orders.a", ascii_bytes("one"))
    let _ = conn.publish("orders.b", ascii_bytes("two"))
    let _ = conn.publish("orders.c", ascii_bytes("three"))
    assert_eq(received.get(), "one")
    assert_eq(received.get(), "two")
    assert_eq(received.get(), "three")
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: queue group load sharing" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let total : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("jobs.*", _ => total.put(()), queue_group="workers")
      is Ok(_)
    // A second connection joins the same group.
    let _ = group.spawn(() => {
      let conn2 = nats_established(group, 4222)
      let _ = conn2.subscribe(
          "jobs.*",
          _ => total.put(()),
          queue_group="workers",
        )
        is Ok(_)
      let _ = conn2.flush(timeout_ms=1000)
      for i in 0..<10 {
        let _ = conn.publish("jobs.\{i}", ascii_bytes("job"))
      }
      let _ = conn.flush(timeout_ms=1000)
      @async.sleep(200)
      let _ = conn2.close(timeout_ms=500)
    })
    for _ in 0..<10 {
      total.get()
    }
    // Exactly 10 deliveries across the group; drain any duplicates quickly.
    @async.sleep(100)
    assert_true(total.try_get() is None)
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: 100 concurrent requests pair correctly" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    // Self responder: echo every request back to its reply subject.
    let _ = conn.subscribe("svc.echo", msg => {
        match msg.reply {
          Some(reply) => {
            let _ = conn.publish(reply, msg.payload)
          }
          None => ()
        }
      })
      is Ok(_)
    let _ = conn.flush(timeout_ms=1000)
    let results : Array[@async.Task[Bool]] = []
    for i in 0..<100 {
      let t = group.spawn(() => {
        match conn.request("svc.echo", ascii_bytes("m\{i}"), timeout_ms=3000) {
          Ok(reply) => bytes_text(reply) == "m\{i}"
          Err(_) => false
        }
      })
      results.push(t)
    }
    let mut all_ok = true
    for t in results {
      if not(t.wait()) {
        all_ok = false
      }
    }
    assert_true(all_ok)
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: no responders fails fast" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let start : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
    let elapsed_ok : @async.Queue[Bool] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      start.put(())
      let r = conn.request("nobody.home", ascii_bytes("x"), timeout_ms=3000)
      ready_text.put(
        match r {
          Err(e) => e.message()
          Ok(_) => "got-response"
        },
      )
      elapsed_ok.put(true)
    })
    start.get()
    let text = ready_text.get()
    assert_true(text.contains("no responders"))
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
let ready_text : @async.Queue[String] = @async.Queue(kind=Unbounded)

///|
async test "integration: binary payload survives the wire" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let received : @async.Queue[Bytes] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("bin.*", msg => received.put(msg.payload)) is Ok(_)
    let payload = Bytes::from_array([
      (0).to_byte(),
      (10).to_byte(),
      (13).to_byte(),
      (255).to_byte(),
      (7).to_byte(),
    ])
    let _ = conn.publish("bin.raw", payload)
    let _ = conn.flush(timeout_ms=1000)
    assert_eq(received.get(), payload)
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: 64KB payload" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let received : @async.Queue[Int] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("big.*", msg => received.put(msg.payload.length()))
      is Ok(_)
    let payload = Bytes::make(65536, (65).to_byte())
    let _ = conn.publish("big.one", payload)
    let _ = conn.flush(timeout_ms=2000)
    assert_eq(received.get(), 65536)
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: auth server rejects wrong credentials" {
  guard integration_enabled() else { return }
  guard port_up(24223) else { return }
  @async.with_task_group(group => {
    let ready : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(24223, options={
        ..Options::default(),
        user: Some("test"),
        pass: Some("wrong"),
      })
      let outcome = conn.handshake()
      ready.put(
        match outcome {
          Ok(_) => "OK"
          Err(e) => e.message()
        },
      )
    })
    let text = ready.get()
    assert_true(text.contains("auth"))
  })
}

///|
async test "integration: auth server accepts correct credentials" {
  guard integration_enabled() else { return }
  guard port_up(24223) else { return }
  @async.with_task_group(group => {
    let ready : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = group.spawn(() => {
      let conn = client_dial(24223, options={
        ..Options::default(),
        user: Some("test"),
        pass: Some("pw"),
      })
      let outcome = conn.handshake()
      ready.put(
        match outcome {
          Ok(_) => "OK"
          Err(e) => e.message()
        },
      )
    })
    assert_eq(ready.get(), "OK")
  })
}

///|
async test "integration: multiple subscriptions interleaved" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let a_count : @async.Queue[Int] = @async.Queue(kind=Unbounded)
    let b_count : @async.Queue[Int] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("multi.a", _ => a_count.put(1)) is Ok(_)
    let _ = conn.subscribe("multi.b", _ => b_count.put(1)) is Ok(_)
    let _ = conn.flush(timeout_ms=1000)
    for _ in 0..<5 {
      let _ = conn.publish("multi.a", ascii_bytes("a"))
      let _ = conn.publish("multi.b", ascii_bytes("b"))
    }
    let _ = conn.flush(timeout_ms=1000)
    let mut a_total = 0
    let mut b_total = 0
    for _ in 0..<5 {
      a_total += a_count.get()
      b_total += b_count.get()
    }
    assert_eq(a_total, 5)
    assert_eq(b_total, 5)
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: graceful close with server round trip" {
  guard integration_enabled() else { return }
  guard port_up(4222) else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let _ = conn.subscribe("t.*", _ => ())
    let _ = conn.publish("t.final", ascii_bytes("bye"))
    // The server answers the close-flow PING itself.
    guard conn.close(timeout_ms=2000) is Ok(_)
    assert_false(conn.is_open())
    guard conn.close(timeout_ms=200) is Ok(_)
  })
}
