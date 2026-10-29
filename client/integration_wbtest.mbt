// Integration tests against a real nats-server.
//
// Gated by MOONNATS_INTEGRATION=1 so plain `moon test` needs no server.
// Once enabled, the expected local NATS servers must be running: 4222 is
// unauthenticated and 24223 accepts user "test" / password "pw".

///|
async fn integration_enabled() -> Bool {
  @env.get_env_var("MOONNATS_INTEGRATION") is Some("1")
}

///|
/// Dial + handshake + start loops; aborts the test on handshake failure.
async fn nats_established(
  group : @async.TaskGroup[Unit],
  port : Int32,
  options~ : Options = Options::default(),
) -> Conn {
  match connect_and_handshake(group, options={ ..options, port, }) {
    Ok(conn) => conn
    Err(e) => abort("integration connection failed: \{e.message()}")
  }
}

///|
async test "integration: wildcard subscribe and delivery" {
  guard integration_enabled() else { return }
  @async.with_task_group(group => {
    let stage = ["connect"]
    let finished = [false]
    let _ = group.spawn(() => {
      for _ in 0..<200 {
        if finished[0] {
          return
        }
        @async.sleep(50)
      }
      abort("wildcard integration timeout at " + stage[0])
    })
    let conn = nats_established(group, 4222)
    stage[0] = "subscribe"
    let received : @async.Queue[String] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("orders.*", msg => {
        received.put(bytes_text(msg.payload))
      })
      is Ok(_)
    stage[0] = "flush before publish"
    guard conn.flush(timeout_ms=1000) is Ok(_) else {
      abort("wildcard integration flush before publish failed")
    }
    stage[0] = "publish"
    let _ = conn.publish("orders.a", ascii_bytes("one"))
    let _ = conn.publish("orders.b", ascii_bytes("two"))
    let _ = conn.publish("orders.c", ascii_bytes("three"))
    stage[0] = "receive"
    assert_eq(received.get(), "one")
    assert_eq(received.get(), "two")
    assert_eq(received.get(), "three")
    finished[0] = true
    stage[0] = "close"
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: queue group load sharing" {
  guard integration_enabled() else { return }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let total : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
    let _ = conn.subscribe("jobs.*", _ => total.put(()), queue_group="workers")
      is Ok(_)
    // A second connection joins the same group.
    let conn2 = nats_established(group, 4222)
    let _ = conn2.subscribe("jobs.*", _ => total.put(()), queue_group="workers")
      is Ok(_)
    let _ = conn2.flush(timeout_ms=1000)
    for i in 0..<10 {
      let _ = conn.publish("jobs.\{i}", ascii_bytes("job"))
    }
    let _ = conn.flush(timeout_ms=1000)
    @async.sleep(200)
    for _ in 0..<10 {
      total.get()
    }
    // Exactly 10 deliveries across the group; drain any duplicates quickly.
    @async.sleep(100)
    assert_true(total.try_get() is None)
    let _ = conn2.close(timeout_ms=500)
    let _ = conn.close(timeout_ms=1000)
  })
}

///|
async test "integration: 100 concurrent requests pair correctly" {
  guard integration_enabled() else { return }
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
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let received : @async.Queue[Int32] = @async.Queue(kind=Unbounded)
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
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    let a_count : @async.Queue[Int32] = @async.Queue(kind=Unbounded)
    let b_count : @async.Queue[Int32] = @async.Queue(kind=Unbounded)
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

///|
async test "integration: connect_and_run subscribe unsubscribe and close" {
  guard integration_enabled() else { return }
  let received : @async.Queue[String] = @async.Queue(kind=Unbounded)
  let outcome = connect_and_run(options={ ..Options::default(), port: 4222, }, conn => {
    let sid = match
      conn.subscribe("lifecycle.*", msg => received.put(bytes_text(msg.payload))) {
      Ok(sid) => sid
      Err(e) => return Err(e)
    }
    match conn.flush(timeout_ms=1000) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    match conn.publish("lifecycle.one", ascii_bytes("before")) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    match conn.flush(timeout_ms=1000) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    if received.get() != "before" {
      return Err(
        ProtocolError("subscription did not receive the first message"),
      )
    }
    match conn.unsubscribe(sid) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    match conn.flush(timeout_ms=1000) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    match conn.publish("lifecycle.two", ascii_bytes("after")) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    match conn.flush(timeout_ms=1000) {
      Err(e) => return Err(e)
      Ok(_) => ()
    }
    Ok(received.try_get() is None)
  })
  assert_eq(outcome, Ok(true))
}

///|
async test "integration: TLS handshake verifies the configured trust root" {
  guard @env.get_env_var("MOONNATS_TLS_INTEGRATION") is Some("1") else {
    return
  }
  let root = match @env.get_env_var("MOONNATS_TLS_CERT") {
    Some(path) => path
    None => abort("MOONNATS_TLS_CERT must point to the test CA certificate")
  }
  let outcome = connect_and_run(
    options={
      ..Options::default(),
      port: 24224,
      tls: true,
      tls_root_certificate: Some(root),
    },
    conn => conn.flush(timeout_ms=2000),
  )
  assert_eq(outcome, Ok(()))
}

///|
async test "integration: TLS handshake-first mode" {
  guard @env.get_env_var("MOONNATS_TLS_INTEGRATION") is Some("1") else {
    return
  }
  let root = match @env.get_env_var("MOONNATS_TLS_CERT") {
    Some(path) => path
    None => abort("MOONNATS_TLS_CERT must point to the test CA certificate")
  }
  let outcome = connect_and_run(
    options={
      ..Options::default(),
      port: 24225,
      tls: true,
      tls_handshake_first: true,
      tls_root_certificate: Some(root),
    },
    conn => conn.flush(timeout_ms=2000),
  )
  assert_eq(outcome, Ok(()))
}

///|
async test "integration: INFO exposes cluster discovery endpoints" {
  guard @env.get_env_var("MOONNATS_CLUSTER_INTEGRATION") is Some("1") else {
    return
  }
  @async.with_task_group(group => {
    let conn = nats_established(group, 4222)
    guard conn.server_info() is Some(info)
    assert_true(info.connect_urls.length() > 0)
    let _ = conn.close(timeout_ms=1000)
  })
}
