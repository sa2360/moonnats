///|
/// Publish a payload; rejects invalid subjects and over-limit payloads
/// locally without touching the network. Delivery is fire-and-forget as per
/// the protocol; use `request` or `flush` when you need confidence.
pub async fn Conn::publish(
  self : Conn,
  subject : String,
  payload : Bytes,
) -> Result[Unit, NatsError] {
  guard self.is_open() else { return Err(self.closed_error()) }
  guard @moonnats.is_valid_subject(subject) else {
    return Err(BadRequest("invalid publish subject: \{subject}"))
  }
  match self.info {
    Some(info) =>
      if info.max_payload > 0 && payload.length() > info.max_payload {
        return Err(
          BadRequest(
            "payload of \{payload.length()} bytes exceeds server max_payload \{info.max_payload}",
          ),
        )
      }
    None => ()
  }
  self.send(
    @moonnats.encode_command(
      @moonnats.Pub(@moonnats.Publish::{ subject, reply: None, payload, }),
    ),
  )
}

///|
/// Register a handler for a subject (wildcards allowed) and send SUB.
///
/// Handlers run in order on the reader task: a slow handler delays delivery
/// for everyone, so spawn your own task for heavy work. A handler that
/// raises is caught and counted (`handler_error_count`); the connection and
/// other subscriptions are unaffected.
///
/// Returns the sid to pass to `unsubscribe`.
pub async fn Conn::subscribe(
  self : Conn,
  subject : String,
  handler : async (@moonnats.DeliveredMsg) -> Unit,
  queue_group? : String,
) -> Result[String, NatsError] {
  guard self.is_open() else { return Err(self.closed_error()) }
  guard @moonnats.is_valid_subject(subject, wildcards=true) else {
    return Err(BadRequest("invalid subscription subject: \{subject}"))
  }
  self.state_lock.acquire()
  self.next_sid += 1
  let sid = self.next_sid.to_string()
  self.subs[sid] = { subject, queue_group, handler, }
  self.state_lock.release()
  let cmd = @moonnats.Sub(@moonnats.Subscribe::{ subject, sid, queue_group, })
  match self.send(@moonnats.encode_command(cmd)) {
    Err(e) => {
      self.state_lock.acquire()
      let _ = self.subs.remove(sid)
      self.state_lock.release()
      return Err(e)
    }
    Ok(_) => ()
  }
  Ok(sid)
}

///|
/// Stop a subscription: the sid is released immediately, the server is told
/// via UNSUB (best effort when already disconnected) and no further messages
/// are delivered to the handler.
pub async fn Conn::unsubscribe(
  self : Conn,
  sid : String,
) -> Result[Unit, NatsError] {
  self.state_lock.acquire()
  let _ = self.subs.remove(sid)
  self.state_lock.release()
  let cmd = @moonnats.Unsub(@moonnats.Unsubscribe::{ sid, max_msgs: None, })
  let result = self.send(@moonnats.encode_command(cmd))
  match result {
    Err(ConnectionClosed(_)) => Ok(())
    other => other
  }
}

///|
/// Send a request and wait for a single reply, with an explicit timeout.
/// Requests are independent: many can be in flight at once, replies are
/// matched per request by auto-generated INBOX subject. When the server
/// supports headers, a request with no responders fails immediately with
/// `NoResponders` instead of burning the timeout.
pub async fn Conn::request(
  self : Conn,
  subject : String,
  payload : Bytes,
  timeout_ms? : Int,
) -> Result[Bytes, NatsError] {
  let timeout = match timeout_ms {
    Some(ms) => ms
    None => self.options.default_request_timeout_ms
  }
  guard self.is_open() else { return Err(self.closed_error()) }
  guard @moonnats.is_valid_subject(subject) else {
    return Err(BadRequest("invalid request subject: \{subject}"))
  }
  // Allocate the reply inbox and register its response queue under the sid
  // the SUB command will carry.
  self.state_lock.acquire()
  self.inbox_seq += 1
  let inbox = "\{self.options.inbox_prefix}.\{self.inbox_seq}"
  self.next_sid += 1
  let sid = self.next_sid.to_string()
  let queue : @async.Queue[Response] = @async.Queue(kind=Unbounded)
  self.pending[sid] = queue
  self.state_lock.release()
  // Subscribe to the inbox before publishing so the no-responders HMSG
  // (and any reply) can arrive.
  let sub = @moonnats.Sub(@moonnats.Subscribe::{
    subject: inbox,
    sid,
    queue_group: None,
  })
  let subscribed = self.send(@moonnats.encode_command(sub))
  if subscribed is Err(e) {
    self.drop_request(sid)
    return Err(e)
  }
  let pub_cmd = @moonnats.Pub(@moonnats.Publish::{
    subject,
    reply: Some(inbox),
    payload,
  })
  let published = self.send(@moonnats.encode_command(pub_cmd))
  if published is Err(e) {
    let _ = self.send(
      @moonnats.encode_command(
        @moonnats.Unsub(@moonnats.Unsubscribe::{ sid, max_msgs: None, }),
      ),
    )
    self.drop_request(sid)
    return Err(e)
  }
  let outcome = poll_response(queue, timeout, subject)
  // Best-effort unsubscribe; ignore results.
  let _ = self.send(
    @moonnats.encode_command(
      @moonnats.Unsub(@moonnats.Unsubscribe::{ sid, max_msgs: None, }),
    ),
  )
  self.drop_request(sid)
  outcome
}

///|
/// Round-trip to the server: send PING and wait for PONG, guaranteeing
/// everything written before this call has been processed.
pub async fn Conn::flush(
  self : Conn,
  timeout_ms? : Int,
) -> Result[Unit, NatsError] {
  let timeout = match timeout_ms {
    Some(ms) => ms
    None => self.options.default_request_timeout_ms
  }
  guard self.is_open() else { return Err(self.closed_error()) }
  let pong_seen : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
  self.state_lock.acquire()
  self.flushers.push(pong_seen)
  self.state_lock.release()
  let sent = self.send(@moonnats.ping_command())
  if sent is Err(e) {
    return Err(e)
  }
  let outcome = poll_pong(pong_seen, timeout)
  self.state_lock.acquire()
  remove_first(self.flushers, pong_seen)
  self.state_lock.release()
  outcome
}

///|
/// Close the connection gracefully: unsubscribe everything, flush to the
/// server with a PING/PONG round-trip, then tear down. Idempotent; returns
/// the flush error (still closing) when the round-trip fails.
pub async fn Conn::close(
  self : Conn,
  timeout_ms? : Int,
) -> Result[Unit, NatsError] {
  if self.status is Closed(_) {
    return Ok(())
  }
  let timeout = match timeout_ms {
    Some(ms) => ms
    None => self.options.default_request_timeout_ms
  }
  self.state_lock.acquire()
  let sids : Array[String] = []
  for sid, _ in self.subs {
    sids.push(sid)
  }
  self.state_lock.release()
  let mut result = Ok(())
  for sid in sids {
    match self.unsubscribe(sid) {
      Err(e) => result = Err(e)
      Ok(_) => ()
    }
  }
  match self.flush(timeout_ms=timeout) {
    Ok(_) => ()
    Err(e) => result = Err(e)
  }
  let _ = self.fail("client closed")
  result
}

///|
async fn Conn::drop_request(self : Conn, sid : String) -> Unit {
  self.state_lock.acquire()
  let _ = self.pending.remove(sid)
  self.state_lock.release()
}

///|
/// Open a connection, run the handshake, start the reader and keepalive
/// loops, then hand the ready connection to `body`. When `body` returns —
/// or the connection fails — everything is torn down. The intended entry
/// point for applications.
pub async fn[X] connect_and_run(
  options? : Options = Options::default(),
  body : async (Conn) -> Result[X, NatsError],
) -> Result[X, NatsError] {
  @async.with_task_group(group => {
    // TCP connect and handshake perform transport I/O, so they run in their
    // own task; the coordinator only waits on the readiness queue.
    let ready : @async.Queue[Result[Conn, NatsError]] = @async.Queue(
      kind=Unbounded,
    )
    let _ = group.spawn(() => {
      @fs.write_file("C:/Users/sa/AppData/Local/Temp/moonnats_dbg/t1", "x")
      let r = connect(options~)
      @fs.write_file("C:/Users/sa/AppData/Local/Temp/moonnats_dbg/t2", "x")
      ready.put(r)
    })
    let conn = match ready.get() {
      Err(e) => return Err(e)
      Ok(c) => c
    }
    conn.spawn_loops(group)
    @fs.write_file("C:/Users/sa/AppData/Local/Temp/moonnats_dbg/t3", "x")
    let outcome = body(conn)
    @fs.write_file("C:/Users/sa/AppData/Local/Temp/moonnats_dbg/t4", "x")
    let _ = conn.close()
    @fs.write_file("C:/Users/sa/AppData/Local/Temp/moonnats_dbg/t5", "x")
    outcome
  })
}

///|
fn[T] remove_first(arr : Array[T], value : T) -> Unit {
  for i in 0..<arr.length() {
    if physical_equal(arr[i], value) {
      let _ = arr.remove(i)
      return
    }
  }
}

///|
/// Spawn the handshake on `group` and race it against a deadline timer from
/// the coordinator. Uses Task::cancel instead of with_timeout, because
/// with_timeout nests a task group and must not run inside a spawned task.
async fn handshake_with_deadline(
  group : @async.TaskGroup[Unit],
  conn : Conn,
  ready : @async.Queue[String],
) -> Unit {
  let hs_task = group.spawn(() => {
    let outcome = conn.handshake()
    ready.put(
      match outcome {
        Ok(_) => "OK"
        Err(e) => e.message()
      },
    )
  })
  let timer = @async.Timer(conn.options.handshake_timeout_ms)
  let timed_out : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
  let _ = group.spawn(() => {
    timer.wait()
    hs_task.cancel()
    timed_out.put(())
  })
  // Whoever finishes first: the handshake result or the deadline.
  @async.with_task_group(_unused => {
    // wait on either queue via sequential polling is not needed: both tasks
    // push exactly one result, so a plain get would block on the loser.
    // Instead, wait for the handshake first and let the timer be cancelled;
    // the timer task only fires if the handshake is genuinely stuck, in
    // which case cancel() aborts its blocked read and ready receives the
    // cancellation error message.
    let text = ready.get()
    timer.cancel()
    ignore(timed_out)
    ready.put(text)
  })
}

///|
/// Dial, handshake, and start the connection loops — with all transport
/// I/O performed on the calling (coordinator) task. Intended for `async
/// fn main` programs (CLI tools) where spawned-task I/O scheduling can be
/// unreliable; applications that prefer the structured form can spawn
/// `connect_and_run`'s equivalent themselves.
pub async fn connect_and_handshake(
  group : @async.TaskGroup[Unit],
  options? : Options = Options::default(),
) -> Result[Conn, NatsError] {
  match connect(options~) {
    Err(e) => Err(e)
    Ok(c) => {
      c.spawn_loops(group)
      Ok(c)
    }
  }
}

///|
/// Poll the pong queue for `timeout_ms`; avoids with_timeout because its
/// nested task group deadlocks inside spawned tasks.
async fn poll_pong(
  queue : @async.Queue[Unit],
  timeout_ms : Int,
) -> Result[Unit, NatsError] {
  let rounds = timeout_ms / 10 + 1
  for _ in 0..<rounds {
    match (try? queue.try_get()) {
      Ok(Some(_)) => return Ok(())
      Ok(None) => @async.sleep(10)
      Err(_) => return Err(ConnectionClosed("connection closed while flushing"))
    }
  }
  Err(Timeout("no PONG within \{timeout_ms}ms"))
}

///|
/// Poll the response queue for `timeout_ms`; see poll_pong.
async fn poll_response(
  queue : @async.Queue[Response],
  timeout_ms : Int,
  subject : String,
) -> Result[Bytes, NatsError] {
  let rounds = timeout_ms / 10 + 1
  for _ in 0..<rounds {
    match (try? queue.try_get()) {
      Ok(Some(Resp(msg))) => return Ok(msg.payload)
      Ok(Some(NoReply)) =>
        return Err(NoResponders("no responders for \{subject}"))
      Ok(None) => @async.sleep(10)
      Err(_) => return Err(ConnectionClosed("connection closed while waiting"))
    }
  }
  Err(Timeout("no response within \{timeout_ms}ms"))
}
