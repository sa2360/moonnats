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
  let outcome = try {
    match @async.with_timeout_opt(timeout, () => queue.get()) {
      Some(Resp(msg)) => Ok(msg.payload)
      Some(NoReply) =>
        Err(NatsError::NoResponders("no responders for \{subject}"))
      None => Err(Timeout("no response within \{timeout}ms"))
    }
  } catch {
    e => Err(ConnectionClosed("aborted while waiting: \{e}"))
  }
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
  let outcome = try {
    match @async.with_timeout_opt(timeout, () => pong_seen.get()) {
      Some(_) => Ok(())
      None => Err(Timeout("no PONG within \{timeout}ms"))
    }
  } catch {
    e => Err(ConnectionClosed("aborted while flushing: \{e}"))
  }
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
      let r = connect(options~)
      ready.put(r)
    })
    let conn = match ready.get() {
      Err(e) => return Err(e)
      Ok(c) => c
    }
    conn.spawn_loops(group)
    let outcome = body(conn)
    let _ = conn.close()
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
