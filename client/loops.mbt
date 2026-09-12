///|
/// Reader task: pull chunks from the transport, extract frames, dispatch.
/// Exits when the transport hits EOF or the connection is failed; any exit
/// fails the connection so waiters never hang.
async fn Conn::read_loop(self : Conn) -> Unit {
  try {
    while self.is_open() {
      match self.reader.read_some() {
        None => break
        Some(bytes) => {
          if bytes.is_empty() {
            break
          }
          self.parser.feed_and_compact(bytes)
        }
      }
      let mut dead = false
      while not(dead) {
        match self.parser.next_op() {
          @moonnats.NeedMore => break
          @moonnats.Fail(msg) => {
            let _ = self.fail("protocol error: \{msg}")
            dead = true
          }
          @moonnats.Op(op, _) =>
            match self.handle_op(op) {
              Err(reason) => {
                let _ = self.fail(reason)
                dead = true
              }
              Ok(_) => ()
            }
        }
      }
      if dead {
        return
      }
    }
    let _ = self.fail(self.close_reason())
  } catch {
    e => {
      // Transport read error: treat as an abrupt disconnect.
      let _ = self.fail("read failed: \{e}")
    }
  }
}

///|
/// Handle one server operation. Err means the connection is unusable.
async fn Conn::handle_op(
  self : Conn,
  op : @moonnats.ServerOp,
) -> Result[Unit, String] {
  match op {
    @moonnats.Info(json) =>
      match @moonnats.parse_server_info(json) {
        Ok(info) => {
          self.info = Some(info)
          Ok(())
        }
        Err(e) => Err("undecodable INFO update: \{e}")
      }
    @moonnats.Msg(m) => {
      self.deliver(m)
      Ok(())
    }
    @moonnats.HMsg(h) => {
      // Headers currently carry the no-responders signal only; the payload
      // is delivered to the subscription like a plain MSG.
      self.deliver_hmsg(h)
      Ok(())
    }
    @moonnats.Ping => {
      let sent = self.send(@moonnats.pong_command())
      match sent {
        Err(e) => Err(e.message())
        Ok(_) => Ok(())
      }
    }
    @moonnats.Pong => {
      self.state_lock.acquire()
      self.ping_misses = 0
      let waiters = self.flushers.copy()
      let drain = self.flushers
      self.flushers = []
      self.state_lock.release()
      for waiter in waiters {
        waiter.put(())
      }
      Ok(())
    }
    @moonnats.Ok => Ok(())
    @moonnats.ErrOp(desc) => {
      let err = categorize_err(desc)
      match err {
        AuthFailed(_) =>
          // Fatal: the server rejects who we are.
          Err("server error: \{desc}")
        other => {
          // Non-fatal (e.g. per-subject permission violation): record it.
          self.last_error = Some(other.message())
          Ok(())
        }
      }
    }
  }
}

///|
async fn Conn::deliver(self : Conn, msg : @moonnats.DeliveredMsg) -> Unit {
  let sub = self.sub_for(msg.sid)
  match sub {
    Some(s) =>
      (s.handler)(msg) catch {
        _ => {
          self.state_lock.acquire()
          self.handler_errors += 1
          self.state_lock.release()
        }
      }
    None => ()
  }
}

///|
async fn Conn::deliver_hmsg(self : Conn, msg : @moonnats.DeliveredHMsg) -> Unit {
  // no-responders: headers block carries "NATS/1.0 503 No Responders".
  let is_no_responders = match @moonnats.parse_header_block(msg.headers) {
    Ok(block) => {
      let line = block.version_line
      let lower = line.to_lower()
      lower.contains("503") || lower.contains("no responders")
    }
    Err(_) => false
  }
  self.state_lock.acquire()
  let queue = self.pending.get(msg.sid)
  self.state_lock.release()
  match queue {
    Some(q) =>
      if is_no_responders {
        q.put(NoReply)
      } else {
        q.put(
          Resp(@moonnats.DeliveredMsg::{
            subject: msg.subject,
            sid: msg.sid,
            reply: msg.reply,
            payload: msg.payload,
          }),
        )
      }
    None => {
      // Headers on a regular subscription: deliver the payload part.
      let plain : @moonnats.DeliveredMsg = {
        subject: msg.subject,
        sid: msg.sid,
        reply: msg.reply,
        payload: msg.payload,
      }
      self.deliver(plain)
    }
  }
}

///|
async fn Conn::sub_for(self : Conn, sid : String) -> Subscription? {
  self.state_lock.acquire()
  let sub = self.subs.get(sid)
  self.state_lock.release()
  sub
}

///|
async fn Conn::ping_loop(self : Conn) -> Unit {
  let interval = self.options.ping_interval_ms
  while self.is_open() {
    @async.sleep(interval)
    if not(self.is_open()) {
      return
    }
    self.state_lock.acquire()
    self.ping_misses += 1
    let missed = self.ping_misses
    self.state_lock.release()
    if missed > self.options.max_outstanding_pings {
      let _ = self.fail("server not responding to PINGs (\{missed} misses)")
      return
    }
    let sent = self.send(@moonnats.ping_command())
    if sent is Err(_) {
      return
    }
  }
}

///|
fn categorize_err(desc : String) -> NatsError {
  let lower = desc.to_lower()
  if lower.contains("authorization") || lower.contains("authentication") {
    AuthFailed(desc)
  } else if lower.contains("permission") {
    PermissionDenied(desc)
  } else {
    ProtocolError(desc)
  }
}

///|
/// Spawn the writer, reader and keepalive loops on `group`, then block the
/// **calling** task until the connection is closed or fails.
///
/// Only call this from a task-group coordinator (the `with_task_group`
/// body): spawning onto a group from inside another spawned task of the
/// same group deadlocks. `connect_and_run` handles the plumbing.
pub async fn[G] Conn::run(self : Conn, group : @async.TaskGroup[G]) -> Unit {
  self.spawn_loops(group)
  self.closed_signal.get() catch {
    _ => ()
  }
}

///|
/// Spawn the three service loops. Coordinator-only, see `run`.
async fn[G] Conn::spawn_loops(self : Conn, group : @async.TaskGroup[G]) -> Unit {
  let _ = group.spawn(() => self.writer_loop())
  let _ = group.spawn(() => self.read_loop())
  let _ = group.spawn(() => self.ping_loop())
}

///|
/// Writer task: the only place that touches the transport for writes.
/// Exits when the outbound queue is closed (connection failed/closed).
async fn Conn::writer_loop(self : Conn) -> Unit {
  try {
    while true {
      let bytes = self.outbound.get()
      self.writer.write(bytes)
    }
  } catch {
    _ => ()
  }
}
