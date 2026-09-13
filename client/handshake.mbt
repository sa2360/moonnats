///|
/// INFO/CONNECT handshake: read the server INFO, send CONNECT, then require
/// a PONG (or a categorized -ERR) before declaring the connection usable.
///
/// Must run inside a task (it performs transport I/O directly — the queued
/// writer task is not running yet at this point).
pub async fn Conn::handshake(self : Conn) -> Result[Unit, NatsError] {
  // NOTE: no with_timeout here — it nests a task group, which deadlocks
  // when the call happens inside an already-spawned task. Enforce deadlines
  // from the coordinator (see connect_and_run / connect_and_handshake).
  self.handshake_loop() catch {
    e => Err(HandshakeFailed("\{e}"))
  }
}

///|
/// Blocking write used only during the handshake; the queued writer task is
/// not running yet, so the CONNECT and PING go out directly.
async fn Conn::send_now(self : Conn, bytes : Bytes) -> Result[Unit, NatsError] {
  if self.is_open() {
    try {
      self.writer.write(bytes)
      Ok(())
    } catch {
      e => Err(ConnectionClosed("write failed: \{e}"))
    }
  } else {
    Err(self.closed_error())
  }
}

///|
async fn Conn::handshake_loop(self : Conn) -> Result[Unit, NatsError] {
  let mut info_seen = false
  while true {
    match self.parser.next_op() {
      @moonnats.NeedMore => {
        let chunk = self.reader.read_some()
        match chunk {
          None =>
            return Err(HandshakeFailed("connection closed during handshake"))
          Some(bytes) =>
            if bytes.is_empty() {
              return Err(HandshakeFailed("connection closed during handshake"))
            } else {
              self.parser.feed(bytes)
            }
        }
      }
      @moonnats.Fail(msg) =>
        return Err(HandshakeFailed("malformed server frame: \{msg}"))
      @moonnats.Op(op, _) =>
        match op {
          @moonnats.Info(json) =>
            match @moonnats.parse_server_info(json) {
              Ok(info) => {
                self.info = Some(info)
                if not(info_seen) {
                  info_seen = true
                  let cfg = @moonnats.ConnectConfig::default(
                    name=self.options.name,
                  )
                  let cfg = {
                    ..cfg,
                    user: self.options.user,
                    pass: self.options.pass,
                    auth_token: self.options.token,
                    verbose: self.options.verbose,
                  }
                  let sent = self.send_now(cfg.encode())
                  if sent is Err(e) {
                    return Err(e)
                  }
                  let _ = self.send_now(@moonnats.ping_command())
                }
              }
              Err(e) => return Err(HandshakeFailed("undecodable INFO: \{e}"))
            }
          @moonnats.Pong => return Ok(())
          @moonnats.Ping => {
            let _ = self.send_now(@moonnats.pong_command())
          }
          @moonnats.ErrOp(desc) => {
            let err = categorize_err(desc)
            return Err(err)
          }
          // Servers may legally send +OK (verbose) before the PONG.
          @moonnats.Ok => ()
          @moonnats.Msg(_) | @moonnats.HMsg(_) =>
            return Err(HandshakeFailed("unexpected message during handshake"))
        }
    }
  }
  Err(HandshakeFailed("unreachable"))
}
