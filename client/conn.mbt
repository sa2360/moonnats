///|
priv enum Status {
  Open
  Closed(String)
} derive(Eq, Debug)

///|
struct Subscription {
  subject : String
  queue_group : String?
  handler : async (@moonnats.DeliveredMsg) -> Unit
}

///|
/// A live client connection. All public methods return `Result` and never
/// raise; after a failure every pending operation is aborted with
/// `ConnectionClosed`.
pub struct Conn {
  priv mut reader : &@io.Reader
  priv mut writer : &@io.Writer
  priv raw_socket : @socket.Tcp?
  priv options : Options
  priv parser : @moonnats.Parser
  /// All socket writes go through this queue and the dedicated writer task,
  /// so callers never touch the transport directly (and never race).
  priv outbound : @async.Queue[Bytes]
  priv state_lock : @async.Mutex
  priv mut status : Status
  /// sid -> externally registered subscription
  priv subs : Map[String, Subscription]
  /// sid -> queue delivering responses of one in-flight request
  priv pending : Map[String, @async.Queue[Response]]
  /// sid of the internal request subscription currently active, per inbox
  priv mut flushers : Array[@async.Queue[Unit]]
  priv mut next_sid : Int32
  priv mut inbox_seq : Int32
  priv mut ping_misses : Int32
  priv mut handler_errors : Int32
  priv mut last_error : String?
  priv mut info : @moonnats.ServerInfo?
  priv mut tls_transport : @tls.Tls?
  priv info_updates : @async.Queue[@moonnats.ServerInfo]
  priv do_close : () -> Unit
  /// Signalled when the connection leaves the Open state; run() waits on it.
  priv closed_signal : @async.Queue[Unit]
  /// Keepalive timer; cancelled on close so ping_loop can exit promptly.
  priv ping_timer : @async.Timer
}

///|
priv enum Response {
  Resp(@moonnats.DeliveredMsg)
  NoReply
}

///|
/// Build a connection over arbitrary transport. Used by `connect` (with a
/// `Tcp` socket) and by tests (with in-memory pipes).
pub fn Conn::make(
  reader : &@io.Reader,
  writer : &@io.Writer,
  do_close : () -> Unit,
  options~ : Options = Options::default(),
) -> Conn {
  Conn::make_inner(reader, writer, do_close, options, None, None)
}

///|
fn Conn::make_inner(
  reader : &@io.Reader,
  writer : &@io.Writer,
  do_close : () -> Unit,
  options : Options,
  raw_socket : @socket.Tcp?,
  tls_transport : @tls.Tls?,
) -> Conn {
  {
    reader,
    writer,
    raw_socket,
    options,
    parser: @moonnats.Parser::new(),
    outbound: @async.Queue(kind=Unbounded),
    state_lock: @async.Mutex(),
    status: Open,
    subs: Map::new(),
    pending: Map::new(),
    flushers: [],
    next_sid: 0,
    inbox_seq: 0,
    ping_misses: 0,
    handler_errors: 0,
    last_error: None,
    info: None,
    tls_transport,
    info_updates: @async.Queue(kind=Unbounded),
    do_close,
    closed_signal: @async.Queue(kind=Unbounded),
    ping_timer: @async.Timer(options.ping_interval_ms),
  }
}

///|
/// Open a connection to `options.host:options.port` and run the INFO/CONNECT
/// handshake directly (including an echo PING). Use `connect_and_run` or
/// `connect_and_handshake` when the handshake must be bounded by the configured
/// deadline. This low-level helper does not start the reader, writer, or
/// keepalive loops; use `connect_and_run` for a managed connection lifecycle.
pub async fn connect(
  options~ : Options = Options::default(),
) -> Result[Conn, NatsError] {
  match connect_socket(options) {
    Err(e) => Err(e)
    Ok(conn) =>
      match conn.handshake() {
        Err(e) => {
          let _ = conn.fail(e.message())
          Err(e)
        }
        Ok(_) => Ok(conn)
      }
  }
}

///|
/// Open the TCP transport without starting the protocol handshake. The
/// structured entry points use this so they can enforce the handshake
/// deadline from their task-group coordinator.
async fn connect_socket(options : Options) -> Result[Conn, NatsError] {
  let sock = @socket.Tcp::connect_to_host(options.host, port=options.port) catch {
    e => return Err(HandshakeFailed("tcp connect: \{e}"))
  }
  let conn = Conn::make_inner(
    sock,
    sock,
    fn() { sock.close() },
    options,
    Some(sock),
    None,
  )
  Ok(conn)
}

///|
/// The server info captured during the handshake (empty until connected).
pub fn Conn::server_info(self : Conn) -> @moonnats.ServerInfo? {
  self.info
}

///|
/// The most recent non-fatal server error (e.g. a permission violation), if any.
pub fn Conn::last_error(self : Conn) -> String? {
  self.last_error
}

///|
/// Number of subscription handlers that panicked so far (see `subscribe`).
pub fn Conn::handler_error_count(self : Conn) -> Int32 {
  self.handler_errors
}

///|
/// True while the connection is usable.
pub fn Conn::is_open(self : Conn) -> Bool {
  !(self.status is Closed(_))
}

///|
fn Conn::close_reason(self : Conn) -> String {
  match self.status {
    Closed(reason) => reason
    Open => "open"
  }
}

///|
fn Conn::closed_error(self : Conn) -> NatsError {
  ConnectionClosed(self.close_reason())
}

///|
/// Transition to Closed once; abort every waiter with an error and tear down
/// the transport. Later calls are no-ops.
async fn Conn::fail(self : Conn, reason : String) -> Unit {
  self.state_lock.acquire()
  let already = self.status is Closed(_)
  if !already {
    self.status = Closed(reason)
    let closed_err = ConnectionClosed(reason)
    self.outbound.close(error=closed_err)
    for _, queue in self.pending {
      queue.close(error=closed_err)
    }
    for queue in self.flushers {
      queue.close(error=closed_err)
    }
  }
  self.state_lock.release()
  if !already {
    self.closed_signal.put(()) catch {
      _ => ()
    }
    match self.tls_transport {
      Some(tls) => tls.close()
      None => ()
    }
    (self.do_close)()
  }
}

///|
/// Upgrade the connected TCP stream to TLS after a plaintext INFO line.
async fn Conn::upgrade_tls(self : Conn) -> Result[Unit, NatsError] {
  if self.tls_transport is Some(_) {
    return Ok(())
  }
  guard self.options.tls || self.options.tls_handshake_first else {
    return Err(HandshakeFailed("server requires TLS but TLS is disabled"))
  }
  // The socket is captured by the connection's close closure, while this
  // wrapper replaces both protocol-facing stream interfaces.
  let sock = match self.raw_socket {
    Some(sock) => sock
    None => return Err(HandshakeFailed("TLS upgrade requires a TCP transport"))
  }
  let trust = match self.options.tls_root_certificate {
    Some(path) => @tls.CustomPemFile(path)
    None => @tls.SystemRoot
  }
  let tls = @tls.Tls::client(sock, host=self.options.host, trust~) catch {
    e => return Err(HandshakeFailed("TLS handshake failed: \{e}"))
  }
  self.reader = tls
  self.writer = tls
  self.tls_transport = Some(tls)
  Ok(())
}

///|
async fn Conn::send(self : Conn, bytes : Bytes) -> Result[Unit, NatsError] {
  if self.is_open() {
    try {
      self.outbound.put(bytes)
      Ok(())
    } catch {
      e => Err(ConnectionClosed("write failed: \{e}"))
    }
  } else {
    Err(self.closed_error())
  }
}
