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
  priv reader : &@io.Reader
  priv writer : &@io.Writer
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
  priv mut next_sid : Int
  priv mut inbox_seq : Int
  priv mut ping_misses : Int
  priv mut handler_errors : Int
  priv mut last_error : String?
  priv mut info : @moonnats.ServerInfo?
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
  options? : Options = Options::default(),
) -> Conn {
  {
    reader,
    writer,
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
    do_close,
    closed_signal: @async.Queue(kind=Unbounded),
    ping_timer: @async.Timer(options.ping_interval_ms),
  }
}

///|
/// Open a connection to `options.host:options.port`, run the INFO/CONNECT
/// handshake (including an echo PING to confirm the server accepted the
/// credentials) and start the read and keepalive loops.
pub async fn connect(
  options? : Options = Options::default(),
) -> Result[Conn, NatsError] {
  let sock = @socket.Tcp::connect_to_host(options.host, port=options.port) catch {
    e => return Err(HandshakeFailed("tcp connect: \{e}"))
  }
  let conn = Conn::make(sock, sock, fn() { sock.close() }, options~)
  match conn.handshake() {
    Err(e) => {
      sock.close()
      Err(e)
    }
    Ok(_) => Ok(conn)
  }
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
pub fn Conn::handler_error_count(self : Conn) -> Int {
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
    (self.do_close)()
  }
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
