// Fake nats-server over real TCP, used to test client logic.
//
// In-memory pipes turned out to be a poor fit for the client's concurrency
// (write-pause semantics and multi-reader states deadlock in ways real
// sockets never do), so each test runs an actual TcpServer on loopback with
// its own port and speaks the protocol for real.

///|
let fake_server_port : Array[Int] = [24100]

///|
fn next_test_port() -> Int {
  fake_server_port[0] += 1
  fake_server_port[0]
}

///|
struct FakeServer {
  sock : @socket.Tcp
  mut max_payload : Int
  mut answer_pings : Bool
}

///|
async fn FakeServer::put(self : FakeServer, text : String) -> Unit {
  self.sock.write(ascii_bytes(text))
}

///|
async fn FakeServer::send_info(self : FakeServer) -> Unit {
  let maxp = if self.max_payload > 0 {
    self.max_payload.to_string()
  } else {
    "1048576"
  }
  self.put(
    "INFO {\"server_id\":\"FAKE\",\"version\":\"2.10.0\",\"host\":\"127.0.0.1\",\"port\":4222,\"headers\":true,\"max_payload\":\{maxp}}\r\n",
  )
}

///|
/// Read one CRLF-terminated line (the terminator is stripped).
async fn FakeServer::read_line(self : FakeServer) -> String? {
  let line = self.sock.read_until("\r\n") catch { _ => None }
  match line {
    Some(text) => Some(text[:text.length() - 2].to_owned())
    None => None
  }
}

///|
/// Read the payload of a PUB/HPUB whose header line was just consumed.
async fn FakeServer::read_payload(self : FakeServer, header : String) -> Bytes? {
  let size = payload_size_of(header)
  match size {
    None => None
    Some(n) =>
      if n == 0 {
        let crlf = self.sock.read_exactly(2) catch { _ => Bytes::new(0) }
        ignore(crlf)
        Some(Bytes::new(0))
      } else {
        let body = self.sock.read_exactly(n) catch { _ => return None }
        let crlf = self.sock.read_exactly(2) catch { _ => Bytes::new(0) }
        ignore(crlf)
        Some(body)
      }
  }
}

///|
fn payload_size_of(header : String) -> Int? {
  let tokens = header.split(" ")
  let mut size : Int? = None
  for token in tokens {
    let text = token.to_owned()
    let mut all_digits = text.length() > 0
    for ch in text {
      if ch < '0' || ch > '9' {
        all_digits = false
        break
      }
    }
    if all_digits {
      size = Some(int_of_digits(text))
    }
  }
  size
}

///|
fn int_of_digits(text : String) -> Int {
  let mut value = 0
  for ch in text {
    value = value * 10 + (ch.to_int() - '0'.to_int())
  }
  value
}

///|
/// Accept one client connection on a fresh loopback port. Returns the
/// connected FakeServer and the port the client must dial.
async fn start_server(group : @async.TaskGroup[Unit]) -> (FakeServer, Int) {
  let port = next_test_port()
  let ready : @async.Queue[FakeServer] = @async.Queue(kind=Unbounded)
  let _ = group.spawn(() => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:\{port}"))
    let (sock, _) = server.accept()
    let srv : FakeServer = { sock, max_payload: 0, answer_pings: true, }
    ready.put(srv)
  })
  let srv = ready.get()
  (srv, port)
}

///|
/// Client side matching `start_server`: dial and build the (unhandshaken)
/// connection.
async fn client_dial(
  port : Int,
  options? : Options = Options::default(),
) -> Conn {
  let sock = @socket.Tcp::connect_to_host("127.0.0.1", port~)
  Conn::make(sock, sock, fn() { sock.close() }, options~)
}

///|
/// Server side of the handshake: consume CONNECT and PING, answer PONG.
/// Returns false when the connection died first.
async fn FakeServer::finish_handshake(self : FakeServer) -> Bool {
  let first = self.read_line()
  if first is None {
    return false
  }
  let ping = self.read_line()
  match ping {
    Some(text) =>
      if text == "PING" && self.answer_pings {
        self.put("PONG\r\n")
        return true
      }
    None => ()
  }
  false
}

///|
/// Schedule a deferred server write (e.g. a PONG for a later flush).
async fn defer_write(srv : FakeServer, delay_ms : Int, text : String) -> Unit {
  @async.sleep(delay_ms)
  srv.put(text)
}

///|
fn ascii_bytes(s : String) -> Bytes {
  let out : Array[Byte] = []
  for ch in s {
    out.push(ch.to_int().to_byte())
  }
  Bytes::from_array(out)
}

///|
fn bytes_text(b : Bytes) -> String {
  let sb = StringBuilder()
  for byte in b {
    sb.write_char((byte.to_int() & 0x7F).unsafe_to_char())
  }
  sb.to_string()
}
