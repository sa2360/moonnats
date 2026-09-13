// moonnats command line tool.
//
// Usage:
//   moonnats_cli pub <subject> <message>   publish one message
//   moonnats_cli sub <subject>             subscribe and print messages
//
// Connection settings via environment variables:
//   NATS_HOST (default 127.0.0.1), NATS_PORT (default 4222).
//
// The CLI speaks the protocol directly over its own socket. This
// coordinator-direct style is deliberately kept: moonbitlang/async 0.21.3
// shows task-scheduling quirks in `async fn main` programs when connection
// work happens inside spawned tasks (see README known issues).

///|
async fn main {
  let args = @env.args()
  if args.length() < 2 {
    usage()
    return
  }
  let host = @env.get_env_var("NATS_HOST").unwrap_or("127.0.0.1")
  let port = match @env.get_env_var("NATS_PORT") {
    Some(text) =>
      match parse_port(text) {
        Some(n) => n
        None => {
          println("invalid NATS_PORT: \{text}")
          return
        }
      }
    None => 4222
  }
  match args[1] {
    "pub" => run_pub(args, host, port)
    "sub" => run_sub(args, host, port)
    "help" | "-h" | "--help" => usage()
    other => {
      println("unknown command: \{other}")
      usage()
    }
  }
}

///|
fn usage() -> Unit {
  println("moonnats - NATS command line tool")
  println("")
  println("usage:")
  println("  moonnats_cli pub <subject> <message>   publish one message")
  println(
    "  moonnats_cli sub <subject>             subscribe and print messages",
  )
  println("")
  println("environment: NATS_HOST, NATS_PORT, NATS_USER, NATS_PASS, NATS_TOKEN")
}

///|
fn parse_port(text : String) -> Int? {
  if text.is_empty() {
    return None
  }
  let mut value = 0
  for ch in text {
    guard ch >= '0' && ch <= '9' else { return None }
    value = value * 10 + (ch.to_int() - '0'.to_int())
  }
  if value > 0 && value < 65536 {
    Some(value)
  } else {
    None
  }
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
    let c = byte.to_int()
    if c >= 32 && c < 127 {
      sb.write_char(c.unsafe_to_char())
    } else {
      sb.write_string("hex-\{c.to_string(radix=16)}")
    }
  }
  sb.to_string()
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
/// Connect, read INFO, send CONNECT, and confirm with a PING/PONG round
/// trip. Returns false when the server is unreachable or rejects us.
async fn handshake(sock : @socket.Tcp) -> Bool {
  let _ = sock.read_until("\r\n")
  sock.write(
    ascii_bytes(
      "CONNECT {\"verbose\":false,\"lang\":\"moonbit\",\"version\":\"0.1.0\"}\r\n",
    ),
  )
  sock.write(ascii_bytes("PING\r\n"))
  let pong = sock.read_until("PONG\r\n")
  pong is Some(_)
}

///|
async fn run_pub(args : Array[String], host : String, port : Int) -> Unit {
  if args.length() != 4 {
    println("usage: moonnats_cli pub <subject> <message>")
    return
  }
  let subject = args[2]
  let payload = args[3]
  let sock = @socket.Tcp::connect_to_host(host, port~)
  if not(handshake(sock)) {
    println("error: handshake failed")
    sock.close()
    return
  }
  sock.write(
    ascii_bytes("PUB \{subject} \{payload.length()}\r\n\{payload}\r\nPING\r\n"),
  )
  let ack = sock.read_until("PONG\r\n")
  sock.close()
  match ack {
    Some(_) => println("published to \{subject}")
    None => println("error: no acknowledgement from server")
  }
}

///|
async fn run_sub(args : Array[String], host : String, port : Int) -> Unit {
  if args.length() != 3 {
    println("usage: moonnats_cli sub <subject>")
    return
  }
  let subject = args[2]
  let sock = @socket.Tcp::connect_to_host(host, port~)
  let _ = sock.read_until("\r\n")
  sock.write(
    ascii_bytes(
      "CONNECT {\"verbose\":false,\"lang\":\"moonbit\",\"version\":\"0.1.0\"}\r\n",
    ),
  )
  sock.write(ascii_bytes("SUB \{subject} 1\r\n"))
  sock.write(ascii_bytes("PING\r\n"))
  let _ = sock.read_until("PONG\r\n")
  println("subscribed to \{subject}, waiting for messages...")
  while true {
    let line = sock.read_until("\r\n") catch { _ => None }
    match line {
      None => {
        println("connection closed")
        sock.close()
        return
      }
      Some(text) =>
        if text.has_prefix("MSG ") {
          let tokens = text.split(" ").collect()
          if tokens.length() >= 4 {
            let size = int_of_digits(tokens[tokens.length() - 1].to_owned())
            let body = if size == 0 {
              Bytes::new(0)
            } else {
              let b = sock.read_exactly(size)
              let _ = sock.read_exactly(2)
              b
            }
            let reply = if tokens.length() == 5 {
              " (reply: \{tokens[3]})"
            } else {
              ""
            }
            println("[\{tokens[1]}] \{bytes_text(body)}\{reply}")
          }
        }
    }
  }
}
