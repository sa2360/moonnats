///|
/// moonnats command line tool.
///
/// Usage:
///   moonnats_cli pub <subject> <message>   publish one message
///   moonnats_cli sub <subject>             subscribe and print messages
///
/// Connection settings via environment variables:
///   NATS_HOST (default 127.0.0.1), NATS_PORT (default 4222),
///   NATS_USER / NATS_PASS / NATS_TOKEN for auth.
async fn main {
  let args = @env.args()
  // args[0] is the program path.
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
    "pub" => run_pub(args, build_options(host, port))
    "sub" => run_sub(args, build_options(host, port))
    "help" | "-h" | "--help" => usage()
    other => {
      println("unknown command: \{other}")
      usage()
    }
  }
}

///|
fn usage() -> Unit {
  println("moonnats — NATS command line tool")
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
fn build_options(host : String, port : Int) -> @client.Options {
  let base = @client.Options::default()
  {
    host,
    port,
    name: Some("moonnats-cli"),
    user: base.user,
    pass: base.pass,
    token: base.token,
    handshake_timeout_ms: base.handshake_timeout_ms,
    ping_interval_ms: base.ping_interval_ms,
    max_outstanding_pings: base.max_outstanding_pings,
    default_request_timeout_ms: base.default_request_timeout_ms,
    inbox_prefix: base.inbox_prefix,
    verbose: base.verbose,
  }
}

///|
/// Publish one message.
///
/// Uses the protocol layer directly over a raw TCP socket: a one-shot
/// publish does not need the connection loops, and this keeps the CLI
/// simple and immune to task-teardown quirks of the async runtime.
async fn run_pub(args : Array[String], options : @client.Options) -> Unit {
  if args.length() != 4 {
    println("usage: moonnats_cli pub <subject> <message>")
    return
  }
  let subject = args[2]
  let payload = args[3]
  let sock = @socket.Tcp::connect_to_host(options.host, port=options.port)
  // Handshake: read INFO, send CONNECT, then PING and require PONG.
  let info = sock.read_until("\r\n\r\n")
  if info is None {
    println("error: no INFO from server")
    return
  }
  sock.write(
    ascii_bytes(
      "CONNECT {\"verbose\":false,\"lang\":\"moonbit\",\"version\":\"0.1.0\"}\r\n",
    ),
  )
  sock.write(ascii_bytes("PING\r\n"))
  let pong = sock.read_until("PONG\r\n")
  if pong is None {
    println("error: no PONG from server")
    return
  }
  // Publish, then confirm with a flush round trip.
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
/// Subscribe and print matching messages until interrupted.
async fn run_sub(args : Array[String], options : @client.Options) -> Unit {
  if args.length() != 3 {
    println("usage: moonnats_cli sub <subject>")
    return
  }
  let subject = args[2]
  let sock = @socket.Tcp::connect_to_host(options.host, port=options.port)
  let _ = sock.read_until("\r\n\r\n")
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
    let line = sock.read_until("\r\n\r\n") catch { _ => None }
    match line {
      None => {
        println("connection closed")
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

///|
fn int_of_digits(text : String) -> Int {
  let mut value = 0
  for ch in text {
    value = value * 10 + (ch.to_int() - '0'.to_int())
  }
  value
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
      sb.write_string("\\x\{c.to_string(radix=16)}")
    }
  }
  sb.to_string()
}
