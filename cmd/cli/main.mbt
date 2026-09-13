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
    Some(text) => match parse_port(text) {
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
  println("  moonnats_cli sub <subject>             subscribe and print messages")
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
    host: host,
    port: port,
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
async fn run_pub(args : Array[String], options : @client.Options) -> Unit {
  if args.length() != 4 {
    println("usage: moonnats_cli pub <subject> <message>")
    return
  }
  let subject = args[2]
  let payload = args[3]
  let outcome = @client.connect_and_run(options=options, conn => {
    match conn.publish(subject, ascii_bytes(payload)) {
      Err(e) => Err(e)
      Ok(_) => conn.flush(timeout_ms=2000)
    }
  })
  match outcome {
    Ok(_) => println("published to \{subject}")
    Err(e) => println("error: \{e.message()}")
  }
}

///|
/// Subscribe and print matching messages until interrupted or disconnected.
async fn run_sub(args : Array[String], options : @client.Options) -> Unit {
  if args.length() != 3 {
    println("usage: moonnats_cli sub <subject>")
    return
  }
  let subject = args[2]
  let outcome = @client.connect_and_run(options=options, conn => {
    let sub = conn.subscribe(subject, msg => {
      let reply = match msg.reply {
        Some(r) => " (reply: \{r})"
        None => ""
      }
      println("[\{msg.subject}] \{bytes_text(msg.payload)}\{reply}")
    })
    match sub {
      Err(e) => Err(e)
      Ok(sid) => {
        println("subscribed to \{subject} (sid \{sid}), waiting for messages...")
        while conn.is_open() {
          @async.sleep(1000)
        }
        Err(@client.NatsError::ConnectionClosed("connection closed"))
      }
    }
  })
  match outcome {
    Ok(_) => ()
    Err(e) => println("error: \{e.message()}")
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
      sb.write_char((c).unsafe_to_char())
    } else {
      sb.write_string("\\x\{c.to_string(radix=16)}")
    }
  }
  sb.to_string()
}
