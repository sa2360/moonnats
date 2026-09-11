///|
/// Runnable protocol-layer demo: encode client commands, feed the encoded
/// bytes plus a server frame stream through the parser, and print each
/// decoded operation. Run with `moon run cmd/main` from the project root.
fn main {
  let cfg = @moonnats.ConnectConfig::default(name=Some("moonnats-demo"))
  let connect = cfg.encode()
  let sub = @moonnats.encode_command(
    @moonnats.Sub(@moonnats.Subscribe::{
      subject: "help",
      sid: "1",
      queue_group: Some("workers"),
    }),
  )
  let ping = @moonnats.encode_command(@moonnats.Ping)
  let pub_cmd = @moonnats.encode_command(
    @moonnats.Pub(@moonnats.Publish::{
      subject: "help",
      reply: None,
      payload: ascii_bytes("need help"),
    }),
  )
  print_command("CONNECT", connect)
  print_command("SUB", sub)
  print_command("PING", ping)
  print_command("PUB", pub_cmd)

  println("\n--- parsing a server frame stream ---")
  let server_stream = ascii_bytes(
    "INFO {\"server_id\":\"NDX\",\"version\":\"2.10.11\",\"host\":\"localhost\",\"port\":4222,\"headers\":true,\"max_payload\":1048576}\r\n" +
    "+OK\r\n" +
    "MSG help 1 9\r\nneed help\r\n" +
    "HMSG help 1 26 35\r\nNATS/1.0\r\nReply-To: me\r\n\r\nneed help\r\n" +
    "PING\r\n",
  )
  let parser = @moonnats.Parser::new()
  parser.feed(server_stream)
  while true {
    match parser.next_op() {
      @moonnats.NeedMore => {
        println("(need more bytes)")
        break
      }
      @moonnats.Fail(reason) => {
        println("parse error: \{reason}")
        break
      }
      @moonnats.Op(op, _) =>
        match op {
          @moonnats.Info(json) =>
            match @moonnats.parse_server_info(json) {
              Ok(info) =>
                println(
                  "INFO  server=\{info.server_id} v\{info.version} max_payload=\{info.max_payload}",
                )
              Err(e) => println("INFO  (undecodable: \{e})")
            }
          @moonnats.Msg(m) =>
            println(
              "MSG   subject=\{m.subject} sid=\{m.sid} payload=\{bytes_text(m.payload)}",
            )
          @moonnats.HMsg(m) => {
            println(
              "HMSG  subject=\{m.subject} payload=\{bytes_text(m.payload)}",
            )
            match @moonnats.parse_header_block(m.headers) {
              Ok(block) =>
                for h in block.headers {
                  println("      header \{h.name}: \{h.value}")
                }
              Err(e) => println("      (bad headers: \{e})")
            }
          }
          @moonnats.Ping => println("PING")
          @moonnats.Pong => println("PONG")
          @moonnats.Ok => println("+OK")
          @moonnats.ErrOp(desc) => println("-ERR  \{desc}")
        }
    }
  }
}

///|
fn print_command(name : String, bytes : Bytes) -> Unit {
  println("\{name}: \{bytes_text(bytes)}")
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
    } else if c == 13 {
      sb.write_string("\\r")
    } else if c == 10 {
      sb.write_string("\\n")
    } else {
      sb.write_string("\\x\{c.to_string(radix=16)}")
    }
  }
  sb.to_string()
}
