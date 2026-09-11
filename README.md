# moonnats

NATS message system protocol codec and async client for MoonBit.

**Status: protocol layer in progress** — subject matching, command encoding,
the streaming server-frame parser, INFO decoding and HMSG header decoding are
done and tested; the async client on `moonbitlang/async` is next.

## Why

MoonBit had no NATS package at all (checked on mooncakes.io) while Redis and
Kafka clients already exist. NATS sits in the light-weight messaging niche —
simple text protocol, pub/sub plus request-reply, widely used around the
cloud-native ecosystem — and is a good base-layer component for MoonBit
services and tools.

## Usage

Add the dependency and import the package:

```bash
moon add sa2360/moonnats
```

Validate subjects and match wildcards:

```moonbit
@moonnats.is_valid_subject("foo.*", wildcards=true) // true
@moonnats.subject_matches("foo.*.baz", "foo.bar.baz") // true
```

Encode client commands to wire bytes:

```moonbit
let bytes = @moonnats.encode_command(
  @moonnats.Pub(@moonnats.Publish::{ subject: "foo", reply: None, payload: b"hello" }),
) // "PUB foo 5\r\nhello\r\n"
```

Parse a server stream incrementally (safe against TCP fragmentation):

```moonbit
let parser = @moonnats.Parser::new()
parser.feed(bytes_from_socket)
match parser.next_op() {
  NeedMore => ...        // frame not complete yet
  Fail(reason) => ...    // malformed stream
  Op(op, consumed) => ... // one decoded ServerOp
}
```

A complete runnable demo lives in `cmd/main`:

```bash
moon run cmd/main
```

## Scope

Done: subject validation/matching, command encoding, streaming parser,
INFO decoding, HMSG header decoding.

Next: async client (connect handshake, subscription dispatch, PING/PONG
keepalive, request-reply) on `moonbitlang/async` for the native backend, a
pub/sub CLI, and integration tests against a real nats-server.

Out of scope for the first release: JetStream, TLS, JWT/NKEY auth, cluster
discovery.

## Development

```bash
moon check
moon test            # native backend (needs a C compiler)
moon test --target wasm-gc
moon run cmd/main
```

CI runs check/build/test on Ubuntu and Windows via GitHub Actions.

## License

Apache-2.0. Protocol behavior follows the
[official NATS protocol specification](https://docs.nats.io/reference/reference-protocols/nats-protocol);
API design takes inspiration from [nats.go](https://github.com/nats-io/nats.go)
(Apache-2.0) without copying implementation code.
