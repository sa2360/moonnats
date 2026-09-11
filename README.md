# moonnats

NATS message system protocol codec and async client for MoonBit.

**Status: work in progress** — scope and design notes live in the project proposal.

## Planned scope

- Pure protocol codec for the NATS text protocol (INFO / CONNECT / PUB / HPUB /
  SUB / UNSUB / MSG / HMSG / PING / PONG / +OK / -ERR) with a streaming parser
  that handles TCP fragmentation — zero I/O dependencies, runnable on any
  backend including WASM.
- Subject validation and wildcard matching (`*` single level, `>` multi level).
- Async client on top of [`moonbitlang/async`](https://github.com/moonbitlang/async)
  TCP sockets (native backend): connect handshake, subscribe dispatch,
  PING/PONG keepalive, request-reply with timeout, graceful close.

Out of scope for the first release: JetStream, TLS, JWT/NKEY auth, cluster
discovery.

## Development

```bash
moon check
moon test
moon run cmd/main        # CLI example (coming soon)
```

## License

Apache-2.0. Protocol behavior follows the
[official NATS protocol specification](https://docs.nats.io/reference/reference-protocols/nats-protocol);
API design takes inspiration from [nats.go](https://github.com/nats-io/nats.go)
(Apache-2.0) without copying implementation code.
