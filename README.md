# moonnats

NATS message system protocol codec and async client for MoonBit.

[![CI](https://github.com/sa2360/moonnats/actions/workflows/ci.yml/badge.svg)](https://github.com/sa2360/moonnats/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![mooncakes](https://img.shields.io/badge/mooncakes-sa2360%2Fmoonnats-orange.svg)](https://mooncakes.io/docs/sa2360/moonnats)

moonnats 为 MoonBit 提供 [NATS](https://nats.io/) 消息系统的完整协议实现：
一个零 I/O 依赖、可在任何后端（含 WASM）运行的**协议编解码层**，以及一个基于
[`moonbitlang/async`](https://github.com/moonbitlang/async) 的**异步客户端**（开发中）。

## 项目背景

NATS 是云原生生态广泛使用的轻量级消息系统，核心协议是基于 TCP 的文本协议，
常用于微服务通信、事件通知和 IoT 场景。本项目启动时 mooncakes.io 上没有任何
NATS 相关的包（Redis、Kafka 已有客户端，NATS 是空位），因此从零实现：

- **协议层**是纯函数式的字节流 ↔ 结构化消息转换，不碰网络，单测即全覆盖；
- **客户端层**负责连接、握手、订阅分发、心跳和 request-reply，只支持 native 后端。

协议行为以 [NATS 官方协议规范](https://docs.nats.io/reference/reference-protocols/nats-protocol)
为准，API 设计参考 [nats.go](https://github.com/nats-io/nats.go)（Apache-2.0），
不复制其实现代码。

## 当前状态

| 模块 | 状态 | 说明 |
|------|------|------|
| 主题校验与通配符匹配 | ✅ 完成 | `*` 单层、`>` 多层通配 |
| 客户端命令编码 | ✅ 完成 | PUB / SUB / UNSUB / PING / CONNECT |
| 服务端帧流式解析 | ✅ 完成 | TCP 粘包/半包安全，二进制 payload 安全 |
| INFO 握手解析 | ✅ 完成 | JSON → `ServerInfo` |
| HMSG 头部块解析 | ✅ 完成 | NATS/1.0 版本行 + 键值对 |
| INBOX 主题生成 | ✅ 完成 | request-reply 的地基 |
| 异步客户端 | 🚧 开发中 | moonbitlang/async，native 后端 |
| CLI 工具 | 🚧 计划中 | pub / sub 命令行 |
| nats-server 集成测试 | 🚧 计划中 | CI 内对真实服务器收发 |

测试在 native 和 wasm-gc 双后端通过（41 个用例），CI 覆盖 Ubuntu 和 Windows。

**明确不在首版范围内**：JetStream、TLS、JWT/NKEY 认证、集群自动发现。

## 安装

```bash
moon add sa2360/moonnats
```

在包的 `moon.pkg` 中导入：

```json
// moon.pkg
{
  "import": [
    "sa2360/moonnats"
  ]
}
```

## 使用

### 主题校验与通配符匹配

```moonbit
// 发布主题不允许通配符
@moonnats.is_valid_subject("foo.bar")            // true
@moonnats.is_valid_subject("foo.*")              // false

// 订阅主题允许 * 和 >
@moonnats.is_valid_subject("foo.*", wildcards=true)  // true
@moonnats.is_valid_subject("foo.>", wildcards=true)  // true
@moonnats.is_valid_subject("foo.>.bar", wildcards=true) // false（> 必须在末尾）

// 匹配：* 恰好一层，> 一层或多层
@moonnats.subject_matches("foo.*", "foo.bar")        // true
@moonnats.subject_matches("foo.*", "foo.bar.baz")    // false
@moonnats.subject_matches("foo.>", "foo.bar.baz")    // true
@moonnats.subject_matches("foo.>", "foo")            // false（> 至少吞一层）
```

### 编码客户端命令

```moonbit
let cmd : @moonnats.ClientCommand = @moonnats.Pub(
  @moonnats.Publish::{ subject: "events", reply: None, payload: b"hi" },
)
let wire : Bytes = @moonnats.encode_command(cmd)
// wire == b"PUB events 2\r\nhi\r\n"
```

CONNECT 握手配置：

```moonbit
let cfg = @moonnats.ConnectConfig::default(name=Some("my-service"))
cfg.encode()       // 完整 CONNECT 命令字节
cfg.to_json()      // 对应的 Json 值
```

### 解析服务端字节流

```moonbit
let parser = @moonnats.Parser::new()
parser.feed(chunk)          // 每次 socket 读到什么都 feed 进去
match parser.next_op() {
  @moonnats.NeedMore => ()  // 帧不完整，等下一次读
  @moonnats.Op(op, n) =>    // 一个完整 ServerOp，n 是消费的字节数
    match op {
      @moonnats.Msg(m) => handle(m.subject, m.payload)
      @moonnats.Ping => send_pong()
      _ => ()
    }
  @moonnats.Fail(reason) => log("protocol error: \{reason}")
}
```

长连接建议用 `feed_and_compact`，它会顺带回收已消费的缓冲区。

### 解析 INFO 和 HMSG 头部

```moonbit
@moonnats.parse_server_info(json_text)  // Result[ServerInfo, String]
@moonnats.parse_header_block(headers_bytes) // Result[HeaderBlock, String]
```

## 架构

```
┌─────────────────────────────────────────────┐
│            应用 / CLI（cmd/）                │
├─────────────────────────────────────────────┤
│  客户端层（开发中）                          │
│  连接握手 · 订阅分发 · PING/PONG · req-rep   │
│  依赖 moonbitlang/async（仅 native）         │
├─────────────────────────────────────────────┤
│  协议层（完成）                              │
│  protocol.mbt   消息数据模型                 │
│  encode.mbt     ClientCommand → Bytes       │
│  parser.mbt     Bytes → ServerOp（流式）     │
│  connect.mbt    CONNECT 配置与 JSON          │
│  info.mbt       INFO JSON → ServerInfo       │
│  headers.mbt    HMSG 头部块 → HeaderBlock    │
│  subject.mbt    主题校验与通配符匹配          │
│  inbox.mbt      _INBOX.<id>.<n> 生成器       │
└─────────────────────────────────────────────┘
```

协议层设计要点：

- **MSG/HMSG 不能按行切**：payload 是二进制且长度写在头里，解析器先解析头行、
  按声明长度精确消费 payload 和终止 CRLF，剩余字节留给下一帧；
- **`-ERR`/`INFO` 的参数不能分词**：错误描述和 JSON 都含空格，参数取整行原文；
- **解析器是增量状态机**：`feed` 进什么就缓冲什么，`next_op` 逐帧取出，
  所有粘包/半包行为都有单测覆盖，不需要起服务器就能验证。

## 开发

### 环境要求

- [MoonBit 工具链](https://www.moonbitlang.com/download/)（开发基于 0.1.20260904）
- native 后端测试需要 C 编译器（Linux/macOS 自带；Windows 见下方说明）

### 常用命令

```bash
moon check                    # 类型检查
moon test                     # 默认后端跑测试
moon test --target native     # native 后端（需要 C 编译器）
moon test --target wasm-gc    # wasm-gc 后端
moon run cmd/main             # 协议层往返演示
moon fmt                      # 格式化（提交前必须跑）
```

### Windows（native 后端）注意事项

1. msys64 自带的 gcc 可能是坏的（include 搜索路径为空）。用
   [scoop](https://scoop.sh/) 装一个完好的 MinGW-w64：

   ```bash
   scoop install gcc
   ```

2. 指定 moon 使用它（一次性配置）：

   ```bash
   setx MOON_CC "C:\Users\<你>\scoop\apps\gcc\current\bin\gcc.exe"
   ```

3. 当前 moon 运行时（0.1.20260904）在 mingw 下编译会报 `rand_s` 未声明——
   `windows.h` 的包含链在 `_CRT_RAND_S` 定义前就拉进了 `stdlib.h`。
   解决：编辑 `~/.moon/lib/runtime/env.c`，把这段挪到文件顶部（任何
   `#include` 之前，`#include "moonbit.h"` 之前）：

   ```c
   #ifndef _CRT_RAND_S
   #define _CRT_RAND_S
   #endif
   ```

   这是 moon 运行时的上游问题，重装工具链后需要重新打一次。

### 提交代码

1. fork 仓库，从 `master` 拉分支；
2. 改动保证 `moon check` 无错误、`moon fmt` 已跑、
   native 和 wasm-gc 双后端测试全绿；
3. 涉及协议行为的改动必须带测试用例（参考 `parser_test.mbt` 的写法）；
4. commit message 用 conventional 风格（`feat:` / `fix:` / `docs:` / `test:`），
   一个提交做一件事；
5. 发 PR，CI（双平台 × 双后端）通过后合并。

### 测试约定

- 黑盒测试（`*_test.mbt`）放在包外视角，通过 `@moonnats.` 前缀访问公开 API，
  公开行为都写在这里；
- 白盒测试（`*_wbtest.mbt`）用于包内私有逻辑；
- 协议测试的二进制输入用 `ascii("...")` 辅助函数构造，payload 里可以放心写
  `\r\n`，测试的就是解析器对它的容忍度。

## 路线图

- [ ] 异步客户端：TCP 连接、INFO/CONNECT 握手、读循环 + 订阅分发
- [ ] PING/PONG 保活与服务器失联检测
- [ ] request-reply（INBOX + 超时）
- [ ] 优雅关闭（UNSUB + flush 后断开）
- [ ] CLI：`moonnats pub <subject> <data>` / `moonnats sub <subject>`
- [ ] 集成测试：CI 内安装 nats-server 跑真实收发
- [ ] 发布 0.1.0 到 mooncakes.io

## 许可证

Apache-2.0。协议行为遵循
[NATS 官方协议规范](https://docs.nats.io/reference/reference-protocols/nats-protocol)；
API 设计参考 [nats.go](https://github.com/nats-io/nats.go)（Apache-2.0），
未复制其实现代码。NATS 是 NATS.io 的商标，本项目是独立的社区实现，与
NATS.io 无隶属关系。
