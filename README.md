# zpr — Zero Portable Runtime

`zpr` gives Object Pascal (Free Pascal/Lazarus and Delphi) one consistent API for JSON, HTTP, and gRPC/protobuf — capabilities that are either missing entirely or split across two incompatible standard libraries, depending on which compiler you use.

## Why

Object Pascal has two major, largely incompatible implementations. Free Pascal ships `fpjson` and `fphttpclient`; Delphi ships `System.JSON` and `System.Net.HttpClient`. Code written against one doesn't compile against the other, so any codebase that needs to build on both ends up with `{$IFDEF}` branches or a parallel implementation per compiler.

And neither implementation has **any** gRPC or protobuf support. There is no gRPC client, no gRPC server, and no protobuf runtime for Object Pascal at all — an application that needs to talk to a gRPC service, or expose one, has nothing to build on.

`zpr` is a small Rust-built shared library plus a single Object Pascal unit that closes both gaps with one API: the same source file compiles unmodified under FPC and Delphi, and runs identically on every platform the library ships for.

## What it gives you

- **JSON** — parse, build, query, and stringify a JSON tree through an opaque handle, without depending on either compiler's own JSON library.
- **HTTP client** — one function covering every verb (GET/POST/PUT/DELETE/...), with HTTP/2 and TLS 1.3 support, proxy detection (`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`NO_PROXY`) or an explicit override, and no HTTP client library on the Pascal side.
- **gRPC client** — a generic pass-through call: give it a method path and raw protobuf bytes, get bytes back. No per-RPC code generation and no generated Pascal units to regenerate every time a `.proto` file changes.
- **gRPC server** — register one handler and it serves every method, unary or streaming alike; a unary call is just the degenerate case of "read one message, write one message."
- **Protobuf ⇄ JSON** — convert between the wire format and JSON at runtime, driven by a compiled `FileDescriptorSet` loaded at startup. Pascal code builds and reads messages as JSON; it never needs compile-time knowledge of a message's field layout.

## How it works

- `crates/zpr` is a Rust crate that compiles to a plain C-ABI shared library (`libzpr.so` / `libzpr.dylib` / `zpr.dll`), plus a generated C header (`zpr.h`).
- `pascal/Zpr.pas` is one unit that loads that library dynamically at runtime (`LoadLibrary`/`dlopen`) rather than linking it statically — that's what lets a single source file target both compilers and every platform without any linker-specific configuration on the Pascal side.

## Status

Builds and runs on macOS, Linux, and Windows today (native and cross-compiled). iOS and Android support is planned — both need a different linking approach than dynamic loading (static linking/an XCFramework on iOS, since it can't `dlopen` arbitrary libraries; a per-ABI shared object on Android).

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
