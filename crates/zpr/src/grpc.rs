//! Generic gRPC client + protobuf<->JSON conversion. There is no per-RPC
//! codegen here on purpose: with 100+ RPCs on the wire contract, binding each
//! one individually into Pascal would mean regenerating and re-vendoring
//! Pascal units on every proto change. Instead this exposes two orthogonal
//! primitives that Pascal composes itself:
//!
//!   1. `zpr_grpc_call`: send raw protobuf bytes to "/pkg.Service/Method",
//!      get raw protobuf bytes back (or a gRPC status + message on error).
//!   2. `zpr_protobuf_json_to_binary` / `zpr_protobuf_binary_to_json`:
//!      convert between JSON and the wire encoding for a named message type,
//!      driven by a `FileDescriptorSet` (the output of
//!      `protoc --descriptor_set_out=... --include_imports`) loaded once at
//!      runtime.
//!
//! Pascal never has to know a message's field layout at compile time — it
//! builds/reads requests as JSON via `zpr_protobuf_json_to_binary` /
//! `zpr_protobuf_binary_to_json` below.

use std::collections::HashMap;
use std::io;
use std::os::raw::c_char;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes};
use hyper_util::rt::TokioIo;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::{Channel, Endpoint};

use crate::ffi::{self, cstr_to_str, runtime, string_to_cstring_ptr, ZprBuffer, ZPR_ERR, ZPR_OK};

// ---------------------------------------------------------------------
// A codec that passes protobuf bytes through untouched. tonic's generated
// clients normally pair a `Codec` with per-message `prost::Message` impls;
// here the "message" already IS the wire bytes, so encode/decode are just
// a length-prefixed memcpy.
// ---------------------------------------------------------------------

#[derive(Default, Clone, Copy)]
struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = BytesCodec;
    type Decoder = BytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        BytesCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesCodec
    }
}

impl Encoder for BytesCodec {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let len = src.remaining();
        let mut out = vec![0u8; len];
        src.copy_to_slice(&mut out);
        Ok(Some(out))
    }
}

// ---------------------------------------------------------------------
// Channel cache: one connected `Channel` per endpoint, reused across calls
// so a Pascal client hammering 100+ RPCs against the same daemon isn't
// paying a fresh TCP+TLS+HTTP2 handshake every time.
// ---------------------------------------------------------------------

static CHANNELS: OnceLock<Mutex<HashMap<String, Channel>>> = OnceLock::new();

// ---------------------------------------------------------------------
// Proxy support. Corporate/dev networks routinely put an HTTP forward proxy
// between the terminal and the daemon; without this every gRPC call would
// silently try (and fail) to dial straight through. Resolution order: an
// explicit `zpr_grpc_set_proxy` override, then the standard
// HTTPS_PROXY/HTTP_PROXY/ALL_PROXY/NO_PROXY environment variables (same
// convention curl/reqwest use).
// ---------------------------------------------------------------------

static PROXY_OVERRIDE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// Explicitly sets (or, with NULL, clears) the proxy used for gRPC calls,
/// overriding environment-variable detection. Takes effect for connections
/// made after this call; existing cached connections are unaffected.
#[no_mangle]
pub extern "C" fn zpr_grpc_set_proxy(proxy_url: *const c_char) -> i32 {
    ffi::guard(ZPR_ERR, move || {
        let mut slot = PROXY_OVERRIDE.get_or_init(Default::default).lock().unwrap();
        if proxy_url.is_null() {
            *slot = None;
            return ZPR_OK;
        }
        match unsafe { cstr_to_str(proxy_url) } {
            Ok(s) => {
                *slot = Some(s.to_string());
                ZPR_OK
            }
            Err(e) => {
                ffi::set_last_error(e);
                ZPR_ERR
            }
        }
    })
}

fn env_var_any(names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| std::env::var(n).ok()).filter(|v| !v.is_empty())
}

fn host_matches_no_proxy(host: &str, no_proxy: &str) -> bool {
    no_proxy.split(',').map(str::trim).filter(|p| !p.is_empty()).any(|pattern| {
        let pattern = pattern.trim_start_matches('.');
        host == pattern || host.ends_with(&format!(".{pattern}"))
    })
}

fn resolve_proxy(target: &http::Uri) -> Option<http::Uri> {
    if let Some(explicit) = PROXY_OVERRIDE.get_or_init(Default::default).lock().unwrap().clone() {
        return explicit.parse().ok();
    }
    let host = target.host().unwrap_or("");
    if let Some(no_proxy) = env_var_any(&["NO_PROXY", "no_proxy"]) {
        if host_matches_no_proxy(host, &no_proxy) {
            return None;
        }
    }
    let is_https = target.scheme_str() == Some("https");
    let names: &[&str] = if is_https {
        &["HTTPS_PROXY", "https_proxy"]
    } else {
        &["HTTP_PROXY", "http_proxy"]
    };
    env_var_any(names)
        .or_else(|| env_var_any(&["ALL_PROXY", "all_proxy"]))
        .and_then(|s| s.parse().ok())
}

/// A `TcpStream` with a few bytes already read off the wire (leftover from
/// peeking past the CONNECT response's `\r\n\r\n` terminator) prepended back
/// onto the read side.
struct PrefixedStream {
    prefix: Vec<u8>,
    prefix_pos: usize,
    inner: TcpStream,
}

impl AsyncRead for PrefixedStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.prefix_pos < this.prefix.len() {
            let remaining = &this.prefix[this.prefix_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.prefix_pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Opens a raw tunnel to `target`'s host:port through an HTTP forward proxy
/// via `CONNECT`, as used for both plaintext and TLS gRPC targets alike.
async fn connect_via_http_proxy(target: &http::Uri, proxy: &http::Uri) -> io::Result<PrefixedStream> {
    let proxy_host = proxy
        .host()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "proxy URI has no host"))?;
    let proxy_port = proxy.port_u16().unwrap_or(if proxy.scheme_str() == Some("https") { 443 } else { 80 });
    let mut stream = TcpStream::connect((proxy_host, proxy_port)).await?;

    let authority = target
        .authority()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "gRPC endpoint URI has no authority"))?
        .to_string();
    let request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: keep-alive\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;

    let mut buf = Vec::new();
    let mut chunk = [0u8; 512];
    let header_end = loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "proxy closed the connection during CONNECT"));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 16 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "proxy CONNECT response too large"));
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]);
    let status_line = head.lines().next().unwrap_or("");
    let ok = status_line.split_whitespace().nth(1) == Some("200");
    if !ok {
        return Err(io::Error::new(io::ErrorKind::Other, format!("proxy CONNECT failed: {status_line}")));
    }

    Ok(PrefixedStream { prefix: buf[header_end..].to_vec(), prefix_pos: 0, inner: stream })
}

async fn connect_channel(endpoint_str: &str) -> Result<Channel, String> {
    let ep = Endpoint::from_shared(endpoint_str.to_string())
        .map_err(|e| format!("invalid gRPC endpoint {endpoint_str:?}: {e}"))?;
    let target: http::Uri = endpoint_str
        .parse()
        .map_err(|e| format!("invalid gRPC endpoint {endpoint_str:?}: {e}"))?;

    match resolve_proxy(&target) {
        Some(proxy) => {
            let target = target.clone();
            ep.connect_with_connector(tower::service_fn(move |_uri: http::Uri| {
                let target = target.clone();
                let proxy = proxy.clone();
                async move {
                    let stream = connect_via_http_proxy(&target, &proxy).await?;
                    Ok::<_, io::Error>(TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|e| format!("failed to connect to {endpoint_str:?} via proxy: {e}"))
        }
        None => ep.connect().await.map_err(|e| format!("failed to connect to {endpoint_str:?}: {e}")),
    }
}

async fn get_channel(endpoint: &str) -> Result<Channel, String> {
    if let Some(ch) = CHANNELS.get_or_init(Default::default).lock().unwrap().get(endpoint) {
        return Ok(ch.clone());
    }
    let ch = connect_channel(endpoint).await?;
    CHANNELS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(endpoint.to_string(), ch.clone());
    Ok(ch)
}

async fn drop_channel(endpoint: &str) {
    CHANNELS.get_or_init(Default::default).lock().unwrap().remove(endpoint);
}

/// Result codes for `zpr_grpc_call`.
pub const GRPC_CALL_OK: i32 = 0;
/// A response was received but carried a non-OK `grpc-status`; see
/// `*out_grpc_status` and `zpr_last_error()`.
pub const GRPC_CALL_STATUS_ERR: i32 = 1;
/// The call never reached the point of getting a gRPC status (bad
/// arguments, DNS/connect/timeout failure, etc.); see `zpr_last_error()`.
pub const GRPC_CALL_TRANSPORT_ERR: i32 = -1;

/// Makes one generic unary gRPC call.
///
/// `endpoint` is a URI such as `"http://127.0.0.1:50051"`. `method_path` is
/// the full gRPC path, e.g. `"/sapphire.v1.Sapphire/Login"`. `request`/
/// `request_len` is the already-protobuf-encoded request message (build it
/// with `zpr_protobuf_json_to_binary`). On `GRPC_CALL_OK`, `*out_response`
/// holds the raw response bytes (free with `zpr_buffer_free`; decode with
/// `zpr_protobuf_binary_to_json`).
#[no_mangle]
pub extern "C" fn zpr_grpc_call(
    endpoint: *const c_char,
    method_path: *const c_char,
    request: *const u8,
    request_len: usize,
    timeout_ms: u32,
    out_response: *mut ZprBuffer,
    out_grpc_status: *mut i32,
) -> i32 {
    if !out_response.is_null() {
        unsafe { *out_response = ZprBuffer::empty() };
    }
    if !out_grpc_status.is_null() {
        unsafe { *out_grpc_status = -1 };
    }

    ffi::guard(GRPC_CALL_TRANSPORT_ERR, move || {
        let endpoint = match unsafe { cstr_to_str(endpoint) } {
            Ok(s) => s.to_string(),
            Err(e) => {
                ffi::set_last_error(e);
                return GRPC_CALL_TRANSPORT_ERR;
            }
        };
        let method_path = match unsafe { cstr_to_str(method_path) } {
            Ok(s) => s.to_string(),
            Err(e) => {
                ffi::set_last_error(e);
                return GRPC_CALL_TRANSPORT_ERR;
            }
        };
        let path: http::uri::PathAndQuery = match method_path.parse() {
            Ok(p) => p,
            Err(e) => {
                ffi::set_last_error(format!("invalid gRPC method path {method_path:?}: {e}"));
                return GRPC_CALL_TRANSPORT_ERR;
            }
        };
        let body: Vec<u8> = if request.is_null() || request_len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(request, request_len) }.to_vec()
        };

        let outcome: Result<Result<Vec<u8>, tonic::Status>, String> = runtime().block_on(async move {
            let channel = get_channel(&endpoint).await?;
            let mut client = tonic::client::Grpc::new(channel);
            client
                .ready()
                .await
                .map_err(|e| format!("gRPC transport not ready for {endpoint:?}: {e}"))?;

            let mut req = tonic::Request::new(body);
            if timeout_ms > 0 {
                req.set_timeout(Duration::from_millis(timeout_ms as u64));
            }

            match client.unary(req, path, BytesCodec).await {
                Ok(resp) => Ok(Ok(resp.into_inner())),
                Err(status) => {
                    // A channel that produced a status error (rather than a
                    // transport error) is still healthy and worth keeping;
                    // only truly broken transports get evicted below.
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::Cancelled | tonic::Code::Unknown
                    ) {
                        drop_channel(&endpoint).await;
                    }
                    Ok(Err(status))
                }
            }
        });

        match outcome {
            Ok(Ok(bytes)) => {
                if !out_response.is_null() {
                    unsafe { *out_response = ZprBuffer::from_vec(bytes) };
                }
                if !out_grpc_status.is_null() {
                    unsafe { *out_grpc_status = 0 };
                }
                GRPC_CALL_OK
            }
            Ok(Err(status)) => {
                if !out_grpc_status.is_null() {
                    unsafe { *out_grpc_status = status.code() as i32 };
                }
                ffi::set_last_error(status.message().to_string());
                GRPC_CALL_STATUS_ERR
            }
            Err(e) => {
                ffi::set_last_error(e);
                GRPC_CALL_TRANSPORT_ERR
            }
        }
    })
}

// ---------------------------------------------------------------------
// Protobuf JSON <-> binary, via a runtime-loaded FileDescriptorSet. This is
// what lets Pascal build/read arbitrary messages by name without any
// generated bindings.
// ---------------------------------------------------------------------

/// Opaque handle wrapping a loaded `DescriptorPool`. Always free with
/// `zpr_protobuf_pool_free`.
pub struct DescriptorPoolHandle(DescriptorPool);

/// Loads a `FileDescriptorSet` (the binary output of
/// `protoc --descriptor_set_out=out.bin --include_imports your.proto`).
/// Returns NULL on failure — check `zpr_last_error()`.
#[no_mangle]
pub extern "C" fn zpr_protobuf_pool_new(descriptor_set: *const u8, len: usize) -> *mut DescriptorPoolHandle {
    ffi::guard(std::ptr::null_mut(), move || {
        if descriptor_set.is_null() {
            ffi::set_last_error("null descriptor_set passed to zpr_protobuf_pool_new");
            return std::ptr::null_mut();
        }
        let bytes = unsafe { std::slice::from_raw_parts(descriptor_set, len) };
        match DescriptorPool::decode(bytes) {
            Ok(pool) => Box::into_raw(Box::new(DescriptorPoolHandle(pool))),
            Err(e) => {
                ffi::set_last_error(format!("invalid FileDescriptorSet: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn zpr_protobuf_pool_free(handle: *mut DescriptorPoolHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

/// Encodes `json` as the protobuf wire format for `message_type` (its full
/// dotted name, e.g. `"sapphire.v1.LoginRequest"`). Returns `ZPR_OK` and
/// fills `*out` on success.
#[no_mangle]
pub extern "C" fn zpr_protobuf_json_to_binary(
    pool: *const DescriptorPoolHandle,
    message_type: *const c_char,
    json: *const c_char,
    out: *mut ZprBuffer,
) -> i32 {
    if !out.is_null() {
        unsafe { *out = ZprBuffer::empty() };
    }
    ffi::guard(ZPR_ERR, move || {
        if pool.is_null() {
            ffi::set_last_error("null descriptor pool handle");
            return ZPR_ERR;
        }
        let message_type = match unsafe { cstr_to_str(message_type) } {
            Ok(s) => s,
            Err(e) => {
                ffi::set_last_error(e);
                return ZPR_ERR;
            }
        };
        let json = match unsafe { cstr_to_str(json) } {
            Ok(s) => s,
            Err(e) => {
                ffi::set_last_error(e);
                return ZPR_ERR;
            }
        };
        let pool = unsafe { &(*pool).0 };
        let Some(descriptor) = pool.get_message_by_name(message_type) else {
            ffi::set_last_error(format!("unknown message type {message_type:?} in descriptor pool"));
            return ZPR_ERR;
        };
        let mut de = serde_json::Deserializer::from_str(json);
        let dynamic = match DynamicMessage::deserialize(descriptor, &mut de) {
            Ok(m) => m,
            Err(e) => {
                ffi::set_last_error(format!("failed to encode {message_type:?} from JSON: {e}"));
                return ZPR_ERR;
            }
        };
        if let Err(e) = de.end() {
            ffi::set_last_error(format!("trailing data after JSON for {message_type:?}: {e}"));
            return ZPR_ERR;
        }
        let bytes = dynamic.encode_to_vec();
        if !out.is_null() {
            unsafe { *out = ZprBuffer::from_vec(bytes) };
        }
        ZPR_OK
    })
}

/// Decodes protobuf-wire `data` (of type `message_type`) into a JSON string.
/// Returns an owned string (free with `zpr_string_free`), or NULL on failure.
#[no_mangle]
pub extern "C" fn zpr_protobuf_binary_to_json(
    pool: *const DescriptorPoolHandle,
    message_type: *const c_char,
    data: *const u8,
    data_len: usize,
) -> *mut c_char {
    ffi::guard(std::ptr::null_mut(), move || {
        if pool.is_null() {
            ffi::set_last_error("null descriptor pool handle");
            return std::ptr::null_mut();
        }
        let message_type = match unsafe { cstr_to_str(message_type) } {
            Ok(s) => s,
            Err(e) => {
                ffi::set_last_error(e);
                return std::ptr::null_mut();
            }
        };
        let pool = unsafe { &(*pool).0 };
        let Some(descriptor) = pool.get_message_by_name(message_type) else {
            ffi::set_last_error(format!("unknown message type {message_type:?} in descriptor pool"));
            return std::ptr::null_mut();
        };
        let bytes: Bytes = if data.is_null() || data_len == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, data_len) })
        };
        let dynamic = match DynamicMessage::decode(descriptor, bytes) {
            Ok(m) => m,
            Err(e) => {
                ffi::set_last_error(format!("failed to decode {message_type:?}: {e}"));
                return std::ptr::null_mut();
            }
        };
        match serde_json::to_string(&dynamic) {
            Ok(s) => string_to_cstring_ptr(s),
            Err(e) => {
                ffi::set_last_error(format!("failed to render {message_type:?} as JSON: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}
