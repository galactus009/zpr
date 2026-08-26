//! Generic gRPC server: one Pascal-registered callback handles every method
//! on every path, the same "no per-RPC codegen" tradeoff as the client in
//! `grpc.rs`. Unary and streaming RPCs go through the *same* mechanism —
//! unary is just the degenerate case of "read one message, write one
//! message" — so there is exactly one handler shape to implement against.
//!
//! This module owns HTTP/2 framing and the gRPC wire protocol (length-
//! prefixed frames, trailers with `grpc-status`/`grpc-message`); Pascal only
//! ever sees `read()`/`write()` calls on an opaque per-RPC stream.
//!
//! Plaintext HTTP/2 (h2c) only — this is meant for a local daemon<->terminal
//! link, not an Internet-facing endpoint. Put it behind a TLS-terminating
//! proxy if that ever changes.

use std::convert::Infallible;
use std::ffi::CString;
use std::future::Future;
use std::os::raw::{c_char, c_void};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use http_body_util::combinators::BoxBody;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::Service;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

use crate::ffi::{self, cstr_to_str, ZprBuffer, ZPR_ERR, ZPR_OK};

/// Callback a Pascal program registers to handle every incoming gRPC call,
/// unary or streaming alike.
///
/// - `method_path`: e.g. `"/sapphire.v1.Sapphire/Login"` (borrowed, valid
///   only for the duration of the call).
/// - `stream`: an opaque handle for this one RPC. Pull inbound messages with
///   `zpr_grpc_stream_read` (a unary request has exactly one, then a 0
///   "client done" signal) and push outbound messages with
///   `zpr_grpc_stream_write` (a unary response is exactly one write). Do
///   not use `stream` after this function returns.
/// - `user_data`: whatever was passed to `zpr_grpc_server_start`.
/// - `out_grpc_status`: a `google.rpc.Code` value; 0 is OK. The handler MUST
///   set this before returning.
/// - `out_message`/`out_message_len`: optional UTF-8 status message,
///   allocated with `zpr_alloc` (ownership transfers to this library);
///   leave both NULL/0 for none.
///
/// Each call runs on its own worker thread via `spawn_blocking` — handlers
/// for different RPCs may run concurrently, so the handler must be
/// reentrant. `read`/`write` on `stream` block the calling thread, which is
/// exactly what that worker thread is for.
pub type GrpcHandler = extern "C" fn(
    method_path: *const c_char,
    stream: *mut GrpcStream,
    user_data: *mut c_void,
    out_grpc_status: *mut i32,
    out_message: *mut *mut u8,
    out_message_len: *mut usize,
);

/// Opaque running-server handle. Always stop with `zpr_grpc_server_stop`.
pub struct GrpcServerHandle {
    shutdown: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// A pointer is not `Send` by default; `user_data` is opaque caller state we
/// never dereference ourselves, only hand back to `handler`, which is itself
/// always safe to call from any thread (it's a plain `extern "C" fn`).
#[derive(Clone, Copy)]
struct SendableUserData(*mut c_void);
unsafe impl Send for SendableUserData {}

/// Same rationale as `SendableUserData`: `*mut GrpcStream` only ever crosses
/// into the handler's dedicated worker thread, which is the sole owner of
/// it for the duration of that call.
#[derive(Clone, Copy)]
struct SendableStream(*mut GrpcStream);
unsafe impl Send for SendableStream {}

#[derive(Clone)]
struct PassthroughService {
    handler: GrpcHandler,
    user_data: SendableUserData,
}

/// Opaque per-RPC stream, bridging the async connection task and the
/// synchronous handler thread. Free implicitly when the handler returns.
pub struct GrpcStream {
    inbound_rx: mpsc::Receiver<Vec<u8>>,
    outbound_tx: mpsc::Sender<Vec<u8>>,
}

/// Blocks until the next inbound message arrives. Returns 1 and fills
/// `*out` (owned — free with `zpr_buffer_free`) if a message arrived, 0 if
/// the client has finished sending (a unary call always sees exactly one
/// message then this), or `ZPR_ERR` on misuse.
#[no_mangle]
pub extern "C" fn zpr_grpc_stream_read(stream: *mut GrpcStream, out: *mut ZprBuffer) -> i32 {
    if !out.is_null() {
        unsafe { *out = ZprBuffer::empty() };
    }
    if stream.is_null() {
        ffi::set_last_error("null stream handle passed to zpr_grpc_stream_read");
        return ZPR_ERR;
    }
    ffi::guard(ZPR_ERR, move || {
        let s = unsafe { &mut *stream };
        match s.inbound_rx.blocking_recv() {
            Some(msg) => {
                if !out.is_null() {
                    unsafe { *out = ZprBuffer::from_vec(msg) };
                }
                1
            }
            None => 0,
        }
    })
}

/// Sends one outbound message to the client. `data` is copied immediately —
/// the caller keeps ownership and may free/reuse it as soon as this
/// returns. Returns `ZPR_OK`, or `ZPR_ERR` if the client has disconnected.
#[no_mangle]
pub extern "C" fn zpr_grpc_stream_write(stream: *mut GrpcStream, data: *const u8, len: usize) -> i32 {
    if stream.is_null() {
        ffi::set_last_error("null stream handle passed to zpr_grpc_stream_write");
        return ZPR_ERR;
    }
    ffi::guard(ZPR_ERR, move || {
        let bytes: Vec<u8> = if data.is_null() || len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
        };
        let s = unsafe { &*stream };
        match s.outbound_tx.blocking_send(bytes) {
            Ok(()) => ZPR_OK,
            Err(_) => {
                ffi::set_last_error("client disconnected");
                ZPR_ERR
            }
        }
    })
}

fn try_decode_one(buf: &mut BytesMut) -> Option<Vec<u8>> {
    if buf.len() < 5 {
        return None;
    }
    if buf[0] != 0 {
        // Compressed messages are not supported; drop the connection's
        // framing rather than silently misinterpret it.
        buf.clear();
        return None;
    }
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if buf.len() < 5 + len {
        return None;
    }
    let mut msg = buf.split_to(5 + len);
    Some(msg.split_off(5).to_vec())
}

fn encode_grpc_frame(msg: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + msg.len());
    out.push(0u8);
    out.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    out.extend_from_slice(msg);
    out
}

/// Percent-encodes a `grpc-message` trailer per the gRPC-over-HTTP2 spec:
/// everything outside `0x20-0x24` / `0x26-0x7E` (which excludes `%` itself,
/// keeping the encoding unambiguous) is escaped.
fn percent_encode_grpc_message(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if (0x20..=0x24).contains(&b) || (0x26..=0x7e).contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn trailers(status: i32, message: &str) -> http::HeaderMap {
    let mut map = http::HeaderMap::new();
    map.insert("grpc-status", http::HeaderValue::from_str(&status.to_string()).unwrap());
    if !message.is_empty() {
        if let Ok(hv) = http::HeaderValue::from_str(&percent_encode_grpc_message(message)) {
            map.insert("grpc-message", hv);
        }
    }
    map
}

type RespBody = BoxBody<Bytes, Infallible>;

/// A trailers-only response, for failures discovered before any handler ran
/// (bad framing, non-UTF8 path, etc).
fn immediate_error(status: i32, message: &str) -> http::Response<RespBody> {
    let frame: Result<Frame<Bytes>, Infallible> = Ok(Frame::trailers(trailers(status, message)));
    let body: RespBody = BoxBody::new(http_body_util::StreamBody::new(futures_util::stream::iter([frame])));
    http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(body)
        .expect("static response head is always valid")
}

enum OutboundState {
    Streaming,
    AwaitingStatus,
    Done,
}

/// Streams `outbound_rx` out as DATA frames, then — once the handler
/// finishes and reports its status via `status_rx` — emits one final
/// TRAILERS frame. This is what lets the response start flowing before the
/// handler (running concurrently on its own thread) has produced anything,
/// which server-streaming RPCs depend on.
struct OutboundBody {
    rx: mpsc::Receiver<Vec<u8>>,
    status_rx: oneshot::Receiver<(i32, String)>,
    state: OutboundState,
}

impl Body for OutboundBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        loop {
            match this.state {
                OutboundState::Streaming => match this.rx.poll_recv(cx) {
                    Poll::Ready(Some(msg)) => {
                        return Poll::Ready(Some(Ok(Frame::data(Bytes::from(encode_grpc_frame(&msg))))));
                    }
                    Poll::Ready(None) => this.state = OutboundState::AwaitingStatus,
                    Poll::Pending => return Poll::Pending,
                },
                OutboundState::AwaitingStatus => {
                    return match Pin::new(&mut this.status_rx).poll(cx) {
                        Poll::Ready(result) => {
                            let (status, message) =
                                result.unwrap_or_else(|_| (13, "handler ended without a status".to_string()));
                            this.state = OutboundState::Done;
                            Poll::Ready(Some(Ok(Frame::trailers(trailers(status, &message)))))
                        }
                        Poll::Pending => Poll::Pending,
                    };
                }
                OutboundState::Done => return Poll::Ready(None),
            }
        }
    }
}

/// Reads the inbound request body incrementally, decoding gRPC-framed
/// messages as they arrive (a streaming client may send many over time) and
/// forwarding each to the handler via `tx`.
async fn pump_inbound(mut body: Incoming, tx: mpsc::Sender<Vec<u8>>) {
    use http_body_util::BodyExt;
    let mut buf = BytesMut::new();
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    buf.extend_from_slice(&data);
                    while let Some(msg) = try_decode_one(&mut buf) {
                        if tx.send(msg).await.is_err() {
                            return;
                        }
                    }
                }
            }
            Some(Err(_)) => return,
            None => return,
        }
    }
}

async fn handle(
    req: http::Request<Incoming>,
    handler: GrpcHandler,
    user_data: SendableUserData,
) -> Result<http::Response<RespBody>, Infallible> {
    let path = req.uri().path().to_string();
    let Ok(path_c) = CString::new(path) else {
        return Ok(immediate_error(13, "method path contains an embedded NUL"));
    };

    let (inbound_tx, inbound_rx) = mpsc::channel::<Vec<u8>>(8);
    let (outbound_tx, outbound_rx) = mpsc::channel::<Vec<u8>>(8);
    let (status_tx, status_rx) = oneshot::channel::<(i32, String)>();

    tokio::spawn(pump_inbound(req.into_body(), inbound_tx));

    let stream = SendableStream(Box::into_raw(Box::new(GrpcStream { inbound_rx, outbound_tx })));

    // Detached on purpose: the response body below drives to completion by
    // watching `outbound_rx`/`status_rx`, not by joining this task.
    tokio::task::spawn_blocking(move || {
        // Rebinding to the whole value (rather than projecting `.0` at each
        // use) forces Rust's disjoint-closure-capture to move `stream`/
        // `user_data` themselves into this closure, not their raw-pointer
        // field — which would silently bypass the `unsafe impl Send` above.
        let stream = stream;
        let user_data = user_data;

        let mut out_status: i32 = 0;
        let mut out_msg: *mut u8 = std::ptr::null_mut();
        let mut out_msg_len: usize = 0;

        (handler)(path_c.as_ptr(), stream.0, user_data.0, &mut out_status, &mut out_msg, &mut out_msg_len);

        // SAFETY: the handler contract requires `out_msg`/`out_msg_len` come
        // from `zpr_alloc` (or are NULL/0), matching `Vec::from_raw_parts` here.
        let message = if out_msg.is_null() {
            String::new()
        } else {
            let bytes = unsafe { Vec::from_raw_parts(out_msg, out_msg_len, out_msg_len) };
            String::from_utf8_lossy(&bytes).into_owned()
        };

        // Reclaiming drops `outbound_tx`, which lets `OutboundBody` move
        // from Streaming to AwaitingStatus even if the handler wrote nothing.
        unsafe { drop(Box::from_raw(stream.0)) };
        let _ = status_tx.send((out_status, message));
    });

    let body = OutboundBody { rx: outbound_rx, status_rx, state: OutboundState::Streaming };
    Ok(http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(BoxBody::new(body))
        .expect("static response head is always valid"))
}

impl Service<http::Request<Incoming>> for PassthroughService {
    type Response = http::Response<RespBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: http::Request<Incoming>) -> Self::Future {
        let handler = self.handler;
        let user_data = self.user_data;
        Box::pin(handle(req, handler, user_data))
    }
}

async fn accept_loop(listener: TcpListener, service: PassthroughService, mut shutdown: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let io = TokioIo::new(stream);
                let svc = service.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
        }
    }
}

/// Starts a gRPC server bound to `bind_addr` (e.g. `"127.0.0.1:50052"`),
/// dispatching every call — unary or streaming — to `handler`. Runs on its
/// own thread with its own async runtime; returns once the socket is bound.
/// Returns `ZPR_OK` and fills `*out_handle` on success.
#[no_mangle]
pub extern "C" fn zpr_grpc_server_start(
    bind_addr: *const c_char,
    handler: GrpcHandler,
    user_data: *mut c_void,
    out_handle: *mut *mut GrpcServerHandle,
) -> i32 {
    if !out_handle.is_null() {
        unsafe { *out_handle = std::ptr::null_mut() };
    }
    ffi::guard(ZPR_ERR, move || {
        let addr = match unsafe { cstr_to_str(bind_addr) } {
            Ok(s) => s.to_string(),
            Err(e) => {
                ffi::set_last_error(e);
                return ZPR_ERR;
            }
        };
        let std_listener = match std::net::TcpListener::bind(&addr) {
            Ok(l) => l,
            Err(e) => {
                ffi::set_last_error(format!("failed to bind {addr:?}: {e}"));
                return ZPR_ERR;
            }
        };
        if let Err(e) = std_listener.set_nonblocking(true) {
            ffi::set_last_error(format!("failed to configure listener for {addr:?}: {e}"));
            return ZPR_ERR;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service = PassthroughService { handler, user_data: SendableUserData(user_data) };

        let thread = std::thread::Builder::new().name("zero-grpc-server".into()).spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build() {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async move {
                let Ok(listener) = TcpListener::from_std(std_listener) else { return };
                accept_loop(listener, service, shutdown_rx).await;
            });
        });

        match thread {
            Ok(thread) => {
                let handle = Box::into_raw(Box::new(GrpcServerHandle { shutdown: shutdown_tx, thread: Some(thread) }));
                if !out_handle.is_null() {
                    unsafe { *out_handle = handle };
                }
                ZPR_OK
            }
            Err(e) => {
                ffi::set_last_error(format!("failed to start gRPC server thread: {e}"));
                ZPR_ERR
            }
        }
    })
}

/// Signals the server to stop accepting new connections and joins its
/// thread. Blocks until shutdown completes. Frees `handle`.
#[no_mangle]
pub extern "C" fn zpr_grpc_server_stop(handle: *mut GrpcServerHandle) -> i32 {
    if handle.is_null() {
        return ZPR_ERR;
    }
    ffi::guard(ZPR_ERR, move || {
        let mut handle = unsafe { Box::from_raw(handle) };
        let _ = handle.shutdown.send(true);
        if let Some(thread) = handle.thread.take() {
            let _ = thread.join();
        }
        ZPR_OK
    })
}
