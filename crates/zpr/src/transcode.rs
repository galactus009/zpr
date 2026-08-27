//! REST/JSON in front of a gRPC daemon, driven entirely by a descriptor set.
//!
//! ══ WHY THIS IS NOT A ROUTE TABLE ═══════════════════════════════════════════
//!
//! The obvious way to put JSON in front of gRPC is a table mapping paths to
//! methods, hand-kept beside the service. Every such table this design has been
//! measured against fell behind the contract it described — a method added to
//! the proto and not to the table is a 404 from a healthy daemon, which reads
//! like a routing fault and is not one.
//!
//! So nothing here knows a single method name. The `FileDescriptorSet` says
//! which services exist, which methods they have and what those methods take
//! and return; this reads the path, asks the pool, and forwards. A method added
//! to the proto works the moment the descriptor set is rebuilt, and this file
//! does not change.
//!
//! ══ WHAT IT SPEAKS ══════════════════════════════════════════════════════════
//!
//!   POST /package.Service/Method    Content-Type: application/json
//!   body: the request message as JSON
//!   →     the response message as JSON, or an error object
//!
//! That is the Connect protocol's unary shape, which is also what an ordinary
//! HTTP client does naturally — no annotations in the proto, no URL templates to
//! parse. RESTful paths (`GET /v1/things/{id}`) need `google.api.http` options
//! on every method; if those ever exist, they belong on top of this rather than
//! instead of it.
//!
//! ⚠ UNARY ONLY, DELIBERATELY, FOR NOW. Streaming over this seam needs a framing
//! decision (chunked JSON lines? Connect's enveloped stream?) that should be made
//! against a real client rather than guessed at here. A streaming method is
//! refused with a clear message rather than half-served.

use std::convert::Infallible;
use std::os::raw::c_char;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::ffi::{self, cstr_to_str, ZPR_ERR, ZPR_OK};

/// Opaque running-transcoder handle. Always stop with `zpr_transcode_stop`.
pub struct TranscodeHandle {
    shutdown: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Config {
    pool: DescriptorPool,
    upstream: String,
    timeout_ms: u32,
}

/// Splits `/package.Service/Method` into its two halves.
fn split_path(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix('/')?;
    let (service, method) = rest.rsplit_once('/')?;
    if service.is_empty() || method.is_empty() {
        return None;
    }
    Some((service, method))
}

fn json_error(status: u16, msg: &str) -> http::Response<Full<Bytes>> {
    let body = serde_json::json!({ "code": status, "message": msg }).to_string();
    http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static response head is always valid")
}

/// gRPC status codes are not HTTP ones, and collapsing them all to 500 throws
/// away the only thing that tells a caller whether to fix the request or retry.
fn http_status_for_grpc(code: i32) -> u16 {
    match code {
        0 => 200,
        3 => 400,  // INVALID_ARGUMENT
        5 => 404,  // NOT_FOUND
        7 => 403,  // PERMISSION_DENIED
        16 => 401, // UNAUTHENTICATED
        8 => 429,  // RESOURCE_EXHAUSTED
        12 => 501, // UNIMPLEMENTED
        14 => 503, // UNAVAILABLE
        4 => 504,  // DEADLINE_EXCEEDED
        _ => 500,
    }
}

async fn serve_one(
    req: http::Request<Incoming>,
    cfg: Arc<Config>,
) -> Result<http::Response<Full<Bytes>>, Infallible> {
    if req.method() != http::Method::POST {
        return Ok(json_error(405, "this surface takes POST /package.Service/Method"));
    }
    let path = req.uri().path().to_string();
    let Some((service_name, method_name)) = split_path(&path) else {
        return Ok(json_error(404, "path must be /package.Service/Method"));
    };

    let Some(service) = cfg.pool.get_service_by_name(service_name) else {
        return Ok(json_error(404, &format!("no service {service_name:?} in the descriptor set")));
    };
    let Some(method) = service.methods().find(|m| m.name() == method_name) else {
        return Ok(json_error(404, &format!("service {service_name:?} has no method {method_name:?}")));
    };
    if method.is_client_streaming() || method.is_server_streaming() {
        return Ok(json_error(
            501,
            &format!("{method_name:?} is a streaming method; this surface transcodes unary calls only"),
        ));
    }

    let body = match req.into_body().collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return Ok(json_error(400, &format!("could not read the request body: {e}"))),
    };
    let json_text = if body.is_empty() { "{}" } else { std::str::from_utf8(&body).unwrap_or("") };
    if json_text.is_empty() {
        return Ok(json_error(400, "request body is not valid UTF-8"));
    }

    // JSON → protobuf, against the method's OWN input type. An unknown field is
    // an error rather than a silent drop: a caller who misspells one has written
    // a request that does not mean what they think it means.
    let mut de = serde_json::Deserializer::from_str(json_text);
    let request_msg = match DynamicMessage::deserialize(method.input(), &mut de) {
        Ok(m) => m,
        Err(e) => return Ok(json_error(400, &format!("request JSON does not match {}: {e}", method.input().full_name()))),
    };
    let request_bytes = request_msg.encode_to_vec();

    let (reply, grpc_status, err) =
        crate::grpc::call_unary_bytes(&cfg.upstream, &path, request_bytes, cfg.timeout_ms).await;

    if grpc_status != 0 {
        return Ok(json_error(http_status_for_grpc(grpc_status), &err));
    }
    let reply_msg = match DynamicMessage::decode(method.output(), reply.as_slice()) {
        Ok(m) => m,
        Err(e) => return Ok(json_error(502, &format!("upstream reply is not a {}: {e}", method.output().full_name()))),
    };

    // `stringify_64_bit_integers` off: protojson quotes 64-bit ints as strings by
    // default, which is correct JSON but surprises every caller that then has to
    // parse a number out of a string. Numbers here, as the wire meant them.
    let opts = SerializeOptions::new().stringify_64_bit_integers(false);
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::new(&mut buf);
    if let Err(e) = reply_msg.serialize_with_options(&mut ser, &opts) {
        return Ok(json_error(500, &format!("could not render the reply as JSON: {e}")));
    }
    Ok(http::Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(buf)))
        .expect("static response head is always valid"))
}

/// Starts a JSON→gRPC transcoder on `bind_addr`, forwarding to `upstream`
/// (an h2c gRPC endpoint like `"http://127.0.0.1:9091"`), routing by `pool`.
///
/// The pool is COPIED, so the caller may free theirs immediately; nothing here
/// depends on that handle outliving this call.
#[no_mangle]
pub extern "C" fn zpr_transcode_start(
    bind_addr: *const c_char,
    upstream: *const c_char,
    pool: *mut crate::grpc::DescriptorPoolHandle,
    timeout_ms: u32,
    out_handle: *mut *mut TranscodeHandle,
) -> i32 {
    if !out_handle.is_null() {
        unsafe { *out_handle = std::ptr::null_mut() };
    }
    ffi::guard(ZPR_ERR, move || {
        if pool.is_null() {
            ffi::set_last_error("null descriptor pool passed to zpr_transcode_start");
            return ZPR_ERR;
        }
        let addr = match unsafe { cstr_to_str(bind_addr) } {
            Ok(s) => s.to_string(),
            Err(e) => { ffi::set_last_error(e); return ZPR_ERR; }
        };
        let upstream = match unsafe { cstr_to_str(upstream) } {
            Ok(s) => s.to_string(),
            Err(e) => { ffi::set_last_error(e); return ZPR_ERR; }
        };
        let cfg = Arc::new(Config {
            pool: unsafe { &*pool }.pool().clone(),
            upstream,
            timeout_ms,
        });

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

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let thread = std::thread::Builder::new().name("zpr-transcode".into()).spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build() {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async move {
                let Ok(listener) = TcpListener::from_std(std_listener) else { return };
                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => break,
                        accepted = listener.accept() => {
                            let Ok((stream, _)) = accepted else { continue };
                            let io = TokioIo::new(stream);
                            let cfg = Arc::clone(&cfg);
                            tokio::spawn(async move {
                                let svc = service_fn(move |req| serve_one(req, Arc::clone(&cfg)));
                                // HTTP/1.1 with an h2c upgrade path: an ordinary
                                // curl and a modern client both work.
                                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                                    .serve_connection(io, svc)
                                    .await;
                            });
                        }
                    }
                }
            });
        });

        match thread {
            Ok(thread) => {
                let handle = Box::into_raw(Box::new(TranscodeHandle { shutdown: shutdown_tx, thread: Some(thread) }));
                if !out_handle.is_null() {
                    unsafe { *out_handle = handle };
                }
                ZPR_OK
            }
            Err(e) => {
                ffi::set_last_error(format!("failed to start the transcoder thread: {e}"));
                ZPR_ERR
            }
        }
    })
}

/// Stops the transcoder and joins its thread. Frees `handle`.
#[no_mangle]
pub extern "C" fn zpr_transcode_stop(handle: *mut TranscodeHandle) -> i32 {
    if handle.is_null() {
        return ZPR_OK;
    }
    ffi::guard(ZPR_ERR, move || {
        let mut h = unsafe { Box::from_raw(handle) };
        let _ = h.shutdown.send(true);
        if let Some(t) = h.thread.take() {
            let _ = t.join();
        }
        ZPR_OK
    })
}
