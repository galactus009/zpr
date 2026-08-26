//! HTTP client capability: one generic request/response function. Object
//! Pascal (outside of full frameworks like Indy) has no built-in HTTP client,
//! and none with TLS 1.3 / HTTP/2 support, so this covers GET/POST/PUT/DELETE/etc.
//! uniformly rather than binding a method per verb.

use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;

use crate::ffi::{self, cstr_to_str, runtime, string_to_cstring_ptr, ZprBuffer, ZprResult, ZPR_ERR, ZPR_OK};

static CLIENT: OnceLock<Client> = OnceLock::new();
static PROXY_OVERRIDE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// Explicitly sets the proxy used for HTTP requests, overriding the
/// `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`NO_PROXY` environment variables
/// that are otherwise honored automatically. Pass NULL to force *no* proxy
/// (including ignoring those environment variables).
///
/// Must be called before the first `zpr_http_request` — the underlying
/// client is built once and reused; calling this after that first request
/// has no effect and returns `ZPR_ERR`.
#[no_mangle]
pub extern "C" fn zpr_http_set_proxy(proxy_url: *const c_char) -> i32 {
    if CLIENT.get().is_some() {
        ffi::set_last_error("zpr_http_set_proxy called after the HTTP client was already built");
        return ZPR_ERR;
    }
    ffi::guard(ZPR_ERR, move || {
        let mut slot = PROXY_OVERRIDE.get_or_init(Default::default).lock().unwrap();
        if proxy_url.is_null() {
            *slot = Some(String::new()); // sentinel: explicit "no proxy"
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

fn client() -> &'static Client {
    CLIENT.get_or_init(|| {
        // Built via `block_on`, not just `.enter()`: `Client::builder().build()`
        // can spawn background tasks (idle-connection reaper, DNS resolver)
        // that need an *actively polling* runtime to ever run, not merely an
        // "entered" one — using `.enter()` here previously caused every
        // request to panic with "there is no reactor running".
        runtime().block_on(async {
            let mut builder = Client::builder();
            if let Some(slot) = PROXY_OVERRIDE.get() {
                match slot.lock().unwrap().as_deref() {
                    Some("") => builder = builder.no_proxy(),
                    Some(url) => {
                        if let Ok(proxy) = reqwest::Proxy::all(url) {
                            builder = builder.proxy(proxy);
                        }
                    }
                    None => {}
                }
            }
            // With no override, reqwest itself reads HTTP_PROXY/HTTPS_PROXY/
            // ALL_PROXY/NO_PROXY, so there is nothing else to do here.
            builder.build().expect("failed to build the shared HTTP client")
        })
    })
}

fn parse_headers(json: &str) -> Result<reqwest::header::HeaderMap, String> {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    let value: Value = serde_json::from_str(json).map_err(|e| format!("invalid headers JSON: {e}"))?;
    let obj = value.as_object().ok_or("headers JSON must be an object")?;
    let mut map = HeaderMap::new();
    for (k, v) in obj {
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| format!("invalid header name {k:?}: {e}"))?;
        let s = v.as_str().ok_or_else(|| format!("header {k:?} value must be a string"))?;
        let value = HeaderValue::from_str(s).map_err(|e| format!("invalid header value for {k:?}: {e}"))?;
        map.insert(name, value);
    }
    Ok(map)
}

/// Performs one HTTP request and blocks the calling thread until it completes.
///
/// `headers_json` is an optional (may be NULL) JSON object of `{"Header": "value"}`.
/// `body`/`body_len` may be NULL/0 for a request with no body.
/// On success (`ZPR_OK`): `*out_status` is the HTTP status code, `*out_headers_json`
/// is an owned JSON object of response headers (free with `zpr_string_free`),
/// and `*out_body` is the owned response body (free with `zpr_buffer_free`).
#[no_mangle]
pub extern "C" fn zpr_http_request(
    method: *const c_char,
    url: *const c_char,
    headers_json: *const c_char,
    body: *const u8,
    body_len: usize,
    timeout_ms: u32,
    out_status: *mut u16,
    out_headers_json: *mut *mut c_char,
    out_body: *mut ZprBuffer,
) -> i32 {
    if !out_status.is_null() {
        unsafe { *out_status = 0 };
    }
    if !out_headers_json.is_null() {
        unsafe { *out_headers_json = std::ptr::null_mut() };
    }
    if !out_body.is_null() {
        unsafe { *out_body = ZprBuffer::empty() };
    }

    ffi::guard(ZPR_ERR, move || {
        let result: ZprResult<()> = (|| {
            let method_str = unsafe { cstr_to_str(method) }.map_err(String::from)?;
            let url_str = unsafe { cstr_to_str(url) }.map_err(String::from)?;
            let method = reqwest::Method::from_bytes(method_str.as_bytes())
                .map_err(|e| format!("invalid HTTP method {method_str:?}: {e}"))?;

            let headers = if headers_json.is_null() {
                reqwest::header::HeaderMap::new()
            } else {
                let s = unsafe { cstr_to_str(headers_json) }.map_err(String::from)?;
                parse_headers(s)?
            };

            let body_bytes: Vec<u8> = if body.is_null() || body_len == 0 {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(body, body_len) }.to_vec()
            };

            let mut req = client().request(method, url_str).headers(headers);
            if !body_bytes.is_empty() {
                req = req.body(body_bytes);
            }
            if timeout_ms > 0 {
                req = req.timeout(Duration::from_millis(timeout_ms as u64));
            }

            // `req.send()` must be *called* from inside the polled future, not
            // passed in as an already-evaluated argument: reqwest eagerly
            // registers a `tokio::time::sleep` for the timeout the moment
            // `.send()` is invoked (not lazily on first poll), which needs an
            // active runtime context at that exact call site.
            let response = runtime()
                .block_on(async { req.send().await })
                .map_err(|e| format!("HTTP request failed: {e}"))?;

            let status = response.status().as_u16();
            let mut header_obj = serde_json::Map::new();
            for name in response.headers().keys() {
                let joined = response
                    .headers()
                    .get_all(name)
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .collect::<Vec<_>>()
                    .join(", ");
                header_obj.insert(name.to_string(), Value::String(joined));
            }
            let headers_json_out =
                serde_json::to_string(&Value::Object(header_obj)).unwrap_or_else(|_| "{}".to_string());

            let body = runtime()
                .block_on(async { response.bytes().await })
                .map_err(|e| format!("failed to read HTTP response body: {e}"))?;

            if !out_status.is_null() {
                unsafe { *out_status = status };
            }
            if !out_headers_json.is_null() {
                unsafe { *out_headers_json = string_to_cstring_ptr(headers_json_out) };
            }
            if !out_body.is_null() {
                unsafe { *out_body = ZprBuffer::from_vec(body.to_vec()) };
            }
            Ok(())
        })();

        match result {
            Ok(()) => ZPR_OK,
            Err(e) => {
                ffi::set_last_error(e);
                ZPR_ERR
            }
        }
    })
}
