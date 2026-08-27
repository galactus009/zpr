//! zpr (Zero Portable Runtime): a C-ABI shim exposing capabilities Object
//! Pascal (Lazarus/FPC and Delphi) has no native equivalent for — JSON, an
//! HTTP client, and a generic gRPC/protobuf client + server. See each
//! module for its capability.

pub mod ffi;
pub mod grpc;
pub mod grpc_server;
pub mod http;
pub mod json;
pub mod transcode;

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::OnceLock;

static VERSION: OnceLock<CString> = OnceLock::new();

/// Returns this library's version string (e.g. `"0.1.0"`). The pointer is
/// static for the process lifetime and must not be freed.
#[no_mangle]
pub extern "C" fn zpr_version() -> *const c_char {
    VERSION.get_or_init(|| CString::new(env!("CARGO_PKG_VERSION")).unwrap()).as_ptr()
}
