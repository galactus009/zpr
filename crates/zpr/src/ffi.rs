//! Shared C-ABI plumbing: owned buffers, owned strings, and a last-error slot.
//! Every other module in this crate builds its FFI surface out of these primitives.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;

pub const ZPR_OK: i32 = 0;
pub const ZPR_ERR: i32 = -1;

/// The shared multi-threaded runtime every blocking `zpr_*` call drives its
/// async work through via `block_on`. Built lazily on first use, shared by
/// the gRPC and HTTP capabilities so the process only ever pays for one.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("failed to start the zpr async runtime")
    })
}

/// Convenience alias used internally by capability modules for fallible
/// operations whose error is a human-readable message destined for
/// `set_last_error`.
pub type ZprResult<T> = Result<T, String>;

/// A Rust-owned byte buffer handed to the caller. Always free with `zpr_buffer_free`.
#[repr(C)]
pub struct ZprBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl ZprBuffer {
    pub fn empty() -> Self {
        Self { data: std::ptr::null_mut(), len: 0, cap: 0 }
    }

    pub fn from_vec(v: Vec<u8>) -> Self {
        let mut v = v;
        let data = v.as_mut_ptr();
        let len = v.len();
        let cap = v.capacity();
        std::mem::forget(v);
        Self { data, len, cap }
    }
}

/// Frees a buffer previously returned by this library. Safe to call on an
/// empty/zeroed buffer.
#[no_mangle]
pub extern "C" fn zpr_buffer_free(buf: ZprBuffer) {
    if !buf.data.is_null() {
        unsafe { drop(Vec::from_raw_parts(buf.data, buf.len, buf.cap)) };
    }
}

/// Allocates a `len`-byte buffer using this library's allocator. Intended for
/// callbacks (e.g. a registered gRPC server handler) that need to hand a
/// Rust-owned response back across the FFI boundary — Pascal's heap and
/// Rust's allocator are not interchangeable, so any buffer freed by
/// `zpr_buffer_free` must have been produced by this function (or by this
/// library internally).
#[no_mangle]
pub extern "C" fn zpr_alloc(len: usize) -> *mut u8 {
    let mut v = vec![0u8; len];
    let ptr = v.as_mut_ptr();
    std::mem::forget(v);
    ptr
}

/// Frees a string previously returned by this library (e.g. `zpr_protobuf_binary_to_json`).
#[no_mangle]
pub extern "C" fn zpr_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

pub fn set_last_error(msg: impl Into<String>) {
    let sanitized = msg.into().replace('\0', "");
    let c = CString::new(sanitized).unwrap_or_else(|_| CString::new("zero: error").unwrap());
    LAST_ERROR.with(|slot| *slot.borrow_mut() = Some(c));
}

/// Returns the last error message set on the *calling thread*, or NULL if
/// none. The pointer is valid until the next call into this library on this
/// thread and must not be freed.
#[no_mangle]
pub extern "C" fn zpr_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    })
}

/// # Safety
/// `ptr` must be NULL or a valid pointer to a NUL-terminated UTF-8 C string
/// that outlives the returned borrow.
pub unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, &'static str> {
    if ptr.is_null() {
        return Err("null pointer passed for a required string argument");
    }
    CStr::from_ptr(ptr).to_str().map_err(|_| "argument is not valid UTF-8")
}

pub fn string_to_cstring_ptr(s: String) -> *mut c_char {
    match CString::new(s.replace('\0', "")) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Runs `f`, catching any panic so it can never unwind across the FFI
/// boundary (that's undefined behavior). On panic, records the panic message
/// as the last error and returns `default`.
///
/// Every FFI entry point in this crate is a fresh, self-contained call — a
/// panic here never leaves shared state half-mutated in a way a *later*
/// call could observe (any raw pointers involved are either not yet handed
/// out or explicitly documented as no longer valid), so asserting
/// unwind-safety at this single boundary is sound even though the ordinary
/// `UnwindSafe` bound (e.g. via a raw pointer to a type containing a
/// `JoinHandle`) would otherwise reject some of these closures.
pub fn guard<T>(default: T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "zero: internal panic".to_string()
            };
            set_last_error(msg);
            default
        }
    }
}
