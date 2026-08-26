//! JSON capability: parse, query, build, and stringify `serde_json::Value` trees
//! through an opaque handle. Object Pascal has no JSON support of its own, so
//! this gives it a full read/write round trip instead of just "parse".

use std::os::raw::c_char;

use serde_json::Value;

use crate::ffi::{self, cstr_to_str, string_to_cstring_ptr, ZPR_ERR, ZPR_OK};

/// Opaque handle to a JSON value. Always free with `zpr_json_free`.
pub struct JsonValue(pub Value);

#[repr(i32)]
#[allow(dead_code)]
enum JsonKind {
    Null = 0,
    Bool = 1,
    Number = 2,
    String = 3,
    Array = 4,
    Object = 5,
}

fn handle_ref<'a>(h: *const JsonValue) -> Option<&'a Value> {
    if h.is_null() {
        None
    } else {
        Some(unsafe { &(*h).0 })
    }
}

/// Parses `text` (NUL-terminated UTF-8 JSON) into a new handle. Returns NULL
/// on a parse error — check `zpr_last_error()`.
#[no_mangle]
pub extern "C" fn zpr_json_parse(text: *const c_char) -> *mut JsonValue {
    ffi::guard(std::ptr::null_mut(), move || {
        let text = match unsafe { cstr_to_str(text) } {
            Ok(s) => s,
            Err(e) => {
                ffi::set_last_error(e);
                return std::ptr::null_mut();
            }
        };
        match serde_json::from_str::<Value>(text) {
            Ok(v) => Box::into_raw(Box::new(JsonValue(v))),
            Err(e) => {
                ffi::set_last_error(format!("json parse error: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

/// Frees a handle returned by any `zpr_json_*` function.
#[no_mangle]
pub extern "C" fn zpr_json_free(handle: *mut JsonValue) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

/// Serializes `handle` back to a JSON string (`pretty != 0` for indented output).
#[no_mangle]
pub extern "C" fn zpr_json_stringify(handle: *const JsonValue, pretty: i32) -> *mut c_char {
    ffi::guard(std::ptr::null_mut(), move || {
        let Some(v) = handle_ref(handle) else {
            ffi::set_last_error("null handle passed to zpr_json_stringify");
            return std::ptr::null_mut();
        };
        let s = if pretty != 0 {
            serde_json::to_string_pretty(v)
        } else {
            serde_json::to_string(v)
        };
        match s {
            Ok(s) => string_to_cstring_ptr(s),
            Err(e) => {
                ffi::set_last_error(format!("json stringify error: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

/// Returns a `JsonKind` value, or -1 if `handle` is NULL.
#[no_mangle]
pub extern "C" fn zpr_json_kind(handle: *const JsonValue) -> i32 {
    match handle_ref(handle) {
        None => -1,
        Some(Value::Null) => JsonKind::Null as i32,
        Some(Value::Bool(_)) => JsonKind::Bool as i32,
        Some(Value::Number(_)) => JsonKind::Number as i32,
        Some(Value::String(_)) => JsonKind::String as i32,
        Some(Value::Array(_)) => JsonKind::Array as i32,
        Some(Value::Object(_)) => JsonKind::Object as i32,
    }
}

#[no_mangle]
pub extern "C" fn zpr_json_as_bool(handle: *const JsonValue, out: *mut u8) -> i32 {
    match handle_ref(handle).and_then(Value::as_bool) {
        Some(b) => {
            if !out.is_null() {
                unsafe { *out = b as u8 };
            }
            ZPR_OK
        }
        None => ZPR_ERR,
    }
}

#[no_mangle]
pub extern "C" fn zpr_json_as_f64(handle: *const JsonValue, out: *mut f64) -> i32 {
    match handle_ref(handle).and_then(Value::as_f64) {
        Some(n) => {
            if !out.is_null() {
                unsafe { *out = n };
            }
            ZPR_OK
        }
        None => ZPR_ERR,
    }
}

/// Returns the string value as an owned C string, or NULL if `handle` isn't a string.
#[no_mangle]
pub extern "C" fn zpr_json_as_string(handle: *const JsonValue) -> *mut c_char {
    match handle_ref(handle).and_then(Value::as_str) {
        Some(s) => string_to_cstring_ptr(s.to_string()),
        None => std::ptr::null_mut(),
    }
}

/// Returns the array length, or -1 if `handle` isn't an array.
#[no_mangle]
pub extern "C" fn zpr_json_array_len(handle: *const JsonValue) -> isize {
    match handle_ref(handle).and_then(Value::as_array) {
        Some(a) => a.len() as isize,
        None => -1,
    }
}

/// Returns a new handle cloned from `array[index]`, or NULL if out of range
/// or `handle` isn't an array.
#[no_mangle]
pub extern "C" fn zpr_json_array_get(handle: *const JsonValue, index: usize) -> *mut JsonValue {
    match handle_ref(handle).and_then(Value::as_array).and_then(|a| a.get(index)) {
        Some(v) => Box::into_raw(Box::new(JsonValue(v.clone()))),
        None => std::ptr::null_mut(),
    }
}

/// Returns a new handle cloned from `object[key]`, or NULL if the key is
/// absent or `handle` isn't an object.
#[no_mangle]
pub extern "C" fn zpr_json_object_get(handle: *const JsonValue, key: *const c_char) -> *mut JsonValue {
    ffi::guard(std::ptr::null_mut(), move || {
        let key = match unsafe { cstr_to_str(key) } {
            Ok(s) => s,
            Err(e) => {
                ffi::set_last_error(e);
                return std::ptr::null_mut();
            }
        };
        match handle_ref(handle).and_then(Value::as_object).and_then(|o| o.get(key)) {
            Some(v) => Box::into_raw(Box::new(JsonValue(v.clone()))),
            None => std::ptr::null_mut(),
        }
    })
}

/// Returns the object's keys as a JSON string array (e.g. `["a","b"]`), or
/// NULL if `handle` isn't an object.
#[no_mangle]
pub extern "C" fn zpr_json_object_keys(handle: *const JsonValue) -> *mut c_char {
    match handle_ref(handle).and_then(Value::as_object) {
        Some(o) => {
            let keys: Vec<&str> = o.keys().map(String::as_str).collect();
            string_to_cstring_ptr(serde_json::to_string(&keys).unwrap_or_else(|_| "[]".to_string()))
        }
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn zpr_json_new_null() -> *mut JsonValue {
    Box::into_raw(Box::new(JsonValue(Value::Null)))
}

#[no_mangle]
pub extern "C" fn zpr_json_new_bool(b: u8) -> *mut JsonValue {
    Box::into_raw(Box::new(JsonValue(Value::Bool(b != 0))))
}

#[no_mangle]
pub extern "C" fn zpr_json_new_f64(n: f64) -> *mut JsonValue {
    let v = serde_json::Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null);
    Box::into_raw(Box::new(JsonValue(v)))
}

#[no_mangle]
pub extern "C" fn zpr_json_new_string(s: *const c_char) -> *mut JsonValue {
    ffi::guard(std::ptr::null_mut(), move || match unsafe { cstr_to_str(s) } {
        Ok(s) => Box::into_raw(Box::new(JsonValue(Value::String(s.to_string())))),
        Err(e) => {
            ffi::set_last_error(e);
            std::ptr::null_mut()
        }
    })
}

#[no_mangle]
pub extern "C" fn zpr_json_new_array() -> *mut JsonValue {
    Box::into_raw(Box::new(JsonValue(Value::Array(Vec::new()))))
}

#[no_mangle]
pub extern "C" fn zpr_json_new_object() -> *mut JsonValue {
    Box::into_raw(Box::new(JsonValue(Value::Object(serde_json::Map::new()))))
}

/// Appends `value` (consumed — do not free it separately) to `array`.
/// Returns `ZPR_ERR` if `array` isn't an array handle.
#[no_mangle]
pub extern "C" fn zpr_json_array_push(array: *mut JsonValue, value: *mut JsonValue) -> i32 {
    if array.is_null() || value.is_null() {
        return ZPR_ERR;
    }
    let value = unsafe { Box::from_raw(value) }.0;
    let arr = unsafe { &mut (*array).0 };
    match arr.as_array_mut() {
        Some(a) => {
            a.push(value);
            ZPR_OK
        }
        None => ZPR_ERR,
    }
}

/// Sets `object[key] = value` (`value` is consumed — do not free it
/// separately). Returns `ZPR_ERR` if `object` isn't an object handle.
#[no_mangle]
pub extern "C" fn zpr_json_object_set(object: *mut JsonValue, key: *const c_char, value: *mut JsonValue) -> i32 {
    if object.is_null() || value.is_null() {
        return ZPR_ERR;
    }
    ffi::guard(ZPR_ERR, move || {
        let key = match unsafe { cstr_to_str(key) } {
            Ok(s) => s.to_string(),
            Err(e) => {
                ffi::set_last_error(e);
                return ZPR_ERR;
            }
        };
        let value = unsafe { Box::from_raw(value) }.0;
        let obj = unsafe { &mut (*object).0 };
        match obj.as_object_mut() {
            Some(o) => {
                o.insert(key, value);
                ZPR_OK
            }
            None => ZPR_ERR,
        }
    })
}
