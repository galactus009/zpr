//! Starts the JSON→gRPC transcoder against a running daemon so it can be poked
//! with curl. Not a test — a harness for verifying the seam by hand.
//!
//!   cargo run --example transcode_smoke -- <descriptors.binpb> <upstream> <bind>
use std::ffi::CString;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let desc_path = args.get(1).expect("descriptor set path");
    let upstream = args.get(2).map(String::as_str).unwrap_or("http://127.0.0.1:9491");
    let bind = args.get(3).map(String::as_str).unwrap_or("127.0.0.1:8099");

    let bytes = std::fs::read(desc_path).expect("read descriptor set");
    let pool = zpr::grpc::zpr_protobuf_pool_new(bytes.as_ptr(), bytes.len());
    assert!(!pool.is_null(), "pool failed to load");

    let c_bind = CString::new(bind).unwrap();
    let c_up = CString::new(upstream).unwrap();
    let mut handle = std::ptr::null_mut();
    let rc = zpr::transcode::zpr_transcode_start(
        c_bind.as_ptr(), c_up.as_ptr(), pool, 5000, &mut handle,
    );
    assert_eq!(rc, 0, "transcode_start failed");
    println!("transcoding {bind} -> {upstream}");
    std::thread::sleep(std::time::Duration::from_secs(45));
    zpr::transcode::zpr_transcode_stop(handle);
}
