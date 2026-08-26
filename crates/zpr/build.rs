fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    let config = cbindgen::Config::from_file(format!("{crate_dir}/cbindgen.toml")).unwrap_or_default();

    match cbindgen::Builder::new().with_crate(&crate_dir).with_config(config).generate() {
        Ok(bindings) => {
            bindings.write_to_file(format!("{crate_dir}/include/zpr.h"));
        }
        Err(e) => {
            // Best-effort: a stale or missing header shouldn't break the
            // build, only a broken *library* should.
            println!("cargo:warning=zpr: failed to generate include/zpr.h: {e}");
        }
    }
}
