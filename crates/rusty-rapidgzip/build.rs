// Links the external libdeflate static library when the `libdeflate` feature is
// enabled. The library directory defaults to the nix-store libdeflate-1.25 used
// during the experiment and can be overridden via $LIBDEFLATE_LIB_DIR.
fn main() {
    println!("cargo:rerun-if-env-changed=LIBDEFLATE_LIB_DIR");
    if std::env::var_os("CARGO_FEATURE_LIBDEFLATE").is_some() {
        let dir = std::env::var("LIBDEFLATE_LIB_DIR").unwrap_or_else(|_| {
            "/nix/store/04valqpy6qymd3zvirs4h6240pamhbkh-libdeflate-1.25/lib".to_string()
        });
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=static=deflate");
    }
}
