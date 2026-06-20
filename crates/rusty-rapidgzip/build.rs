// Links the external decode libraries when their features are enabled.
//
// `libdeflate`: links the static libdeflate. The library directory defaults to
// the nix-store libdeflate-1.25 used during the experiment, overridable via
// $LIBDEFLATE_LIB_DIR.
//
// `isal`: compiles the small C shim (src/isal_shim.c) against ISA-L's headers
// and links the shared libisal. Paths default to the nix-store isa-l-2.31.1 and
// are overridable via $ISAL_LIB_DIR / $ISAL_INCLUDE_DIR.
fn main() {
    println!("cargo:rerun-if-env-changed=LIBDEFLATE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=ISAL_LIB_DIR");
    println!("cargo:rerun-if-env-changed=ISAL_INCLUDE_DIR");

    if std::env::var_os("CARGO_FEATURE_LIBDEFLATE").is_some() {
        let dir = std::env::var("LIBDEFLATE_LIB_DIR").unwrap_or_else(|_| {
            "/nix/store/04valqpy6qymd3zvirs4h6240pamhbkh-libdeflate-1.25/lib".to_string()
        });
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=static=deflate");
    }

    if std::env::var_os("CARGO_FEATURE_ISAL").is_some() {
        let isal_root = "/nix/store/si3q8xbkvcyl496wa0nz2ard39w8f21c-isa-l-2.31.1";
        let lib_dir =
            std::env::var("ISAL_LIB_DIR").unwrap_or_else(|_| format!("{isal_root}/lib"));
        let include_dir =
            std::env::var("ISAL_INCLUDE_DIR").unwrap_or_else(|_| format!("{isal_root}/include"));

        println!("cargo:rerun-if-changed=src/isal_shim.c");
        cc::Build::new()
            .file("src/isal_shim.c")
            .include(&include_dir)
            .opt_level(3)
            .compile("rrg_isal_shim");

        println!("cargo:rustc-link-search=native={lib_dir}");
        println!("cargo:rustc-link-lib=dylib=isal");
        // libisal is a shared object; embed an rpath so the binary finds it at
        // runtime without LD_LIBRARY_PATH.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
    }
}
