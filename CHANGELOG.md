# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

### Changed
- **libdeflate is now built from vendored C source** via the `libdeflate-sys`
  crate and linked statically, instead of linking a system/Nix-store libdeflate.
  The default `libdeflate` backend now builds portably on Linux, macOS and
  Windows with no external library. Build with `--no-default-features` for the
  pure-Rust kernel and no C dependency.
- `mmap`'s sequential-access hint (`madvise`) is now `#[cfg(unix)]`-gated so the
  crate compiles on Windows.
- Declared license corrected to `MIT` (matches the `LICENSE` file).

### Added
- Self-contained Miri UB test (`tests/miri_edge.rs`) for the back-reference copy
  kernels, run under both Stacked and Tree Borrows.
- Fully static (musl) binary build and portable Linux / macOS / Windows release
  artifacts; CI now also runs native macOS/Windows tests and exercises the static
  binary inside plain Alpine and Debian containers.

### Removed
- The experimental process-isolated binary (`rusty-rapidgzip-bin-isolated`).
- The experimental `zune` decode backend and the vendored `zune-inflate` crate.

## [0.1.0]

- Initial release: streaming, parallel gzip decoder.
