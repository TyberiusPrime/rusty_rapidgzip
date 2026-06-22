{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-26.05"; 
    utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nmattia/naersk";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      utils,
      naersk,
      rust-overlay,
    }:
    utils.lib.eachDefaultSystem (
      system:
      let
        #pkgs = nixpkgs.legacyPackages."${system}";
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        # Nightly toolchain for local dev shell
        # rust = pkgs.rust-bin.selectLatestNightlyWith (
        #   toolchain:
        #   toolchain.default.override {
        #     extensions = [ "rust-analyzer" ];
        #     targets = [ "x86_64-unknown-linux-musl" ];
        #   }
        # );

        # Stable toolchain for CI checks (pinned via flake.lock)
        rustStable = pkgs.rust-bin.stable.latest.default;
        rust = rustStable;

        # Pinned nightly toolchain for the Miri UB check. Miri ships only on
        # nightly, as the `miri` rustup component; `rust-src` is needed so
        # `cargo miri setup` can build the std sysroot offline.
        #
        # The date must be one the *locked* rust-overlay knows about (run
        # `nix flake metadata` / bump with `nix flake update rust-overlay` to
        # move it forward — the newest the current lock exposes is 2026-05-25).
        rustMiri = pkgs.rust-bin.nightly."2026-05-25".default.override {
          extensions = [ "miri" "rust-src" ];
        };

        # Vendor dir for the Miri check. `cargo miri setup` rebuilds the std
        # sysroot *from source*, so std's own crate deps (hashbrown, libc, …)
        # must live in the same source-replacement directory as our project's
        # deps — otherwise the offline sysroot build fails with
        # "no matching package named `hashbrown`". We vendor both lockfiles and
        # merge them into a single `cargo-vendor-dir` (the project's Cargo.lock
        # stays at the root, since cargoSetupHook diffs it against ours).
        miriProjectVendor = pkgs.rustPlatform.importCargoLock {
          lockFile = ./Cargo.lock;
        };
        miriStdVendor = pkgs.rustPlatform.importCargoLock {
          lockFile = "${rustMiri}/lib/rustlib/src/rust/library/Cargo.lock";
        };
        miriCargoVendor = pkgs.runCommandLocal "cargo-vendor-dir" { } ''
          mkdir -p "$out/.cargo"
          for src in ${miriStdVendor} ${miriProjectVendor}; do
            for entry in "$src"/*; do
              name=$(basename "$entry")
              [ "$name" = "Cargo.lock" ] && continue
              ln -sfn "$entry" "$out/$name"
            done
          done
          cp ${miriStdVendor}/.cargo/config.toml "$out/.cargo/config.toml"
          cp -f ${./Cargo.lock} "$out/Cargo.lock"
        '';

        # Override the version used in naersk
        naersk-lib = naersk.lib."${system}".override {
          cargo = rust;
          rustc = rust;
        };

        # naersk with stable rust for reproducible CI checks
        naersk-lib-ci = naersk.lib."${system}".override {
          cargo = rustStable;
          rustc = rustStable;
        };

        # ── Windows cross-compilation ───────────────────────────────────────
        # Minimal stable toolchain with the MinGW Windows target added, plus the
        # MinGW cross-compiler from nixpkgs. Used by `packages.check-windows`
        # (and `checks.check-windows`) to compile-check the workspace for
        # x86_64-pc-windows-gnu from Linux — catching cfg(windows)/type/API
        # breakage without a real Windows runner. libdeflate (the default
        # backend) is built from source by `libdeflate-sys`, so the MinGW gcc
        # must also be the C compiler the `cc` crate uses for that target.
        rust-windows = pkgs.rust-bin.stable.latest.minimal.override {
          targets = [ "x86_64-pc-windows-gnu" ];
        };
        mingw = pkgs.pkgsCross.mingwW64.stdenv.cc;
        naersk-lib-windows = naersk.lib."${system}".override {
          cargo = rust-windows;
          rustc = rust-windows;
        };

        # ── Fully static (musl) Linux binary ────────────────────────────────
        # The truly portable "runs on any Linux distro" build: statically linked
        # against musl libc, so there is no glibc version coupling (the glibc
        # binary above is built against a very recent nixpkgs glibc and would
        # fail the symbol-version check on older distros). libdeflate's C source
        # is compiled for the musl target by the musl cross-gcc and linked in.
        rust-musl = pkgs.rust-bin.stable.latest.default.override {
          targets = [ "x86_64-unknown-linux-musl" ];
        };
        naersk-lib-musl = naersk.lib."${system}".override {
          cargo = rust-musl;
          rustc = rust-musl;
        };
        muslCC = pkgs.pkgsCross.musl64.stdenv.cc;

        bacon = pkgs.bacon;

        # cargo-afl is not in nixpkgs, so we build it from the crates.io tarball.
        # The build produces just the `cargo-afl` binary — it does NOT compile
        # aflplusplus (build.rs only does that during `cargo install`, which we
        # bypass). Instead, we populate the xdg data dir cargo-afl looks in
        # with symlinks to `pkgs.aflplusplus` and wrap cargo-afl so it finds
        # them (plus the nix `cargo`, otherwise it panics with NotPresent).
        cargo-afl-unwrapped = pkgs.rustPlatform.buildRustPackage rec {
          pname = "cargo-afl";
          version = "0.18.1";
          src = pkgs.fetchCrate {
            inherit pname version;
            hash = "sha256-W2ELM28vHs8xjgh0gRyH/O17kDgMFxKNOnnlbputQb0=";
          };
          cargoLock.lockFile = "${src}/Cargo.lock";
          doCheck = false;
        };

        # cargo-afl-common uses `rustc-<semver>-<short-hash>/afl.rs-<ver>` as
        # the xdg subdirectory. Extract it from the pinned rust toolchain so
        # the path matches at runtime.
        aflRustcDir = pkgs.lib.removeSuffix "\n" (
          builtins.readFile (
            pkgs.runCommand "afl-rustc-dir" { } ''
              ${rust}/bin/rustc -vV | ${pkgs.gawk}/bin/awk '
                /^rustc/ { ver=$2 }
                /^commit-hash:/ { printf "rustc-%s-%s", ver, substr($2, 1, 7) }
              ' > $out
            ''
          )
        );

        aflXdgDataHome = pkgs.runCommand "afl-xdg-data-home" { } ''
          base=$out/afl.rs/${aflRustcDir}/afl.rs-${cargo-afl-unwrapped.version}
          mkdir -p "$base/afl/bin" "$base/afl-llvm"
          for b in ${pkgs.aflplusplus}/bin/afl-*; do
            ln -s "$b" "$base/afl/bin/$(basename "$b")"
          done
          ln -s ${pkgs.aflplusplus}/lib/afl/afl-compiler-rt.o "$base/afl-llvm/afl-compiler-rt.o"
        '';

        cargo-afl =
          pkgs.runCommand "cargo-afl-wrapped"
            {
              nativeBuildInputs = [ pkgs.makeWrapper ];
              inherit (cargo-afl-unwrapped) version meta;
              pname = "cargo-afl";
            }
            ''
              mkdir -p $out/bin
              makeWrapper ${cargo-afl-unwrapped}/bin/cargo-afl $out/bin/cargo-afl \
                --set-default XDG_DATA_HOME ${aflXdgDataHome} \
                --set-default CARGO ${rust}/bin/cargo \
                --prefix PATH : ${
                  pkgs.lib.makeBinPath [
                    rust
                    pkgs.aflplusplus
                  ]
                }
            '';

        # ── CI matrix ───────────────────────────────────────────────────────
        # Each job builds the workspace offline from the committed Cargo.lock
        # (no network in the sandbox). `nix flake check` runs the FULL matrix
        # (see `checks` below); GitHub CI fans out only over the lean subset
        # via `nix build .#checks.<system>.<name>`.
        cargoVendor = pkgs.rustPlatform.importCargoLock { lockFile = ./Cargo.lock; };

        # The declared MSRV (Cargo.toml `rust-version`). Floor drivers:
        #   * `#[expect(...)]` lint attribute (stable since 1.81)
        #   * the `clap` 4.6 dependency tree uses edition 2024 (needs Cargo 1.85)
        rustMsrv = pkgs.rust-bin.stable."1.85.0".default;

        # Build one check-only derivation: a fixed toolchain, the vendored
        # deps wired up offline by cargoSetupHook, and a single cargo command.
        mkCargoCheck =
          {
            name,
            toolchain ? rustStable,
            phase,
          }:
          pkgs.stdenv.mkDerivation {
            inherit name;
            src = ./.;
            cargoDeps = cargoVendor;
            nativeBuildInputs = [
              toolchain
              pkgs.rustPlatform.cargoSetupHook
              # libdeflate is the default backend: `libdeflate-sys` compiles the
              # vendored libdeflate C source with the `cc` crate, so a C compiler
              # must be on PATH for the default-feature builds (stable/clippy).
              pkgs.stdenv.cc
            ];
            # Check-only: nothing to install, and the default fixup/strip
            # phases have nothing useful to do.
            dontFixup = true;
            buildPhase = ''
              runHook preBuild
              export HOME=$(mktemp -d)
              ${phase}
              runHook postBuild
            '';
            installPhase = "touch $out";
          };

        # Lean subset — fast, needs no external corpus (the committed synth-*
        # fixtures are the required real-decode subset; see golden_hash.rs).
        ciStable = mkCargoCheck {
          name = "rusty-rapidgzip-test-stable";
          phase = "cargo test --workspace --offline";
        };
        ciClippy = mkCargoCheck {
          name = "rusty-rapidgzip-clippy";
          phase = "cargo clippy --workspace --all-targets --offline -- -D warnings";
        };
        # Format only our own crates. `--all` also reaches the vendored,
        # locally-patched third-party `zune-inflate` (it tracks upstream's
        # style, not ours), and rustfmt's `ignore` option is nightly-only, so we
        # name the workspace members explicitly instead.
        ciFmt = mkCargoCheck {
          name = "rusty-rapidgzip-fmt";
          phase = "cargo fmt -p rusty-rapidgzip -p rusty-rapidgzip-bin -- --check";
        };
        # MSRV guarantee is for library/binary consumers, so build (not test):
        # dev-only deps like criterion needn't satisfy 1.85 themselves. Built
        # with `--no-default-features` (the pure-Rust kernel): the MSRV promise
        # covers *our* Rust source, not the third-party C build tooling the
        # optional `libdeflate` feature pulls in (`cc`/`libdeflate-sys` track
        # their own, faster-moving MSRV, which we do not control).
        ciMsrv = mkCargoCheck {
          name = "rusty-rapidgzip-msrv-1.85";
          toolchain = rustMsrv;
          phase = "cargo build --workspace --no-default-features --offline";
        };

        # Full-corpus-only extra (local): re-run the golden-hash test without
        # the large-fixture skip. With only the committed synth fixtures present
        # it just stops skipping nothing; on a machine with the fetched corpus
        # it exercises the big files too.
        ciCorpusFull = mkCargoCheck {
          name = "rusty-rapidgzip-corpus-full";
          phase = ''
            export RAPIDGZIP_FULL_CORPUS=1
            cargo test -p rusty-rapidgzip --test golden_hash --offline
          '';
        };

      in
      rec {
        # ── Shippable binaries ──────────────────────────────────────────────
        # `nix build` → the parallel gzip decoder CLI (`rusty-rapidgzip-rs`).
        # libdeflate (the default backend) is compiled from vendored source by
        # the `libdeflate-sys` crate and statically linked, so the binary has no
        # runtime C-library dependency.
        packages.rusty-rapidgzip = naersk-lib.buildPackage {
          pname = "rusty-rapidgzip";
          root = ./.;
          # A C compiler is required: `libdeflate-sys` builds libdeflate's C
          # source with the `cc` crate.
          nativeBuildInputs = [ pkgs.stdenv.cc ];
          release = true;
          CARGO_PROFILE_RELEASE_debug = "0";
          # The internal micro-benchmark helper isn't a user-facing tool.
          postInstall = "rm -f $out/bin/bench-inflate";
        };
        defaultPackage = packages.rusty-rapidgzip;

        # `nix build .#other-linux` → the same binary, but with its ELF
        # interpreter repointed at the conventional glibc loader so it runs on
        # ordinary (non-NixOS) Linux distributions. libdeflate is statically
        # linked, so only the host's libc / libgcc are needed at runtime. CI
        # proves this by executing it inside a plain Debian container (ci.yml).
        packages.other-linux = naersk-lib.buildPackage {
          pname = "rusty-rapidgzip-other-linux";
          root = ./.;
          nativeBuildInputs = [ pkgs.stdenv.cc ];
          release = true;
          CARGO_PROFILE_RELEASE_debug = "0";
          postInstall = ''
            rm -f $out/bin/bench-inflate
            patchelf $out/bin/rusty-rapidgzip-rs \
              --set-interpreter /lib64/ld-linux-x86-64.so.2
          '';
        };

        # `nix build .#static-linux` → fully static musl binary. Has no ELF
        # interpreter and no dynamic dependencies at all, so it runs unchanged on
        # any Linux distribution (Alpine, old CentOS, Debian, …) regardless of
        # its libc. CI executes this one inside plain Alpine and Debian
        # containers (ci.yml).
        packages.static-linux = naersk-lib-musl.buildPackage {
          pname = "rusty-rapidgzip-static";
          root = ./.;
          CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
          # libdeflate-sys builds libdeflate's C source; for the musl target it
          # must use the musl cross-gcc so the objects link into the static binary.
          CC_x86_64_unknown_linux_musl = "${muslCC}/bin/${muslCC.targetPrefix}cc";
          nativeBuildInputs = [ muslCC ];
          release = true;
          CARGO_PROFILE_RELEASE_debug = "0";
          postInstall = "rm -f $out/bin/bench-inflate";
        };

        # `nix build .#check-windows` — cross-compile-check the workspace for
        # x86_64-pc-windows-gnu (MinGW) from Linux. Catches cfg(windows) / type /
        # API breakage cheaply; real Windows test execution happens on a native
        # GitHub runner (see ci.yml). libdeflate's C source is compiled by the
        # MinGW cross-gcc that `cc` is pointed at below.
        packages.check-windows = naersk-lib-windows.buildPackage {
          src = ./.;
          mode = "check";
          name = "rusty-rapidgzip-windows-check";
          CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
          CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER =
            "${mingw}/bin/x86_64-w64-mingw32-gcc";
          CC_x86_64_pc_windows_gnu = "${mingw}/bin/x86_64-w64-mingw32-gcc";
          nativeBuildInputs = [ mingw pkgs.pkg-config ];
        };

        # `nix run` → the CLI.
        apps.rusty-rapidgzip = utils.lib.mkApp {
          drv = packages.rusty-rapidgzip;
          name = "rusty-rapidgzip-rs";
        };
        defaultApp = apps.rusty-rapidgzip;

        # `nix flake check` runs the FULL local matrix: the lean subset
        # (stable test, clippy -D warnings, rustfmt, MSRV build) plus the
        # heavier full-corpus, Miri and Windows-cross jobs. GitHub CI runs only
        # the lean subset directly via `nix build .#checks.<system>.<name>`
        # (see `.github/workflows/ci.yml`).
        checks = {
          stable = ciStable;
          clippy = ciClippy;
          fmt = ciFmt;
          msrv = ciMsrv;
          corpus-full = ciCorpusFull;
          # Compile-check for Windows (MinGW). Native Windows test execution
          # additionally runs on a windows-latest GitHub runner (ci.yml).
          check-windows = packages.check-windows;

          # `nix build .#checks.<system>.miri` — run the crate's `unsafe` under
          # Miri to catch UB (out-of-bounds, uninit reads, provenance/aliasing
          # violations) in the back-reference copy kernels, via the
          # self-contained `rusty-rapidgzip/tests/miri_edge.rs` (raw-DEFLATE
          # vectors — no `gzip` subprocess and no threads, both of which Miri
          # can't run). Built `--no-default-features` so the pure-Rust kernel is
          # exercised and no C (`libdeflate-sys`/`cc`) is pulled in. Runs under
          # both the default Stacked Borrows and the stricter Tree Borrows
          # aliasing models.
          miri =
            pkgs.stdenv.mkDerivation {
              name = "rusty-rapidgzip-miri";
              src = ./.;
              cargoDeps = miriCargoVendor;
              nativeBuildInputs = [
                rustMiri
                pkgs.rustPlatform.cargoSetupHook
              ];
              # Miri interprets MIR — no host linker / cc needed, and the
              # default check/fixup phases have nothing to do here.
              dontFixup = true;
              buildPhase = ''
                runHook preBuild
                export HOME=$(mktemp -d)
                # Build the std sysroot Miri runs against (offline: rust-src is
                # in the toolchain; cargoSetupHook vendored all crate deps).
                cargo miri setup --offline

                base="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check"

                # Run one cargo-test selection under both aliasing models.
                run_miri() {
                  local label="$1"; shift
                  echo "── Miri [$label]: Stacked Borrows ──"
                  MIRIFLAGS="$base" cargo miri test --offline "$@"
                  echo "── Miri [$label]: Tree Borrows ──"
                  MIRIFLAGS="$base -Zmiri-tree-borrows" cargo miri test --offline "$@"
                }

                run_miri deflate --no-default-features -p rusty-rapidgzip --test miri_edge

                runHook postBuild
              '';
              installPhase = "touch $out";
            };
        };

        # `nix develop`
        devShells.default = pkgs.mkShell {
          COMMIT_HASH = self.rev or (pkgs.lib.removeSuffix "-dirty" self.dirtyRev or "unknown-not-in-git");
          # we only link with mold in our dev environment for build speed. CI can use the old school rust linker
          shellHook = ''
            #export RUSTFLAGS="-C link-arg=-fuse-ld=mold"
            # Set shell for cmake builds
            export CONFIG_SHELL="${pkgs.bash}/bin/bash"
            export SHELL="${pkgs.bash}/bin/bash"
          '';
          # supply the specific rust version
          nativeBuildInputs = [
            bacon
            pkgs.bash
            pkgs.cargo-audit
            pkgs.cargo-bloat
            pkgs.cargo-crev
            pkgs.cargo-deny
            pkgs.cargo-features-manager
            pkgs.cargo-flamegraph
            pkgs.cargo-insta
            pkgs.cargo-license
            pkgs.cargo-llvm-cov
            pkgs.cargo-llvm-lines
            pkgs.cargo-machete
            pkgs.cargo-mutants
            pkgs.cargo-nextest
            pkgs.cargo-outdated
            pkgs.cargo-readme
            pkgs.cargo-shear
            cargo-afl
            #pkgs.cargo-udeps
            pkgs.cargo-vet
            pkgs.cargo-expand
            pkgs.cmake
            pkgs.gcc
            pkgs.gnumake
            pkgs.git
            pkgs.hugo
            pkgs.jq
            pkgs.mold
            pkgs.openssl
            pkgs.pkg-config
            pkgs.samply
            pkgs.which
            pkgs.ripgrep
            #rust.rust-analyzer
            pkgs.shellcheck
            rust
          ];
        };
      }
    );
}
# {
