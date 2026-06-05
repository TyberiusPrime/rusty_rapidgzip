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
        # exposed as `packages.test.*` via `nix build .#test.<name>`.
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
        ciFmt = mkCargoCheck {
          name = "rusty-rapidgzip-fmt";
          phase = "cargo fmt --all -- --check";
        };
        # MSRV guarantee is for library/binary consumers, so build (not test):
        # dev-only deps like criterion needn't satisfy 1.80 themselves.
        ciMsrv = mkCargoCheck {
          name = "rusty-rapidgzip-msrv-1.85";
          toolchain = rustMsrv;
          phase = "cargo build --workspace --offline";
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
        # `nix flake check` runs the FULL local matrix: the lean subset
        # (stable test, clippy -D warnings, rustfmt, MSRV build) plus the
        # heavier full-corpus and Miri jobs. GitHub CI runs only the lean
        # subset (see `packages.test` below and `.github/workflows/ci.yml`).
        checks = {
          stable = ciStable;
          clippy = ciClippy;
          fmt = ciFmt;
          msrv = ciMsrv;
          corpus-full = ciCorpusFull;

          # `nix build .#checks.<system>.miri` — run the crates' `unsafe` under
          # Miri to catch UB (out-of-bounds, uninit reads, provenance/aliasing
          # violations). Two targets, sharing one std-sysroot build:
          #   * rusty-rapidgzip-deflate: the back-reference copy kernels, via the
          #     self-contained `tests/miri_edge.rs` (no `gzip` subprocess — Miri
          #     can't exec).
          # Each runs under the default Stacked Borrows and the stricter Tree
          # Borrows aliasing models.
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

                run_miri deflate   -p rusty-rapidgzip-deflate --test miri_edge

                runHook postBuild
              '';
              installPhase = "touch $out";
            };
        };

        # Lean CI subset under a dedicated `test` output (not `checks`, so
        # `nix flake check` isn't forced to treat the parent as a derivation,
        # and not `packages`, which flake-check validates leaf-by-leaf).
        # GitHub CI runs `nix build .#test.<system>.<name>`; locally the same
        # jobs are reachable via `nix build .#checks.<system>.<name>`.
        test = {
          stable = ciStable;
          clippy = ciClippy;
          fmt = ciFmt;
          msrv = ciMsrv;
        };

        # `nix develop`
        devShell = pkgs.mkShell {
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
