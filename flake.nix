{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-25.11"; # that's 23.05
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

      in
      rec {
        # `nix flake check` — runs tests and clippy with pinned stable Rust
        checks = {
          test = naersk-lib-ci.buildPackage {
            src = ./.;
            mode = "test";
          };
          clippy = naersk-lib-ci.buildPackage {
            src = ./.;
            mode = "clippy";
          };

          # `nix build .#checks.<system>.miri` — run the remaining `unsafe`
          # blocks in rusty-rapidgzip-deflate under Miri to catch UB
          # (out-of-bounds, uninit reads, provenance/aliasing violations) in
          # the back-reference copy kernels. Runs the self-contained
          # `tests/miri_edge.rs` (no `gzip` subprocess — Miri can't exec) under
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

                target="-p rusty-rapidgzip-deflate --test miri_edge --offline"
                base="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check"

                echo "── Miri: Stacked Borrows ──"
                MIRIFLAGS="$base" cargo miri test $target

                echo "── Miri: Tree Borrows ──"
                MIRIFLAGS="$base -Zmiri-tree-borrows" cargo miri test $target
                runHook postBuild
              '';
              installPhase = "touch $out";
            };
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
