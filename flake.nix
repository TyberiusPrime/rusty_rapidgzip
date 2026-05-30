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
