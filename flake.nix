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
            export RUSTFLAGS="-C link-arg=-fuse-ld=mold"
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
