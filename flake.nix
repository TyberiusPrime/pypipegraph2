{
  description = "Wraps mbf-bam into an mach-nix importable builder";

  inputs = {
    import-cargo.url = github:edolstra/import-cargo;

    nixpkgs.url = "github:NixOS/nixpkgs/22.05"; # that's 21.05
    naersk.url = "github:nmattia/naersk";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    import-cargo,
    nixpkgs,
    naersk,
    rust-overlay,
  }: let
    inherit (import-cargo.builders) importCargo;
    system = "x86_64-linux";
    overlays = [(import rust-overlay)];
    pkgs = import nixpkgs {inherit system overlays;};
    rust = pkgs.rust-bin.stable."1.65.0".default.override {
      targets = ["x86_64-unknown-linux-gnu" "x86_64-unknown-linux-musl"];
    };

    naersk-lib = naersk.lib."${system}".override {
      cargo = rust;
      rustc = rust;
    };

    bacon = naersk-lib.buildPackage {
      # could also pull a slightly older one from nixpkgs
      pname = "bacon";
      version = "2.2.5";
      src = pkgs.fetchFromGitHub {
        owner = "Canop";
        repo = "bacon";
        rev = "4ed1206119d67cc9d538ae711ec04df1577369d1";
        sha256 = "sha256-KoAaECfZ8DwGN/U1HCp/4NUvTvFYiN+li3I5gNYM/oU=";
      };
    };
  in let
    build_mbf_bam = pkgs: pythonpkgs: outside_version: let
      cargo_in = importCargo {
        lockFile = ./Cargo.lock;
        inherit pkgs;
      };
    in
      pythonpkgs.buildPythonPackage
      {
        src = ./.;
        version = outside_version;

        nativeBuildInputs = [
          cargo_in.cargoHome

          # Build-time dependencies
          pkgs.rustc
          pkgs.cargo
          pkgs.openssl.dev
          pkgs.perl
        ];
        requirementsExtra = ''
          setuptools-rust
        '';
      };
    mypython = pkgs.python39.withPackages (p: [p.setuptools-rust p.pytest]);
  in {
    # pass in nixpkgs, mach-nix and what you want it to report back as a version
    mach-nix-build-python-package = build_mbf_bam;
    devShell.x86_64-linux = pkgs.mkShell {
      # supplx the specific rust version
      # be sure to set this back in your build scripts,
      # otherwise pyo3 will get recompiled all the time
      CARGO_TARGET_DIR="target_rust_analyzer";
      nativeBuildInputs = [
        rust
        pkgs.rust-analyzer
        pkgs.git
        pkgs.cargo-udeps
        pkgs.cargo-audit
        bacon
        mypython
        pkgs.maturin
      ];
    };
  };
}
