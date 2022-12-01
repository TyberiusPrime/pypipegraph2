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
    pkgs_with_rust =
      pkgs
      // {
        cargo = rust;
        rustc = rust;
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
        pname = "pypipegraph2";
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
          maturin
        '';
        format = "pyproject";
      };
    palettable = let
      p = pkgs.python39Packages;
    in
      p.buildPythonPackage rec {
        pname = "palettable";
        version = "3.3.0";
        # buildInputs = [p.pandas];
        propagatedBuildInputs = [p.numpy];
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-cv7Kcc99eYMM1tkYGwLt8ie4Z9UDvslTz5+pG/RIlr0=";
        };
      };

    mizani = let
      p = pkgs.python39Packages;
    in
      p.buildPythonPackage rec {
        pname = "mizani";
        version = "0.8.1";
        # buildInputs = [p.pandas];
        propagatedBuildInputs = [p.numpy palettable p.pandas p.matplotlib p.scipy];
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-itCg76UvG830H2dbZKjA980k52PVO6ztZhPyC9btSSg=";
        };
        patchPhase = ''
          sed -i '3 a version=${version}' setup.cfg
        '';

        doCheck = false;
      };
    plotnine = let
      p = pkgs.python39Packages;
    in
      p.buildPythonPackage rec {
        pname = "plotnine";
        version = "0.10.1";
        buildInputs = [mizani];
        propagatedBuildInputs = [p.pandas p.statsmodels mizani];
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-2RKgS2ONz4IsUaZ4i4VmQjI0jVFfFR2zpkwAAZZvaEE=";
        };
        doCheck = false;
      };
    ppg1 = let
      p = pkgs.python39Packages;
    in
      p.buildPythonPackage rec {
        pname = "pypipegraph";
        version = "0.197";
        buildInputs = [p.pandas];
        propagatedBuildInputs = [p.pandas];
        patchPhase = ''
          substituteInPlace setup.cfg --replace "extras = True" ""
        '';
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-x7UiibWXcCsLwJNKcYWMXHsqt3pCIbv5NoQdx6iD+2o=";
        };
      };
    mypython = pkgs.python39.withPackages (p: [
      #todo: figure out how to derive this from pyproject.toml
      p.pytest
      p.pytest-mock
      p.loguru
      p.rich
      p.xxhash
      p.wrapt
      p.deepdiff
      p.psutil
      p.networkx
      p.cython
      # for testing...
      ppg1
      plotnine
    ]);
  in {
    # pass in nixpkgs, mach-nix and what you want it to report back as a version
    mach-nix-build-python-package = build_mbf_bam;
    devShell.x86_64-linux = pkgs.mkShell {
      # supplx the specific rust version
      # be sure to set this back in your build scripts,
      # otherwise pyo3 will get recompiled all the time
      CARGO_TARGET_DIR = "target_rust_analyzer";
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