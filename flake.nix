{
  description = "Wraps mbf-bam into an mach-nix importable builder";

  inputs = {
    import-cargo.url = github:edolstra/import-cargo;

    nixpkgs.url = "github:NixOS/nixpkgs/24.05";
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
    rust = pkgs.rust-bin.stable."1.78.0".default.override {
      targets = ["x86_64-unknown-linux-gnu" "x86_64-unknown-linux-musl"];
      extensions = ["llvm-tools-preview"];
    };
    pkgs_with_rust =
      pkgs
      // {
        cargo = rust;
        rustc = rust;
        cargo-binutils = pkgs.cargo-binutils.override {
          cargo = rust;
          rustc = rust;
        };
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
          pkgs.maturin
        ];
        requirementsExtra = ''
          maturin
        '';
        requirements = ''
          pytest
          pytest-cov
           pytest-mock
           loguru
           rich
           xxhash
           wrapt
           deepdiff
           psutil
           networkx
           cython
           filelock
           pyzstd
           watchfiles
           lib_programname

        '';
        format = "pyproject";
      };
    palettable = let
      p = pkgs.python311Packages;
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
      p = pkgs.python311Packages;
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
      p = pkgs.python311Packages;
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
      p = pkgs.python311Packages;
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
    dppd = let
      p = pkgs.python311Packages;
    in
      p.buildPythonPackage rec {
        pname = "dppd";
        version = "0.23";
        buildInputs = [mizani];
        propagatedBuildInputs = [p.pandas p.wrapt p.natsort];
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-vze6u5gF50mvyro08BL4kFBQU83NB8ETmJF9DM9a2fM=";
        };
        doCheck = false;
      };
    dppd_plotnine = let
      p = pkgs.python311Packages;
    in
      p.buildPythonPackage rec {
        pname = "dppd_plotnine";
        version = "0.2.5";
        buildInputs = [plotnine];
        propagatedBuildInputs = [p.pandas plotnine dppd];
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-agUD7FK4oFO5VbOQZh31nJ8lefJlzjmZ71u2/hL46lo=";
        };
        doCheck = false;
      };
    pyzstd = let
      p = pkgs.python311Packages;
    in
      p.buildPythonPackage rec {
        pname = "pyzstd";
        version = "0.16.0";
        buildInputs = [];
        propagatedBuildInputs = [];
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-/UOgrjiuFSI/sQV3KQAYKcMzbpD0rPBM8S697DM0Zlg=";
        };
        doCheck = false;
      };
    lib-detect-testenv = let
      p = pkgs.python311Packages;
    in
      p.buildPythonPackage rec {
        pname = "lib-detect-testenv";
        version = "2.0.8";
        buildInputs = [p.setuptools p.setuptools-scm];
        propagatedBuildInputs = [];
        format = "pyproject";
        src = p.fetchPypi {
          inherit version;
          pname = "lib_detect_testenv";
          sha256 = "sha256-llJ7MRRyfnDoD2ccIEoiWuaqrxF5g/j6T1blQrI2jUM=";
        };
        doCheck = false;
      };

    cli-exit-tools = let
      p = pkgs.python311Packages;
    in
      p.buildPythonPackage rec {
        pname = "cli-exit-tools";
        version = "1.2.7";
        buildInputs = [p.setuptools p.setuptools-scm];
        propagatedBuildInputs = [p.click lib-detect-testenv];
        format = "pyproject";
        src = p.fetchPypi {
          inherit version;
          pname = "cli_exit_tools";
          sha256 = "sha256-51JCekqp2x8YNwyNwR6+9uJFzFiR7C+nnnFpvlg8JCM=";
        };
        doCheck = false;
      };

    lib_programname = let
      p = pkgs.python311Packages;
    in
      p.buildPythonPackage rec {
        pname = "lib_programname";
        version = "2.0.9";
        buildInputs = [p.setuptools p.setuptools-scm];
        propagatedBuildInputs = [p.click cli-exit-tools lib-detect-testenv];
        format = "pyproject";
        src = p.fetchPypi {
          inherit pname version;
          sha256 = "sha256-3eAMcs9bea7fl4HVoO5DGCVeFMD1T+9NNL2xFVzEwrw=";
        };
        doCheck = false;
      };

    mypython = pkgs.python311.withPackages (p: [
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
      p.setproctitle
      p.setuptools
      dppd
      dppd_plotnine
      # for testing...
      ppg1
      plotnine
      p.filelock
      pyzstd
      lib_programname
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
        pkgs.cargo-binutils

        pkgs.rust-analyzer
        pkgs.git
        pkgs.cargo-udeps
        pkgs.cargo-audit
        bacon
        mypython
        pkgs.maturin
        pkgs.py-spy
        pkgs.file
        pkgs.pre-commit
        pkgs.black
      ];
    };
  };
}
