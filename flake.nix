{
  description = "Wraps mbf-bam into an mach-nix importable builder";

  inputs = {
    import-cargo.url = "github:edolstra/import-cargo";

    nixpkgs.url = "github:NixOS/nixpkgs/26.05";
    naersk.url = "github:nmattia/naersk";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      import-cargo,
      nixpkgs,
      naersk,
      rust-overlay,
    }:
    let
      inherit (import-cargo.builders) importCargo;
      system = "x86_64-linux";
      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs { inherit system overlays; };
      rust = pkgs.rust-bin.stable."1.93.1".default.override {
        targets = [
          "x86_64-unknown-linux-gnu"
          "x86_64-unknown-linux-musl"
        ];
        extensions = [ "llvm-tools-preview" ];
      };
      pkgs_with_rust = pkgs // {
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

    in
    let
      build_mbf_bam =
        pkgs: pythonpkgs: outside_version:
        let
          cargo_in = importCargo {
            lockFile = ./Cargo.lock;
            inherit pkgs;
          };
        in
        pythonpkgs.buildPythonPackage {
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
             setuptools
             filelock
             pyzstd
             watchfiles
             lib_programname

          '';
          format = "pyproject";
        };
      palettable =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "palettable";
          version = "3.3.0";
          # buildInputs = [p.pandas];
          propagatedBuildInputs = [ p.numpy ];
          src = p.fetchPypi {
            inherit pname version;
            sha256 = "sha256-cv7Kcc99eYMM1tkYGwLt8ie4Z9UDvslTz5+pG/RIlr0=";
          };
        };

      # mizani = let
      #   p = pkgs.python314Packages;
      # in
      # p.buildPythonPackage rec {
      #   pname = "mizani";
      #   version = "0.8.1";
      #   # buildInputs = [p.pandas];
      #   propagatedBuildInputs = [p.numpy palettable p.pandas p.matplotlib p.scipy];
      #   src = p.fetchPypi {
      #     inherit pname version;
      #     sha256 = "sha256-itCg76UvG830H2dbZKjA980k52PVO6ztZhPyC9btSSg=";
      #   };
      #   patchPhase = ''
      #     sed -i '3 a version=${version}' setup.cfg
      #   '';
      #
      #   doCheck = false;
      # };
      #

      ppg1 =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "pypipegraph";
          version = "0.197";
          #format = "setuptools";
          pyproject = true;
          build-system = [ p.setuptools ];
          buildInputs = [ p.pandas ];
          propagatedBuildInputs = [ p.pandas ];
          patchPhase = ''
            substituteInPlace setup.cfg --replace "extras = True" ""
          '';
          src = p.fetchPypi {
            inherit pname version;
            sha256 = "sha256-x7UiibWXcCsLwJNKcYWMXHsqt3pCIbv5NoQdx6iD+2o=";
          };
        };
      dppd =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "dppd";
          version = "0.31";

          pyproject = true;
          build-system = [ p.setuptools ];
          #buildInputs = [mizani];
          propagatedBuildInputs = [
            p.pandas
            p.wrapt
            p.natsort
          ];
          src = p.fetchPypi {
            inherit pname version;
            sha256 = "sha256-/TRRtP5pbBjMPzwVYE405oT02HpYntTRc0rH/Zl6iwU=";
          };
          doCheck = false;
        };
      dppd_plotnine =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "dppd_plotnine";
          version = "0.2.9";

          pyproject = true;
          build-system = [ p.setuptools ];

          buildInputs = [ p.plotnine ];
          propagatedBuildInputs = [
            p.pandas
            p.plotnine
            dppd
          ];
          src = p.fetchPypi {
            inherit pname version;
            sha256 = "sha256-Uo/eBzxLfLe4Kv/oZKwChwnBLZM7VY/1GGEzZqmFeU0=";
          };
          doCheck = false;
        };
      lib-detect-testenv =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "lib-detect-testenv";
          version = "2.0.8";
          buildInputs = [
            p.setuptools
            p.setuptools-scm
          ];
          propagatedBuildInputs = [ ];
          format = "pyproject";
          src = p.fetchPypi {
            inherit version;
            pname = "lib_detect_testenv";
            sha256 = "sha256-llJ7MRRyfnDoD2ccIEoiWuaqrxF5g/j6T1blQrI2jUM=";
          };
          doCheck = false;
        };

      cli-exit-tools =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "cli-exit-tools";
          version = "1.2.7";
          buildInputs = [
            p.setuptools
            p.setuptools-scm
          ];
          propagatedBuildInputs = [
            p.click
            lib-detect-testenv
          ];
          format = "pyproject";
          src = p.fetchPypi {
            inherit version;
            pname = "cli_exit_tools";
            sha256 = "sha256-51JCekqp2x8YNwyNwR6+9uJFzFiR7C+nnnFpvlg8JCM=";
          };
          doCheck = false;
        };

      lib_programname =
        let
          p = pkgs.python314Packages;
        in
        p.buildPythonPackage rec {
          pname = "lib_programname";
          version = "2.0.9";
          buildInputs = [
            p.setuptools
            p.setuptools-scm
          ];
          propagatedBuildInputs = [
            p.click
            cli-exit-tools
            lib-detect-testenv
          ];
          format = "pyproject";
          src = p.fetchPypi {
            inherit pname version;
            sha256 = "sha256-3eAMcs9bea7fl4HVoO5DGCVeFMD1T+9NNL2xFVzEwrw=";
          };
          doCheck = false;
        };
      # localscope = let
      #      p = pkgs.python314Packages;
      #    in
      #      p.buildPythonPackage rec {
      #        pname = "localscope";
      #        version = "0.2.5";
      #        buildInputs = [p.setuptools];
      #        propagatedBuildInputs = [];
      #        format = "pyproject";
      #        src = p.fetchPypi {
      #          inherit pname version;
      #          sha256 = "sha256-rmjx77cegkvXjM02e7Mly8hGR8LOUfKejEoKFXNmYzo=";
      #        };
      #        doCheck = false;
      #      };

      mypython = pkgs.python314.withPackages (p: [
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
        p.filelock
        p.pyzstd
        lib_programname
        #localscope
      ]);

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
    {
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
          pkgs.bacon
          mypython
          cargo-afl
          pkgs.maturin
          pkgs.py-spy
          pkgs.file
          pkgs.pre-commit
          pkgs.black
        ];
      };
    };
}
