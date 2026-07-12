{
  description = "pypipegraph2 dev shell and test matrix";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/26.05";
    naersk.url = "github:nmattia/naersk";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

    # Used to build the Python test/dev environments straight from uv.lock,
    # preferring prebuilt PyPI wheels. This sidesteps nixpkgs' own
    # pythonXXXPackages sets, whose from-source builds can lag badly behind
    # freshly-released interpreters (e.g. nixpkgs 26.05's python315Packages
    # has a broken pydantic-core: PyO3 doesn't support the 3.15 C API yet).
    # PyPI already has working cp315 wheels for everything we need.
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      naersk,
      rust-overlay,
      pyproject-nix,
      uv2nix,
      pyproject-build-systems,
    }:
    let
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

      # uv.lock-driven workspace: resolves the exact same package set `uv
      # sync --group test --locked` would, but installs prebuilt PyPI
      # wheels via nix instead of nixpkgs' own (often stale-for-new-
      # interpreters) pythonXXXPackages builds.
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      pyprojectOverlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
        # Only resolve what the `test` group needs (skips `dev`-only tools
        # like black/pre-commit, which we don't need for running pytest).
        dependencies = {
          pypipegraph2 = [ "test" ];
        };
      };

      mkPythonSet =
        python:
        (pkgs.callPackage pyproject-nix.build.packages { inherit python; }).overrideScope (
          pkgs.lib.composeManyExtensions [
            # `.default` (not `.wheel`): a few deps (e.g. dppd, dppd-plotnine)
            # only publish an sdist on PyPI, so we need real build-system
            # (setuptools etc.) support, not just wheel-install hooks.
            pyproject-build-systems.overlays.default
            pyprojectOverlay
          ]
        );

      # Third-party deps only (pyproject.toml's `test` dependency-group +
      # main `dependencies`), keyed by their uv.lock package names. The
      # local `pypipegraph2` package is deliberately NOT listed here: its
      # Rust extension is built once via naersk below (see `pypipegraph2-so`)
      # and injected via PYTHONPATH, so uv2nix never needs to invoke
      # maturin. `textual` is also omitted: it's only imported lazily
      # inside cli.py and no test touches it.
      testDepsSpec = {
        "dppd-plotnine" = [ ];
        pytest = [ ];
        "pytest-cov" = [ ];
        "pytest-mock" = [ ];
        pandas = [ ];
        cython = [ ];
        setuptools = [ ];
        pypipegraph = [ ];
        flake8 = [ ];
        deepdiff = [ ];
        filelock = [ ];
        "lib-programname" = [ ];
        loguru = [ ];
        networkx = [ ];
        psutil = [ ];
        pyzstd = [ ];
        rich = [ ];
        wrapt = [ ];
        xxhash = [ ];
      };

      # Python versions covered by the (former) GitHub Actions test matrix.
      # 3.15 is deliberately excluded: it's still beta and PyPI doesn't have
      # cp315 wheels yet for matplotlib (pulled in transitively via
      # dppd-plotnine -> plotnine -> mizani), which would require hand-
      # porting nixpkgs' native matplotlib buildInputs (freetype/libpng/
      # qhull/meson-python) to build from sdist. Re-add once upstream
      # publishes cp315 wheels.
      pythonTestVersions = [
        "312"
        "313"
        "314"
      ];

      mkTestVenv =
        ver: (mkPythonSet pkgs.${"python" + ver}).mkVirtualEnv "pytest-python${ver}-env" testDepsSpec;

      mypython = mkTestVenv "313";

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

      # The compiled extension is built against pyo3's stable ABI
      # (abi3-py38, see Cargo.toml), so one build serves every Python
      # version in the test matrix below.
      pypipegraph2-so = naersk-lib.buildPackage {
        pname = "pypipegraph2";
        version = "3.4.3";
        src = ./.;
        copyLibs = true;
        copyBins = false;
        cargoBuildOptions = x: x ++ [ "--lib" ];
      };

      # tests/run and tests/__pycache__ are gitignored scratch/output dirs
      # from local test runs; keep them out of the derivation input so
      # rebuilds stay reproducible.
      testsSrc = pkgs.lib.cleanSourceWith {
        src = ./tests;
        filter =
          path: _type:
          let
            base = baseNameOf path;
          in
          base != "run" && base != "__pycache__";
      };

      mkPytestCheck =
        ver:
        let
          venv = mkTestVenv ver;
        in
        pkgs.runCommand "pytest-python${ver}" {
          nativeBuildInputs = [
            venv
            pkgs.procps # tests/test_external_jobs.py shells out to `ps`
          ];
        } ''
          mkdir -p work
          cp -r ${./python} work/python
          cp -r ${testsSrc} work/tests
          cp ${./pyproject.toml} work/pyproject.toml
          cp ${./Cargo.toml} work/Cargo.toml # tests/test_version.py reads the crate version from here
          chmod -R u+w work
          cp ${pypipegraph2-so}/lib/libpypipegraph2.so work/python/pypipegraph2/pypipegraph2.abi3.so
          cd work
          export HOME=$TMPDIR
          pytest tests
          touch $out
        '';

      pytestChecks = builtins.listToAttrs (
        map (ver: {
          name = "pytest-python${ver}";
          value = mkPytestCheck ver;
        }) pythonTestVersions
      );

    in
    {
      checks.x86_64-linux = pytestChecks;
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
          pkgs.cargo-mutants
          pkgs.maturin
          pkgs.py-spy
          pkgs.file
          pkgs.pre-commit
          pkgs.black
        ];
      };
    };
}
