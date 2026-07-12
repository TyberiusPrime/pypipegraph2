import base64
import json
import os
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import PYTHON_DIR, requires_blake3, requires_cloudpickle


def run_shim(spec: dict, timeout=30):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(PYTHON_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [sys.executable, "-m", "ppg3._shim"],
        input=json.dumps(spec).encode("utf-8"),
        capture_output=True,
        timeout=timeout,
        env=env,
    )
    return proc


def test_shim_source_mode_writes_output(tmp_path):
    out_file = tmp_path / "out.txt"
    source = (
        "def run(io):\n"
        "    with open(io.path('result'), 'w') as fh:\n"
        "        fh.write('hello ' + str(io.params['n']))\n"
    )
    spec = {
        "mode": "callback",
        "transport": {"mode": "source", "source": source, "name": "run"},
        "io": {
            "inputs": {},
            "outputs": {"result": str(out_file)},
            "tools": {},
            "log_dir": str(tmp_path),
            "params": {"n": 3},
        },
    }
    proc = run_shim(spec)
    assert proc.returncode == 0, proc.stderr.decode()
    assert out_file.read_text() == "hello 3"


def test_shim_source_mode_closure_violation_fails(tmp_path):
    trapped = "leak"

    def make():
        def run(io):
            return trapped

        return run

    fn = make()
    import inspect
    import textwrap

    src = textwrap.dedent(inspect.getsource(fn))
    spec = {
        "mode": "callback",
        "transport": {"mode": "source", "source": src, "name": "run"},
        "io": {"inputs": {}, "outputs": {}, "tools": {}, "log_dir": str(tmp_path), "params": {}},
    }
    proc = run_shim(spec)
    assert proc.returncode == 1
    assert b"DefinitionError" in proc.stderr or b"not eligible" in proc.stderr


def test_shim_source_file_mode(tmp_path):
    step_file = tmp_path / "step.py"
    step_file.write_text(
        "def normalize(io):\n"
        "    with open(io.path(), 'w') as fh:\n"
        "        fh.write('normalized')\n"
    )
    out_file = tmp_path / "out.txt"
    spec = {
        "mode": "callback",
        "transport": {
            "mode": "source_file",
            "path": str(step_file),
            "qualname": "normalize",
        },
        "io": {
            "inputs": {},
            "outputs": {"result": str(out_file)},
            "tools": {},
            "log_dir": str(tmp_path),
            "params": {},
        },
    }
    proc = run_shim(spec)
    assert proc.returncode == 0, proc.stderr.decode()
    assert out_file.read_text() == "normalized"


@requires_cloudpickle
def test_shim_cloudpickle_mode(tmp_path):
    import cloudpickle

    def run(io):
        with open(io.path(), "w") as fh:
            fh.write("via cloudpickle")

    blob = base64.b64encode(cloudpickle.dumps(run)).decode("ascii")
    out_file = tmp_path / "out.txt"
    spec = {
        "mode": "callback",
        "transport": {"mode": "cloudpickle", "blob": blob},
        "io": {
            "inputs": {},
            "outputs": {"result": str(out_file)},
            "tools": {},
            "log_dir": str(tmp_path),
            "params": {},
        },
    }
    proc = run_shim(spec)
    assert proc.returncode == 0, proc.stderr.decode()
    assert out_file.read_text() == "via cloudpickle"


def test_shim_data_job_pickles_return_value(tmp_path):
    out_pickle = tmp_path / "data.pickle"
    source = "def train(io):\n    return {'a': 1, 'b': [1, 2, 3]}\n"
    spec = {
        "mode": "callback",
        "transport": {"mode": "source", "source": source, "name": "train"},
        "io": {
            "inputs": {},
            "outputs": {"data.pickle": str(out_pickle)},
            "tools": {},
            "log_dir": str(tmp_path),
            "params": {},
        },
        "pickle_output": True,
    }
    proc = run_shim(spec)
    assert proc.returncode == 0, proc.stderr.decode()
    with open(out_pickle, "rb") as fh:
        obj = pickle.load(fh)
    assert obj == {"a": 1, "b": [1, 2, 3]}


def test_shim_callback_exception_exits_nonzero(tmp_path):
    source = "def run(io):\n    raise ValueError('boom')\n"
    spec = {
        "mode": "callback",
        "transport": {"mode": "source", "source": source, "name": "run"},
        "io": {"inputs": {}, "outputs": {}, "tools": {}, "log_dir": str(tmp_path), "params": {}},
    }
    proc = run_shim(spec)
    assert proc.returncode == 1
    assert b"ValueError" in proc.stderr
    assert b"boom" in proc.stderr


@requires_blake3
def test_shim_fetch_mode_success(tmp_path):
    src_file = tmp_path / "source_data.bin"
    src_file.write_bytes(b"hello ppg3 fetch")
    out_file = tmp_path / "fetched.bin"

    import blake3 as blake3_mod

    digest = blake3_mod.blake3(src_file.read_bytes()).hexdigest()

    spec = {
        "mode": "fetch",
        "fetch": {
            "url": src_file.resolve().as_uri(),
            "blake3": digest,
            "output_path": str(out_file),
        },
    }
    proc = run_shim(spec)
    assert proc.returncode == 0, proc.stderr.decode()
    assert out_file.read_bytes() == b"hello ppg3 fetch"
    assert digest.encode() in proc.stdout


@requires_blake3
def test_shim_fetch_mode_hash_mismatch_fails(tmp_path):
    src_file = tmp_path / "source_data.bin"
    src_file.write_bytes(b"hello ppg3 fetch")
    out_file = tmp_path / "fetched.bin"

    spec = {
        "mode": "fetch",
        "fetch": {
            "url": src_file.resolve().as_uri(),
            "blake3": "0" * 64,
            "output_path": str(out_file),
        },
    }
    proc = run_shim(spec)
    assert proc.returncode == 1
    assert b"mismatch" in proc.stderr
