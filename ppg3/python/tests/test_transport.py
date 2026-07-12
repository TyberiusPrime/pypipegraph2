import base64

import pytest

from ppg3.localscope import DefinitionError
from ppg3.tools import PyEnv
from ppg3.transport import Source, select_transport

from conftest import requires_blake3, requires_cloudpickle


def top_level_fn(io):
    return 1


@requires_cloudpickle
@requires_blake3
def test_same_env_uses_cloudpickle():
    env = PyEnv.current()
    spec = select_transport(top_level_fn, env, paranoid=False)
    assert spec["transport"]["mode"] == "cloudpickle"
    import cloudpickle

    blob = base64.b64decode(spec["transport"]["blob"])
    fn = cloudpickle.loads(blob)
    assert fn(None) == 1


@requires_blake3
def test_paranoid_forces_source_mode():
    env = PyEnv.current()
    spec = select_transport(top_level_fn, env, paranoid=True)
    assert spec["transport"]["mode"] == "source"
    assert spec["transport"]["name"] == "top_level_fn"


@requires_blake3
def test_cross_env_pyenv_uses_source_mode():
    nix_env = PyEnv.__new__(PyEnv)
    nix_env.kind = "nix"
    nix_env.preload = []
    nix_env.flake_ref = "github:owner/repo/" + "a" * 40 + "#python3"
    nix_env.weakly_hermetic = False
    spec = select_transport(top_level_fn, nix_env, paranoid=False)
    assert spec["transport"]["mode"] == "source"


@requires_blake3
def test_source_mode_roundtrip_via_exec():
    env = PyEnv.current()
    spec = select_transport(top_level_fn, env, paranoid=True)
    ns = {"__name__": "test_module"}
    exec(compile(spec["transport"]["source"], "<test>", "exec"), ns)
    fn = ns[spec["transport"]["name"]]
    assert fn(None) == 1


def closure_over_local():
    trapped = "state"

    def f(io):
        return trapped

    return f


@requires_blake3
def test_closure_rejected_at_definition_time():
    env = PyEnv.current()
    fn = closure_over_local()
    with pytest.raises(DefinitionError):
        select_transport(fn, env, paranoid=True)


@requires_blake3
def test_source_form_recipe_and_transport(tmp_path):
    f = tmp_path / "step.py"
    f.write_text("def normalize(io):\n    return 1\n")
    src = Source(f"{f}::normalize")
    spec = select_transport(src, PyEnv.current())
    assert spec["transport"]["mode"] == "source_file"
    assert spec["transport"]["qualname"] == "normalize"
    assert spec["transport"]["path"] == str(f)
    assert isinstance(spec["recipe"], str)


def test_source_ref_requires_double_colon():
    with pytest.raises(DefinitionError):
        Source("no_separator_here")
