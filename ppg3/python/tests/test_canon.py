import dataclasses
import enum
import json
import struct

import pytest

from ppg3 import canon

from conftest import GOLDEN_DIR, requires_blake3, requires_golden


def test_none_bool_int_str():
    assert canon.canonicalize_value(None) is None
    assert canon.canonicalize_value(True) is True
    assert canon.canonicalize_value(False) is False
    assert canon.canonicalize_value(42) == 42
    assert canon.canonicalize_value("hello") == "hello"


def test_float_encoding():
    v = canon.canonicalize_value(1.5)
    assert v == {canon.FLOAT_KEY: struct.pack("<d", 1.5).hex()}
    # round trip via decanonicalize
    assert canon.decanonicalize_value(v) == 1.5


def test_bytes_encoding():
    v = canon.canonicalize_value(b"\x00\x01\xff")
    assert v == {canon.BYTES_KEY: "0001ff"}
    assert canon.decanonicalize_value(v) == b"\x00\x01\xff"


def test_list_tuple():
    assert canon.canonicalize_value([1, 2, 3]) == [1, 2, 3]
    assert canon.canonicalize_value((1, 2, 3)) == [1, 2, 3]


def test_dict_requires_str_keys():
    with pytest.raises(TypeError) as exc:
        canon.canonicalize_value({1: "a"}, path="$.params")
    assert "$.params" in str(exc.value)


def test_dict_key_sorting_in_encoding():
    data = canon.canonicalize_value({"b": 1, "a": 2, "c": 3})
    encoded = canon.canonical_json_bytes(data)
    assert encoded == b'{"a":2,"b":1,"c":3}'


def test_set_sorted_by_canonical_encoding():
    v = canon.canonicalize_value({3, 1, 2})
    assert v[canon.SET_KEY] == [1, 2, 3]
    fv = canon.canonicalize_value(frozenset({"b", "a"}))
    assert fv[canon.SET_KEY] == ["a", "b"]


def test_set_of_mixed_types_sorted_stably():
    v = canon.canonicalize_value({"z", "a", "m"})
    assert v[canon.SET_KEY] == sorted(["z", "a", "m"])


class Color(enum.Enum):
    RED = 1
    BLUE = 2


def test_enum_encoding():
    v = canon.canonicalize_value(Color.RED)
    expected_name = f"{Color.__module__}.{Color.__qualname__}.RED"
    assert v == {canon.ENUM_KEY: expected_name}


def test_enum_decanonicalize_roundtrip():
    v = canon.canonicalize_value(Color.BLUE)
    assert canon.decanonicalize_value(v) is Color.BLUE


@dataclasses.dataclass
class Point:
    x: int
    y: int


def test_dataclass_encoding():
    v = canon.canonicalize_value(Point(1, 2))
    expected_name = f"{Point.__module__}.{Point.__qualname__}"
    assert v == {
        canon.DATACLASS_KEY: expected_name,
        "fields": {"x": 1, "y": 2},
    }


def test_rejects_arbitrary_object():
    class Whatever:
        pass

    with pytest.raises(TypeError) as exc:
        canon.canonicalize_value(Whatever(), path="$.params.model.weights")
    assert "$.params.model.weights" in str(exc.value)


def test_ppg3_hash_hook():
    class Custom:
        def __ppg3_hash__(self):
            return {"kind": "custom", "value": 42}

    v = canon.canonicalize_value(Custom())
    assert v == {"kind": "custom", "value": 42}


def test_nested_path_reporting():
    with pytest.raises(TypeError) as exc:
        canon.canonicalize_value({"a": {"b": [object()]}}, path="$")
    assert "$.a.b[0]" in str(exc.value)


def test_canonical_json_bytes_unicode():
    data = canon.canonicalize_value({"name": "héllo wörld é"})
    encoded = canon.canonical_json_bytes(data)
    assert "héllo wörld".encode("utf-8") in encoded
    # ensure_ascii=False: no escaped \u sequences for these chars
    assert b"\\u" not in encoded


def test_canonical_json_bytes_no_whitespace_sorted_keys():
    data = canon.canonicalize_value({"z": 1, "a": 2})
    encoded = canon.canonical_json_bytes(data)
    assert b" " not in encoded
    assert encoded == b'{"a":2,"z":1}'


def test_canonical_json_bytes_rejects_raw_float():
    with pytest.raises(AssertionError):
        canon.canonical_json_bytes({"x": 1.5})


@requires_blake3
def test_input_key_local_deterministic():
    doc = {"a": 1, "b": [1, 2, 3]}
    k1 = canon.input_key_local(doc)
    k2 = canon.input_key_local(doc)
    assert k1 == k2
    assert len(k1) == 64
    int(k1, 16)  # hex


@requires_blake3
def test_input_key_local_sensitive_to_content():
    k1 = canon.input_key_local({"a": 1})
    k2 = canon.input_key_local({"a": 2})
    assert k1 != k2


# --- Golden fixtures (ppg3/tests/golden/) -----------------------------


def _load_fixtures(pattern):
    return sorted(GOLDEN_DIR.glob(pattern))


@requires_golden
@pytest.mark.parametrize("path", _load_fixtures("keydoc_[0-9]*.json") if GOLDEN_DIR.is_dir() else [])
def test_golden_keydoc_canonical(path):
    fixture = json.loads(path.read_text())
    doc = fixture["doc"]
    encoded = canon.canonical_json_bytes(doc)
    assert encoded.decode("utf-8") == fixture["canonical"]


@requires_golden
@requires_blake3
@pytest.mark.parametrize("path", _load_fixtures("keydoc_[0-9]*.json") if GOLDEN_DIR.is_dir() else [])
def test_golden_keydoc_ik(path):
    fixture = json.loads(path.read_text())
    doc = fixture["doc"]
    ik = canon.input_key_local(doc)
    assert ik == fixture["ik"]


@requires_golden
@pytest.mark.parametrize(
    "path", _load_fixtures("keydoc_reject_*.json") if GOLDEN_DIR.is_dir() else []
)
def test_golden_keydoc_reject(path):
    fixture = json.loads(path.read_text())
    assert fixture.get("error") is True
    doc = fixture["doc"]
    with pytest.raises((AssertionError, TypeError, ValueError)):
        canon.canonical_json_bytes(doc)
