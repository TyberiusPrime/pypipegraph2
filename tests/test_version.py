import pypipegraph2 as ppg


def test_version_is_correct():

    from pathlib import Path

    raw = (Path(__file__).parent.parent / "Cargo.toml").read_text()
    q = f'version = "{ppg.__version__}"'
    print('searching for', repr(q))
    assert q in raw
