import pypipegraph2 as ppg


def test_version_is_correct():
    import configparser
    from pathlib import Path

    c = configparser.ConfigParser()
    c.read(Path(__file__).parent.parent / "setup.cfg")
    version = c["metadata"]["version"]
    assert version == ppg.__version__
