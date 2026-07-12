import time

import pytest

from ppg3.statcache import StatCache

from conftest import requires_blake3


@requires_blake3
def test_hash_cached_until_content_changes(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("hello")
    cache = StatCache(tmp_path / "statcache.sqlite")

    h1 = cache.hash_file(f)
    h2 = cache.hash_file(f)
    assert h1 == h2

    # Change content; mtime_ns/size should differ enough to invalidate.
    time.sleep(0.01)
    f.write_text("hello world, much longer now")
    h3 = cache.hash_file(f)
    assert h3 != h1


@requires_blake3
def test_hash_survives_reopen(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("content")
    db = tmp_path / "statcache.sqlite"

    cache1 = StatCache(db)
    h1 = cache1.hash_file(f)
    cache1.close()

    cache2 = StatCache(db)
    # Row should already be present without re-hashing (can't directly
    # observe "no rehash" without instrumentation, but the value must
    # match).
    h2 = cache2.hash_file(f)
    assert h1 == h2


@requires_blake3
def test_different_files_different_hashes(tmp_path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("aaa")
    b.write_text("bbb")
    cache = StatCache(tmp_path / "statcache.sqlite")
    assert cache.hash_file(a) != cache.hash_file(b)
