from pathlib import Path
from xxhash import xxh3_128


def hash_file(path: Path):
    """delegate to a fast and somewhat collision resistant hash function"""
    # I profiled a bunch of hash functions
    # and xx3 and spooky were the fastest 128bit hashers
    # (we want 128 bit to prevent collisions).
    # single core, spooky seemed a bit faster
    # but the xxhash implementation releases the gil
    # when passed more than 100kb (otherwise it's a
    # faster *not* to acquire the lock!)
    hasher = xxh3_128()
    # if known_st_size is None:
    # known_st_size = path.stat().st_size
    # we are not acquiring the core lock here.
    # why? because this is essentially always
    # limited by the read-bandwidth, not the
    # cpu.
    # (even on a very fast Samsung EVO equivalent SSD
    # (about 3gb/s), doing it from memory is 4 times faster)
    # so we shouldn't be stalling everything else much
    # (except for memory bandwidth. oh well, at least
    # it should not go into swap with the tiny buffer we use here)
    with open(path, "rb") as op:
        block = op.read(1024 * 512)
        while block:
            hasher.update(block)
            block = op.read(1024 * 512)
    stat = path.stat()

    return {
        "hash": hasher.hexdigest(),
        "mtime": int(stat.st_mtime),
        "size": stat.st_size,
    }

def hash_bytes(input: bytes):
    hasher = xxh3_128()
    hasher.update(input)
    return hasher.hexdigest()

def hash_str(input: str):
    return hash_bytes(input.encode('utf-8'))
