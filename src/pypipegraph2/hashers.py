from pathlib import Path
from hashlib import md5
from .parallel import CoreLock
from xxhash import xxh3_128


def hash_file(path: Path):
    # I profiled a bunch of hash functions
    # and xx3 and spooky were the fastest 128bit hashers
    # (we want 128 bit to prevent collisions).
    # single core, spooky seemed a bit faster
    # but the xxhash implementation releases the gil
    # when passed more than 100kb (otherwise it's a
    # faster *not* to acquire the lock!)
    hasher = xxh3_128()
    #if known_st_size is None:
        #known_st_size = path.stat().st_size
    # we are not acquiring the core lock here.
    # why? because this is essentially always
    # limited by the read-bandwidth, not the
    # cpu.
    # (even on a very fast Samsung EVO equivalent SSD
    # (about 3gb/s), doing it from memory is 4 times faster)
    # so we shouldn't be stalling everything else much
    # (except for memory bandwith. oh well, at least
    # it shouldn't go into swap with the tiny buffer we use here)
    with open(path, "rb") as op:
        block = op.read(1024*512)
        while block:
            hasher.update(block)
            block = op.read(1024*512)

    return {"xx": hasher.hexdigest()}
