from pathlib import Path
from hashlib import md5
from .parallel import CoreLock


def hash_file(path: Path, core_lock: CoreLock, known_st_size: None):
    hasher = md5()
    if known_st_size is None:
        known_st_size = path.stat().st_size
    # todo: fork. Or at least push to C I suppose.!
    # needs some benchmarking though.
    with open(path, "rb") as op:
        block = op.read(10 * 1024 * 1024)
        while block:
            hasher.update(block)
            block = op.read(10 * 1024 * 1024)

    return {"md5": hasher.hexdigest()}
