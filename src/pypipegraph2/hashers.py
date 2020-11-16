from pathlib import Path
from hashlib import md5


def hash_file(path: Path):
    hasher = md5()
    with open(path, "rb") as op:
        block = op.read(10 * 1024 * 1024)
        while block:
            hasher.update(block)
            block = op.read(10 * 1024 * 1024)
    return {"md5": hasher.hexdigest()}
