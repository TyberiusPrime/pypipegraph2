from pathlib import Path
from .exceptions import JobContractError
from xxhash import xxh3_128


def hash_file(path: Path):
    """delegate to a fast and somewhat collision resistant hash function.
    Or reuse a .sha256 file if present and at least as new as the file.
    (Having one present that's older than the file is a JobContractError).
    The file must contain exactly one 64 character hash (+- a newline)
    """
    stat = path.stat()
    sha256_path = path.with_name(path.name + ".sha256")
    try:
        sha256_stat = sha256_path.stat()
        if sha256_stat.st_mtime < stat.st_mtime:
            raise JobContractError(
                f"Found {sha256_path} but it's older than {path}. Have the generating code set correct mtimes, or not produce the file"
            )
        the_hash = sha256_path.read_text().strip()
        if len(the_hash) != 64:
            raise JobContractError(f"Expected a 64 character hash in {sha256_path}, but got {the_hash} with len {len(the_hash)}")
        if not all([x in "0123456789abcdefABCDEF" for x in the_hash]):
            raise JobContractError(f"Expected a 64 character hash in {sha256_path}, but got {the_hash} with invalid characters (not 0-9a-fA-F)")
    except FileNotFoundError:

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
        the_hash = hasher.hexdigest()

    return {
        "hash": the_hash,
        "mtime": int(stat.st_mtime),
        "size": stat.st_size,
    }


def hash_bytes(input: bytes):
    hasher = xxh3_128()
    hasher.update(input)
    return hasher.hexdigest()


def hash_str(input: str):
    return hash_bytes(input.encode("utf-8"))
