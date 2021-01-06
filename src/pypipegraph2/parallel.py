from threading import Lock, Condition
import threading

# from loguru import logger


class _CoreLockContextManager:
    def __init__(self, core_lock, cores):
        self.core_lock = core_lock
        self.cores = cores

    def __enter__(self):
        self.core_lock._acquire(self.cores)

    def __exit__(self, exc_type, exc_value, traceback):
        self.core_lock._release(self.cores)


class CoreLock:
    """Allow threads to request n 'cores',
    if they're available, let it proceed.
    If they're not available, block.
    If they exceed the maxmimum number available: raise.

    Essentially, this is a Semaphore with multi-resource-one-step-acquisition.

    """

    def __init__(self, max_cores):
        self.max_cores = int(max_cores)
        self.remaining = max_cores
        self.lock = Lock()
        self.condition = Condition()

    def using(self, cores):
        return _CoreLockContextManager(self, cores)

    def _acquire(self, count):
        # logger.info(f" {_thread.get_ident()} - acquire({count}) called")
        if count > self.max_cores:
            raise ValueError(f"Count {count} > max_cores {self.max_cores}")
        if count == 0:
            raise ValueError("Count == 0")
        while True:
            with self.lock:
                # logger.info(f"{_thread.get_ident()} lock acquired, - remaining: {self.remaining}")
                if self.remaining >= count:
                    self.remaining -= count
                    # logger.info(f"{_thread.get_ident()}, had available")
                    return
                else:
                    pass
            self.condition.acquire()
            self.condition.wait()
            self.condition.release()
            # logger.info(f"{_thread.get_ident()} condition triggered")

    def _release(self, count):
        # logger.info(f"{_thread.get_ident()} release({count}) called")
        if count == 0:  # pragma: no cover
            raise ValueError("Count == 0")
        with self.lock:
            # logger.info(f"{_thread.get_ident()} lock aquired in release")
            self.remaining += count
            if self.remaining > self.max_cores:  # pragma: no cover
                raise ValueError("Remaining exceeded max_cores")
        # logger.info(f"{_thread.get_ident()} remaning: {self.remaining}")
        with self.condition:
            # logger.info(f"{_thread.get_ident()} notify condition")
            self.condition.acquire()
            self.condition.notify_all()
            self.condition.release()
            # logger.info(f"{_thread.get_ident()} done notify condition")
