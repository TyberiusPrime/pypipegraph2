"""Parlallel computation / threading support helpers"""

from threading import Lock, Condition
import ctypes


class _CoreLockContextManager:
    def __init__(self, core_lock, cores):
        self.core_lock = core_lock
        self.cores = cores

    def __enter__(self):
        self.core_lock._acquire(self.cores)

    def __exit__(self, exc_type, exc_value, traceback):
        self.core_lock._release(self.cores)


class FakeContextManager:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class FakeLock:
    def __init__(self):
        self.cm = FakeContextManager()

    def using(self, _cores):
        return self.cm


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
        self.terminated = False

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
                if self.remaining >= count:
                    self.remaining -= count
                    # logger.info(f"{_thread.get_ident()}, had available")
                    return

            # not enough remaining. try again once somebody releases something
            with self.condition:
                self.condition.wait()
            # logger.info(f"{_thread.get_ident()} condition triggered")

    def _release(self, count):
        # logger.info(f"{_thread.get_ident()} release({count}) called")
        if count == 0:  # pragma: no cover
            raise ValueError("Count == 0")
        with self.lock:
            self.remaining += count
            if self.remaining > self.max_cores:  # pragma: no cover
                raise ValueError("Remaining exceeded max_cores")

        # logger.info(f"{_thread.get_ident()} remaning: {self.remaining}")
        with self.condition:
            self.condition.notify_all()


def async_raise(target_tid, exception):
    """Raises an asynchronous exception in another thread.
    Read http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc
    for further enlightenments.
    :param target_tid: target thread identifier
    :param exception: Exception class to be raised in that thread
    """
    # Ensuring and releasing GIL are useless since we're not in C
    # gil_state = ctypes.pythonapi.PyGILState_Ensure()
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(target_tid), ctypes.py_object(exception)
    )
    # ctypes.pythonapi.PyGILState_Release(gil_state)
    if ret == 0:
        raise ValueError("Invalid thread ID {}".format(target_tid))
    elif ret > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), None)
        raise SystemError("PyThreadState_SetAsyncExc failed")
