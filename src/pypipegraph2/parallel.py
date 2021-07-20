"""Parlallel computation / threading support helpers"""
from threading import Lock, Condition
import ctypes
from .util import log_info


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
        self.terminated = False

    def using(self, cores):
        return _CoreLockContextManager(self, cores)

    def terminate(self):
        log_info("Terminating core lock")
        self.terminated = True
        self.condition.acquire() # wake them all up.
        self.condition.notify_all()
        self.condition.release()
        log_info("Terminating core lock2")


    def _acquire(self, count):
        # logger.info(f" {_thread.get_ident()} - acquire({count}) called")
        if count > self.max_cores:
            raise ValueError(f"Count {count} > max_cores {self.max_cores}")
        if count == 0:
            with open("/home/finkernagel/shu.dat",'w') as op:
                op.write("yes")
            raise ValueError("Count == 0")
        while True:
            locked = False
            try:
                locked = self.lock.acquire(timeout=10)
                if not locked:
                    continue
                # logger.info(f"{_thread.get_ident()} lock acquired, - remaining: {self.remaining}")
                if self.terminated:
                    log_info("inner core lock termination")
                    raise KeyboardInterrupt('Terminated lock')
                elif self.remaining >= count:
                    self.remaining -= count
                    # logger.info(f"{_thread.get_ident()}, had available")
                    return
                else:
                    pass
            finally:
                self.lock.release()
            self.condition.acquire()
            try:
                self.condition.wait(10)
            except RuntimeError:
                if self.terminated:
                    log_info("inner core lock termination2")
                    raise KeyboardInterrupt('Terminated lock')

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
