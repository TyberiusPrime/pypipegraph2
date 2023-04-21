import pytest
import threading
from pypipegraph2.parallel import CoreLock


class TestCoreLock:
    def test_single_thread(self):
        mylock = CoreLock(1)
        with pytest.raises(ValueError):
            mylock._acquire(2)
        mylock._acquire(1)
        mylock._release(1)

    def test_multithreading_simple(self):
        mylock = CoreLock(1)
        counter = []
        threads = []

        def inner(c):
            mylock._acquire(1)
            counter.append(c)
            mylock._release(1)

        for i in range(5):
            t = threading.Thread(target=inner, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        assert len(counter) == 5
        assert set(counter) == set([0, 1, 2, 3, 4])

    def test_multithreading_two_by_two(self):
        mylock = CoreLock(3)
        counter = []
        threads = []

        def inner(c):
            with mylock.using(2):
                counter.append(c)

        for i in range(5):
            t = threading.Thread(target=inner, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        assert len(counter) == 5
        assert set(counter) == set([0, 1, 2, 3, 4])

    def test_multithreading_complex(self):
        mylock = CoreLock(8)
        counter = []
        threads = []

        def inner(c):
            with mylock.using(c % 8 + 1):
                counter.append(c)

        for i in range(8 * 4 + 1):
            t = threading.Thread(target=inner, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        assert len(counter) == 33
        assert set(counter) == set(range(33))

    def test_multithreading_dieing(self):
        mylock = CoreLock(1)
        counter = []
        threads = []

        def inner(c):
            with mylock.using(1):
                if c == 1:
                    raise ValueError(
                        "This should show up in the pytest warnings summary"
                    )
                counter.append(c)

        for i in range(5):
            t = threading.Thread(target=inner, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        assert len(counter) == 4
        assert set(counter) == set([0, 2, 3, 4])

    def test_aquire_0_raise(self):
        mylock = CoreLock(1)
        with pytest.raises(ValueError):
            mylock._acquire(0)
        with pytest.raises(ValueError):
            with mylock.using(0):
                pass
