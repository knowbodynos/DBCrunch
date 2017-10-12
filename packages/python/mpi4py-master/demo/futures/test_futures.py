import os
import sys
import time
import functools
import unittest

from mpi4py import MPI
from mpi4py import futures
try:
    from concurrent.futures._base import (
        PENDING, RUNNING, CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED)
except ImportError:
    from mpi4py.futures._base import (
        PENDING, RUNNING, CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED)


def create_future(state=PENDING, exception=None, result=None):
    f = futures.Future()
    f._state = state
    f._exception = exception
    f._result = result
    return f


PENDING_FUTURE = create_future(state=PENDING)
RUNNING_FUTURE = create_future(state=RUNNING)
CANCELLED_FUTURE = create_future(state=CANCELLED)
CANCELLED_AND_NOTIFIED_FUTURE = create_future(state=CANCELLED_AND_NOTIFIED)
EXCEPTION_FUTURE = create_future(state=FINISHED, exception=OSError())
SUCCESSFUL_FUTURE = create_future(state=FINISHED, result=42)


def mul(x, y):
    return x * y


def sleep_and_raise(t):
    time.sleep(t)
    raise Exception('this is an exception')


class ExecutorMixin:
    worker_count = 2

    def setUp(self):
        self.t1 = time.time()
        try:
            self.executor = self.executor_type(max_workers=self.worker_count)
        except NotImplementedError:
            e = sys.exc_info()[1]
            self.skipTest(str(e))
        self._prime_executor()

    def tearDown(self):
        self.executor.shutdown(wait=True)
        dt = time.time() - self.t1
        self.assertLess(dt, 60, 'synchronization issue: test lasted too long')

    def _prime_executor(self):
        # Make sure that the executor is ready to do work before running the
        # tests. This should reduce the probability of timeouts in the tests.
        futures = [self.executor.submit(time.sleep, 0)
                   for _ in range(self.worker_count)]
        for f in futures:
            f.result()


class ProcessPoolMixin(ExecutorMixin):
    executor_type = futures.MPIPoolExecutor

    if 'coverage' in sys.modules:
        executor_type = functools.partial(
            executor_type,
            python_args='-m coverage run'.split(),
            )


class ProcessPoolInitTest(ProcessPoolMixin,
                          unittest.TestCase):

    def _prime_executor(self):
        pass

    def test_init(self):
        self.executor_type()

    def test_init_args(self):
        self.executor_type(1)

    def test_init_kwargs(self):
        executor = self.executor_type(
            python_exe=sys.executable,
            max_workers=None,
            mpi_info=dict(soft="0:1"),
            main=False,
            path=[],
            wdir=os.getcwd(),
            env={},
            )
        futures = [executor.submit(time.sleep, 0)
                   for _ in range(self.worker_count)]
        for f in futures:
            f.result()
        executor.shutdown()

    def test_max_workers_environ(self):
        save = os.environ.get('MPI4PY_MAX_WORKERS')
        os.environ['MPI4PY_MAX_WORKERS'] = '1'
        try:
            executor = self.executor_type()
            executor.submit(time.sleep, 0).result()
            executor.shutdown()
        finally:
            del os.environ['MPI4PY_MAX_WORKERS']
            if save is not None:
                os.environ['MPI4PY_MAX_WORKERS'] = save

    def test_max_workers_negative(self):
        for number in (0, -1):
            self.assertRaises(ValueError,
                              self.executor_type,
                              max_workers=number)


class ProcessPoolBootupTest(ProcessPoolMixin,
                            unittest.TestCase):

    def _prime_executor(self):
        pass

    def test_bootup(self):
        executor = self.executor_type(1)
        executor.bootup()
        executor.bootup()
        executor.shutdown()
        self.assertRaises(RuntimeError, executor.bootup)

    def test_bootup_wait(self):
        executor = self.executor_type(1)
        executor.bootup(wait=True)
        executor.bootup(wait=True)
        executor.shutdown(wait=True)
        self.assertRaises(RuntimeError, executor.bootup, True)

    def test_bootup_nowait(self):
        executor = self.executor_type(1)
        executor.bootup(wait=False)
        executor.bootup(wait=False)
        executor.shutdown(wait=False)
        self.assertRaises(RuntimeError, executor.bootup, False)
        executor.shutdown(wait=True)

    def test_bootup_nowait_wait(self):
        executor = self.executor_type(1)
        executor.bootup(wait=False)
        executor.bootup(wait=True)
        executor.shutdown()
        self.assertRaises(RuntimeError, executor.bootup)

    def test_bootup_shutdown_nowait(self):
        executor = self.executor_type(1)
        executor.bootup(wait=False)
        executor.shutdown(wait=False)
        worker = executor._pool
        del executor
        worker.join()


class ExecutorShutdownTestMixin:

    def test_run_after_shutdown(self):
        self.executor.shutdown()
        self.assertRaises(RuntimeError,
                          self.executor.submit,
                          pow, 2, 5)

    def test_hang_issue12364(self):
        fs = [self.executor.submit(time.sleep, 0.01) for _ in range(50)]
        self.executor.shutdown()
        for f in fs:
            f.result()


class ProcessPoolShutdownTest(ProcessPoolMixin,
                              ExecutorShutdownTestMixin,
                              unittest.TestCase):

    def _prime_executor(self):
        pass

    def test_shutdown(self):
        executor = self.executor_type(max_workers=1)
        self.assertEqual(executor._pool, None)
        self.assertEqual(executor._shutdown, False)
        executor.submit(mul, 21, 2)
        executor.submit(mul, 6, 7)
        executor.submit(mul, 3, 14)
        self.assertNotEqual(executor._pool.thread, None)
        self.assertEqual(executor._shutdown, False)
        executor.shutdown(wait=False)
        self.assertNotEqual(executor._pool.thread, None)
        self.assertEqual(executor._shutdown, True)
        executor.shutdown(wait=True)
        self.assertEqual(executor._pool, None)
        self.assertEqual(executor._shutdown, True)

    def test_init_bootup_shutdown(self):
        executor = self.executor_type(max_workers=1)
        self.assertEqual(executor._pool, None)
        self.assertEqual(executor._shutdown, False)
        executor.bootup()
        self.assertTrue(executor._pool.event.is_set())
        self.assertEqual(executor._shutdown, False)
        executor.shutdown()
        self.assertEqual(executor._pool, None)
        self.assertEqual(executor._shutdown, True)

    def test_context_manager_shutdown(self):
        with self.executor_type(max_workers=1) as e:
            self.assertEqual(list(e.map(abs, range(-5, 5))),
                             [5, 4, 3, 2, 1, 0, 1, 2, 3, 4])
            threads = [e._pool.thread]
            queues = [e._pool.queue]
            events = [e._pool.event]

        for t in threads:
            t.join()
        for q in queues:
            self.assertRaises(LookupError, q.pop)
        for e in events:
            self.assertTrue(e.is_set())

    def test_del_shutdown(self):
        executor = self.executor_type(max_workers=1)
        list(executor.map(abs, range(-5, 5)))
        threads = [executor._pool.thread]
        queues = [executor._pool.queue]
        events = [executor._pool.event]
        if hasattr(sys, 'pypy_version_info'):
            executor.shutdown(False)
        else:
            del executor

        for t in threads:
            t.join()
        for q in queues:
            self.assertRaises(LookupError, q.pop)
        for e in events:
            self.assertTrue(e.is_set())


class WaitTestMixin:

    def test_first_completed(self):
        future1 = self.executor.submit(mul, 21, 2)
        future2 = self.executor.submit(time.sleep, 0.2)

        done, not_done = futures.wait(
                [CANCELLED_FUTURE, future1, future2],
                return_when=futures.FIRST_COMPLETED)

        self.assertEqual(set([future1]), done)
        self.assertEqual(set([CANCELLED_FUTURE, future2]), not_done)

    def test_first_completed_some_already_completed(self):
        future1 = self.executor.submit(time.sleep, 0.2)

        finished, pending = futures.wait(
                 [CANCELLED_AND_NOTIFIED_FUTURE, SUCCESSFUL_FUTURE, future1],
                 return_when=futures.FIRST_COMPLETED)

        self.assertEqual(
                set([CANCELLED_AND_NOTIFIED_FUTURE, SUCCESSFUL_FUTURE]),
                finished)
        self.assertEqual(set([future1]), pending)

    def test_first_exception(self):
        future1 = self.executor.submit(mul, 2, 21)
        future2 = self.executor.submit(sleep_and_raise, 0.1)
        future3 = self.executor.submit(time.sleep, 0.2)

        finished, pending = futures.wait(
                [future1, future2, future3],
                return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([future1, future2]), finished)
        self.assertEqual(set([future3]), pending)

    def test_first_exception_some_already_complete(self):
        future1 = self.executor.submit(divmod, 21, 0)
        future2 = self.executor.submit(time.sleep, 0.2)

        finished, pending = futures.wait(
                [SUCCESSFUL_FUTURE,
                 CANCELLED_FUTURE,
                 CANCELLED_AND_NOTIFIED_FUTURE,
                 future1, future2],
                return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([SUCCESSFUL_FUTURE,
                              CANCELLED_AND_NOTIFIED_FUTURE,
                              future1]), finished)
        self.assertEqual(set([CANCELLED_FUTURE, future2]), pending)

    def test_first_exception_one_already_failed(self):
        future1 = self.executor.submit(time.sleep, 0.2)

        finished, pending = futures.wait(
                 [EXCEPTION_FUTURE, future1],
                 return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([EXCEPTION_FUTURE]), finished)
        self.assertEqual(set([future1]), pending)

    def test_all_completed(self):
        future1 = self.executor.submit(divmod, 2, 0)
        future2 = self.executor.submit(mul, 2, 21)

        finished, pending = futures.wait(
                [SUCCESSFUL_FUTURE,
                 CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 future1,
                 future2],
                return_when=futures.ALL_COMPLETED)

        self.assertEqual(set([SUCCESSFUL_FUTURE,
                              CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              future1,
                              future2]), finished)
        self.assertEqual(set(), pending)

    def test_timeout(self):
        future1 = self.executor.submit(mul, 6, 7)
        future2 = self.executor.submit(time.sleep, 0.2)

        finished, pending = futures.wait(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2],
                timeout=0.1,
                return_when=futures.ALL_COMPLETED)

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE,
                              future1]), finished)
        self.assertEqual(set([future2]), pending)


class ProcessPoolWaitTest(ProcessPoolMixin,
                          WaitTestMixin,
                          unittest.TestCase):
    pass


class AsCompletedTestMixin:

    def test_no_timeout(self):
        future1 = self.executor.submit(mul, 2, 21)
        future2 = self.executor.submit(mul, 7, 6)

        completed = set(futures.as_completed(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2]))
        self.assertEqual(set(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2]),
                completed)

    def test_zero_timeout(self):
        future1 = self.executor.submit(time.sleep, 0.2)
        completed_futures = set()
        try:
            for future in futures.as_completed(
                    [CANCELLED_AND_NOTIFIED_FUTURE,
                     EXCEPTION_FUTURE,
                     SUCCESSFUL_FUTURE,
                     future1],
                    timeout=0):
                completed_futures.add(future)
        except futures.TimeoutError:
            pass

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE]),
                         completed_futures)

    def test_nonzero_timeout(self):
        future1 = self.executor.submit(time.sleep, 0.0)
        future2 = self.executor.submit(time.sleep, 0.2)
        completed_futures = set()
        try:
            for future in futures.as_completed(
                    [CANCELLED_AND_NOTIFIED_FUTURE,
                     EXCEPTION_FUTURE,
                     SUCCESSFUL_FUTURE,
                     future1],
                    timeout=0.1):
                completed_futures.add(future)
        except futures.TimeoutError:
            pass

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE,
                              future1]),
                         completed_futures)

    def test_duplicate_futures(self):
        py_version = sys.version_info[:3]
        if py_version[0] == 3 and py_version < (3, 3, 5): return
        # Issue 20367. Duplicate futures should not raise exceptions or give
        # duplicate responses.
        future1 = self.executor.submit(time.sleep, 0.1)
        completed = [f for f in futures.as_completed([future1, future1])]
        self.assertEqual(len(completed), 1)


class ProcessPoolAsCompletedTest(ProcessPoolMixin,
                                 AsCompletedTestMixin,
                                 unittest.TestCase):
    pass


class ExecutorTestMixin:

    def test_submit(self):
        future = self.executor.submit(pow, 2, 8)
        self.assertEqual(256, future.result())

    def test_submit_keyword(self):
        future = self.executor.submit(mul, 2, y=8)
        self.assertEqual(16, future.result())
        future = self.executor.submit(mul, x=2, y=8)
        self.assertEqual(16, future.result())

    def test_submit_cancel(self):
        future1 = self.executor.submit(time.sleep, 0.25)
        future2 = self.executor.submit(time.sleep, 0)
        future2.cancel()
        self.assertEqual(None,  future1.result())
        self.assertEqual(False, future1.cancelled())
        self.assertEqual(True,  future2.cancelled())

    def test_map(self):
        self.assertEqual(
                list(self.executor.map(pow, range(10), range(10))),
                list(map(pow, range(10), range(10))))

    def test_map_exception(self):
        i = self.executor.map(divmod, [1, 1, 1, 1], [2, 3, 0, 5])
        self.assertEqual(next(i), (0, 1))
        self.assertEqual(next(i), (0, 1))
        self.assertRaises(ZeroDivisionError, next, i)

    def test_map_timeout(self):
        results = []
        try:
            for i in self.executor.map(time.sleep,
                                       [0, 0, 1],
                                       timeout=0.25):
                results.append(i)
        except futures.TimeoutError:
            pass
        else:
            self.fail('expected TimeoutError')

        self.assertEqual([None, None], results)

    def test_map_timeout_one(self):
        results = []
        for i in self.executor.map(time.sleep, [0, 0, 0], timeout=1):
            results.append(i)
        self.assertEqual([None, None, None], results)


class ProcessPoolExecutorTest(ProcessPoolMixin,
                              ExecutorTestMixin,
                              unittest.TestCase):

    def test_map_chunksize(self):
        ref = list(map(pow, range(40), range(40)))
        self.assertEqual(
            list(self.executor.map(pow, range(40), range(40), chunksize=6)),
            ref)
        self.assertEqual(
            list(self.executor.map(pow, range(40), range(40), chunksize=50)),
            ref)
        self.assertEqual(
            list(self.executor.map(pow, range(40), range(40), chunksize=40)),
            ref)

        def bad():
            list(self.executor.map(pow, range(40), range(40), chunksize=-1))
        self.assertRaises(ValueError, bad)

    def test_map_unordered(self):
        map_unordered = functools.partial(self.executor.map, unordered=True)
        self.assertEqual(
                set(map_unordered(pow, range(10), range(10))),
                set(map(pow, range(10), range(10))))

    def test_map_unordered_timeout(self):
        map_unordered = functools.partial(self.executor.map, unordered=True)
        num_workers = self.executor._num_workers
        results = []
        try:
            args = [0.2] + [0]*(num_workers-1)
            for i in map_unordered(time.sleep, args, timeout=0.1):
                results.append(i)
        except futures.TimeoutError:
            pass
        else:
            self.fail('expected TimeoutError')

        self.assertEqual([None]*(num_workers-1), results)

    def test_map_unordered_timeout_one(self):
        map_unordered = functools.partial(self.executor.map, unordered=True)
        results = []
        for i in map_unordered(time.sleep, [0, 0, 0], timeout=1):
            results.append(i)
        self.assertEqual([None, None, None], results)

    def test_map_unordered_exception(self):
        map_unordered = functools.partial(self.executor.map, unordered=True)
        i = map_unordered(divmod, [1, 1, 1, 1], [2, 3, 0, 5])
        try:
            self.assertEqual(next(i), (0, 1))
        except ZeroDivisionError:
            return

    def test_map_unordered_chunksize(self):
        map_unordered = functools.partial(self.executor.map, unordered=True)
        ref = set(map(pow, range(40), range(40)))
        self.assertEqual(
            set(map_unordered(pow, range(40), range(40), chunksize=6)),
            ref)
        self.assertEqual(
            set(map_unordered(pow, range(40), range(40), chunksize=50)),
            ref)
        self.assertEqual(
            set(map_unordered(pow, range(40), range(40), chunksize=40)),
            ref)

        def bad():
            set(map_unordered(pow, range(40), range(40), chunksize=-1))
        self.assertRaises(ValueError, bad)


class ProcessPoolSubmitTest(unittest.TestCase):

    def test_multiple_executors(self):
        executor1 = futures.MPIPoolExecutor(1).bootup(wait=True)
        executor2 = futures.MPIPoolExecutor(1).bootup(wait=True)
        executor3 = futures.MPIPoolExecutor(1).bootup(wait=True)
        fs1 = [executor1.submit(abs, i) for i in range(100, 200)]
        fs2 = [executor2.submit(abs, i) for i in range(200, 300)]
        fs3 = [executor3.submit(abs, i) for i in range(300, 400)]
        futures.wait(fs3+fs2+fs1)
        for i, f in enumerate(fs1):
            self.assertEqual(f.result(), i + 100)
        for i, f in enumerate(fs2):
            self.assertEqual(f.result(), i + 200)
        for i, f in enumerate(fs3):
            self.assertEqual(f.result(), i + 300)
        executor1 = executor2 = executor3 = None

    def test_mpi_serialized_support(self):
        futures._worker.setup_mpi_threads()
        threading = futures._worker.threading
        serialized = futures._worker.serialized
        lock_save = serialized.lock
        try:
            if lock_save is None:
                serialized.lock = threading.Lock()
                executor = futures.MPIPoolExecutor(1).bootup()
                executor.submit(abs, 0).result()
                executor.shutdown()
                serialized.lock = lock_save
            else:
                serialized.lock = None
                with lock_save:
                    executor = futures.MPIPoolExecutor(1).bootup()
                    executor.submit(abs, 0).result()
                    executor.shutdown()
                serialized.lock = lock_save
        finally:
            serialized.lock = lock_save

    def orig_test_mpi_serialized_support(self):
        threading = futures._worker.threading
        serialized = futures._worker.serialized
        lock_save = serialized.lock
        try:
            serialized.lock = threading.Lock()
            executor = futures.MPIPoolExecutor(1).bootup()
            executor.submit(abs, 0).result()
            if lock_save is not None:
                serialized.lock = None
                with lock_save:
                    executor.submit(abs, 0).result()
            serialized.lock = lock_save
            executor.submit(abs, 0).result()
            executor.shutdown()
            if lock_save is not None:
                serialized.lock = None
                with lock_save:
                    executor = futures.MPIPoolExecutor(1).bootup()
                    executor.submit(abs, 0).result()
                    executor.shutdown()
                serialized.lock = lock_save
        finally:
            serialized.lock = lock_save

    if futures._worker.SharedPool:

        def test_shared_executors(self):
            executors = [futures.MPIPoolExecutor() for _ in range(16)]
            fs = []
            for i in range(128):
                fs.extend(e.submit(abs, i*16+j)
                          for j, e in enumerate(executors))
            assert sorted(f.result() for f in fs) == list(range(16*128))
            world_size = MPI.COMM_WORLD.Get_size()
            num_workers = max(1, world_size - 1)
            for e in executors:
                self.assertEqual(e._num_workers, num_workers)
            del e, executors


def inout(arg):
    return arg


class GoodPickle(object):

    def __init__(self, value=0):
        self.value = value
        self.pickled = False
        self.unpickled = False

    def __getstate__(self):
        self.pickled = True
        return (self.value,)

    def __setstate__(self, state):
        self.unpickled = True
        self.value = state[0]


class BadPickle(object):

    def __init__(self):
        self.pickled = False

    def __getstate__(self):
        self.pickled = True
        1/0

    def __setstate__(self, state):
        pass


class BadUnpickle(object):

    def __init__(self):
        self.pickled = False

    def __getstate__(self):
        self.pickled = True
        return (None,)

    def __setstate__(self, state):
        if state[0] is not None:
            raise ValueError
        1/0


class ProcessPoolPickleTest(unittest.TestCase):

    def setUp(self):
        self.executor = futures.MPIPoolExecutor(1)

    def tearDown(self):
        self.executor.shutdown()

    def test_good_pickle(self):
        o = GoodPickle(42)
        r = self.executor.submit(inout, o).result()
        self.assertEqual(o.value, r.value)
        self.assertTrue(o.pickled)
        self.assertTrue(r.unpickled)

        r = self.executor.submit(GoodPickle, 77).result()
        self.assertEqual(r.value, 77)
        self.assertTrue(r.unpickled)

    def test_bad_pickle(self):
        o = BadPickle()
        self.assertFalse(o.pickled)
        f = self.executor.submit(inout, o)
        self.assertRaises(ZeroDivisionError, f.result)
        self.assertTrue(o.pickled)

        f = self.executor.submit(BadPickle)
        self.assertRaises(ZeroDivisionError, f.result)

        f = self.executor.submit(abs, 42)
        self.assertEqual(f.result(), 42)

    def test_bad_unpickle(self):
        executor = futures.MPIPoolExecutor(1).bootup(wait=True)

        o = BadUnpickle()
        self.assertFalse(o.pickled)
        f = executor.submit(inout, o)
        self.assertRaises(ZeroDivisionError, f.result)
        self.assertTrue(o.pickled)

        f = self.executor.submit(BadUnpickle)
        self.assertRaises(ZeroDivisionError, f.result)

        f = self.executor.submit(abs, 42)
        self.assertEqual(f.result(), 42)


class MPICommExecutorTest(unittest.TestCase):

    MPICommExecutor = futures.MPICommExecutor

    def test_default(self):
        with self.MPICommExecutor() as executor:
            if executor is not None:
                executor.bootup()
                future1 = executor.submit(time.sleep, 0)
                future2 = executor.submit(time.sleep, 0)
                executor.shutdown()
                self.assertEqual(None, future1.result())
                self.assertEqual(None, future2.result())

    def test_self(self):
        with self.MPICommExecutor(MPI.COMM_SELF) as executor:
            future = executor.submit(time.sleep, 0)
            self.assertEqual(None, future.result())
            self.assertEqual(None, future.exception())

            future = executor.submit(sleep_and_raise, 0)
            self.assertRaises(Exception, future.result)
            self.assertEqual(Exception, type(future.exception()))

            list(executor.map(time.sleep, [0, 0]))
            list(executor.map(time.sleep, [0, 0], timeout=1))
            iterator = executor.map(time.sleep, [0.01, 0], timeout=0)
            self.assertRaises(futures.TimeoutError, list, iterator)

    def test_args(self):
        with self.MPICommExecutor(MPI.COMM_SELF) as executor:
            self.assertTrue(executor is not None)
        with self.MPICommExecutor(MPI.COMM_SELF, 0) as executor:
            self.assertTrue(executor is not None)

    def test_kwargs(self):
        with self.MPICommExecutor(comm=MPI.COMM_SELF) as executor:
            self.assertTrue(executor is not None)
        with self.MPICommExecutor(comm=MPI.COMM_SELF, root=0) as executor:
            self.assertTrue(executor is not None)

    def test_arg_root(self):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        for root in range(comm.Get_size()):
            with self.MPICommExecutor(comm, root) as executor:
                if rank != root:
                    self.assertTrue(executor is None)
            with self.MPICommExecutor(root=root) as executor:
                if rank != root:
                    self.assertTrue(executor is None)

    def test_arg_root_bad(self):
        size = MPI.COMM_WORLD.Get_size()
        self.assertRaises(ValueError, self.MPICommExecutor, root=-size)
        self.assertRaises(ValueError, self.MPICommExecutor, root=-1)
        self.assertRaises(ValueError, self.MPICommExecutor, root=+size)

    def test_arg_comm_bad(self):
        if MPI.COMM_WORLD.Get_size() == 1:
            return
        intercomm = futures._worker.split(MPI.COMM_WORLD)
        try:
            self.assertRaises(ValueError, self.MPICommExecutor, intercomm)
        finally:
            intercomm.Free()

    def test_with_bad(self):
        mpicommexecutor = self.MPICommExecutor(MPI.COMM_SELF)
        with mpicommexecutor as executor:
            try:
                with mpicommexecutor:
                    pass
            except RuntimeError:
                pass
            else:
                self.fail('expected RuntimeError')


SKIP_POOL_TEST = False
name, version = MPI.get_vendor()
if name == 'Open MPI':
    if version < (2,2,0):
        SKIP_POOL_TEST = True
    if version < (1,8,0):
        SKIP_POOL_TEST = True
    if sys.platform.startswith('win'):
        SKIP_POOL_TEST = True
if name == 'MPICH':
    if MPI.COMM_WORLD.Get_attr(MPI.APPNUM) is None:
        SKIP_POOL_TEST = True
if name == 'MVAPICH2':
    SKIP_POOL_TEST = True
if name == 'MPICH2':
    if (version > (1,2,0) and
        MPI.COMM_WORLD.Get_attr(MPI.APPNUM) is None):
        SKIP_POOL_TEST = True
    if version < (1,0,6):
        SKIP_POOL_TEST = True
    if sys.platform.startswith('win'):
        SKIP_POOL_TEST = True
if name == 'Microsoft MPI':
    SKIP_POOL_TEST = True
if name == 'Platform MPI':
    SKIP_POOL_TEST = True
if name == 'HP MPI':
    SKIP_POOL_TEST = True
if MPI.Get_version() < (2,0):
    SKIP_POOL_TEST = True


if futures._worker.SharedPool:
    del MPICommExecutorTest.test_arg_root
    del MPICommExecutorTest.test_arg_comm_bad
    if MPI.COMM_WORLD.Get_size() == 1:
        del ProcessPoolPickleTest
elif SKIP_POOL_TEST or MPI.COMM_WORLD.Get_size() > 1:
    del ProcessPoolInitTest
    del ProcessPoolBootupTest
    del ProcessPoolShutdownTest
    del ProcessPoolWaitTest
    del ProcessPoolAsCompletedTest
    del ProcessPoolExecutorTest
    del ProcessPoolSubmitTest
    del ProcessPoolPickleTest

if __name__ == '__main__':
    unittest.main()
