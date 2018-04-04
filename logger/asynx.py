from functools import wraps
from multiprocessing import Process, get_context
from multiprocessing.queues import Queue
from threading import Thread
import time

from multiprocessing import Lock


class BlockedQueue(Queue):
    def __init__(self, maxsize=-1, block=True, timeout=None):
        self.block = block
        self.timeout = timeout
        super().__init__(maxsize, ctx=get_context())

    def put(self, obj, block=True, timeout=None):
        super().put(obj, block=self.block, timeout=self.timeout)

    def get(self, block=True, timeout=None):
        if self.empty():
            return None
        return super().get(block=self.block, timeout=self.timeout)


def _execute(queue, f, *args, **kwargs):
    try:
        queue.put(f(*args, **kwargs))
    except Exception as e:
        queue.put(e)


def threaded(timeout=None, block=True):
    def decorator(func):
        queue = BlockedQueue(1, block, timeout)

        @wraps(func)
        def wrapper(*args, **kwargs):
            args = (queue, func) + args
            t = Thread(target=_execute, args=args, kwargs=kwargs)
            t.start()
            return queue.get()

        return wrapper

    return decorator


def processed(timeout=None, block=True):
    def decorator(func):
        queue = BlockedQueue(1, block, timeout)

        @wraps(func)
        def wrapper(*args, **kwargs):
            args = (queue, func) + args
            p = Process(target=_execute, args=args, kwargs=kwargs)
            p.start()
            return queue.get()

        return wrapper

    return decorator


def async_call(async_api=Thread, timeout=None, block=True):
    def decorator(func):
        queue = BlockedQueue(1, block, timeout)

        @wraps(func)
        def wrapper(*args, **kwargs):
            args = (queue, func) + args
            async = async_api(target=_execute, args=args, kwargs=kwargs)
            async.start()
            return queue.get()

        return wrapper

    return decorator


def scheduled(period, delay=None, loop_count=None):
    delay = delay or 0
    loop_count = loop_count or 0

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            counter = 0
            time.sleep(delay)
            while True:
                start = time.time()
                if loop_count and loop_count > 0:
                    if counter == loop_count:
                        break
                    counter += 1
                func(*args, **kwargs)
                run_time = time.time() - start
                if run_time < period:
                    time.sleep(period - run_time)

        return wrapper

    return decorator


simple_lock = Lock()


def synchronized(lock=simple_lock):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                return func(*args, **kwargs)

        return wrapper

    return decorator


if __name__ == '__main__':
    @threaded(block=False)
    def test1(x):
        time.sleep(x)
        print("test 1")


    @processed(block=False)
    def test2(x):
        time.sleep(x)
        print("test 2")


    @threaded(block=False)
    @scheduled(period=2, loop_count=3)
    def test3(x):
        time.sleep(x)
        print("test 3")


    @threaded()
    @scheduled(period=1, loop_count=2)
    @processed()
    def test_pow(x):
        print(x * x)


    @threaded()
    @synchronized()
    def lock_test_a():
        print('lock_test_a')


    @async_call(Thread)
    @synchronized()
    def lock_test_b():
        print('lock_test_b')


    test3(0)
    test1(2)
    test2(1)
    test_pow(5)
    lock_test_a()
    lock_test_b()
