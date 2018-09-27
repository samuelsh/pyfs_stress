"""
    Scheduled stats collector - 2018 (c)
"""
import multiprocessing

import threading


class TestStatsCollector(threading.Timer):
    def __init__(self, func, args=None, interval=60):
        """
        :param func: func
        :param args: set
        :param interval: int
        """
        super().__init__(interval, func, args=args)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)


class Counters:
    def __init__(self):
        self.total_files_in_queue_counter = 0
        self.total_files_created = 0
        self.rewrite_files_counter = 0
        self.chunks_in_queue = 0
        self.chunks_on_disk = 0


class MPCounters:
    def __init__(self):
        self.total_files_in_queue_counter = multiprocessing.Value('i', 0)
        self.total_files_created = multiprocessing.Value('i', 0)
        self.rewrite_files_counter = multiprocessing.Value('i', 0)
        self.chunks_in_queue = multiprocessing.Value('i', 0)
        self.chunks_on_disk = multiprocessing.Value('i', 0)


class Stats:
    pass
