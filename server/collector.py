"""
Collector service provides methods for collection of test runtime results results and storing results

2017 - samuels(c)
"""
import time

from logger import server_logger


class Collector:
    def __init__(self, test_stats, stop_event):
        self.logger = server_logger.StatsLogger('__Collector__').logger
        self.test_stats = test_stats
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            self.logger.info("{0}".format("############################"))
            self.logger.info("{0}".format("#### Test Runtime Stats ####"))
            self.logger.info("{0}".format("############################"))
            self.logger.info("{0:10d}".format("Total file operations executed {0}".format(self.test_stats['total'])))
            self.logger.info("{0:10d}".format("Total file operations succeeded {0}"
                                          .format(self.test_stats['success']['total'])))
            self.logger.info("{0:10d}".format("Total file operations failed {0}"
                                          .format(self.test_stats['failed']['total'])))
            self.logger.info("{0}".format("=== Successful operations stats ==="))
            for k, v in self.test_stats['success'].items():
                self.logger.info("{0:10d}".format("{0}: {1}".format(k, v)))
            self.logger.info("{0}".format("=== Failed operations stats ==="))
            for k, v in self.test_stats['failed'].items():
                self.logger.info("{0:10d}".format("{0}: {1}".format(k, v)))
            time.sleep(60)
