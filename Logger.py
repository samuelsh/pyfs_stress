import logging
import os
import sys

__author__ = 'samuels'


class Logger:
    def __init__(self, output_dir=""):
        self.output_dir = output_dir

        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)

        # create console handler and set level to info
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        # create debug file handler and set level to debug
        handler = logging.FileHandler(os.path.join(output_dir, "debug_log.log"), "w")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger
