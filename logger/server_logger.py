import gzip
import logging
from logging import handlers
import os
import sys

import zlib

import shutil

__author__ = 'samuels'


class Logger:
    def __init__(self, output_dir=""):
        self.output_dir = output_dir
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.DEBUG)

        # create console handler and set level to info
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        # create debug file handler and set level to debug, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "logs/controller_debug.log"), "a",
                                               100 * 1024 * 1024, 10)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        handler.rotator = zip_rotator
        handler.namer = zip_namer
        self._logger.addHandler(handler)

        # create debug file handler and set level to error, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "logs/controller_error.log"), "a",
                                               100 * 1024 * 1024, 10)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        handler.rotator = zip_rotator
        handler.namer = zip_namer
        self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger


class ConsoleLogger:
    def __init__(self, name):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        # create console handler and set level to info
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - [%(name)s]: %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger


class StatsLogger:
    def __init__(self, name, output_dir=""):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)

        # create console handler and set level to info
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s; - %(message)s")
        handler.setFormatter(formatter)
        handler.rotator = zip_rotator
        handler.namer = zip_namer
        self._logger.addHandler(handler)

        # create debug file handler and set level to error, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "logs/test_stats.log"),
                                               "a", 100 * 1024 * 1024, 10)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s; - %(message)s")
        handler.setFormatter(formatter)
        handler.rotator = zip_rotator
        handler.namer = zip_namer
        self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger


def zip_namer(name):
    return name + ".gz"


def zip_rotator(source, dest):
    with open(source, 'rb') as f_in:
        with gzip.open(dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(source)
