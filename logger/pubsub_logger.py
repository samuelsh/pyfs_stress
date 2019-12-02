import logging
import pathlib
from logging import handlers
import os

import gzip

import shutil
import zmq
import socket
from zmq.log.handlers import PUBHandler

import config

__author__ = 'samuels'

formatters = {
    logging.DEBUG: logging.Formatter("[%(name)s] %(message)s"),
    logging.INFO: logging.Formatter("[%(name)s] %(message)s"),
    logging.WARN: logging.Formatter("[%(name)s] %(message)s"),
    logging.ERROR: logging.Formatter("[%(name)s] %(message)s"),
    logging.CRITICAL: logging.Formatter("[%(name)s] %(message)s")
}


class PUBLogger:
    def __init__(self, host, port=config.PUBSUB_LOGGER_PORT):
        self._logger = logging.getLogger(socket.gethostname())
        self._logger.setLevel(logging.DEBUG)
        self.ctx = zmq.Context()
        self.pub = self.ctx.socket(zmq.PUB)
        self.pub.connect('tcp://{0}:{1}'.format(host, port))
        # create console handler and set level to info
        # handler = logging.StreamHandler(sys.stdout)
        self._handler = PUBHandler(self.pub)
        self._handler.formatters = formatters
        self._logger.addHandler(self._handler)

    @property
    def logger(self):
        return self._logger


class SUBLogger:
    def __init__(self, ip, output_dir="", port=config.PUBSUB_LOGGER_PORT):
        self.output_dir = output_dir
        self._logger = logging.getLogger('sub_logger')
        self._logger.setLevel(logging.DEBUG)

        self.ctx = zmq.Context()
        self._sub = self.ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.SUBSCRIBE, b"")
        self._sub.bind('tcp://*:{1}'.format(ip, port))
        try:
            pathlib.Path('logs').mkdir()
        except FileExistsError:
            pass

        # create debug file handler and set level to debug, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "logs/client_debug.log"), "w",
                                               100 * 1024 * 1024, 100)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        handler.rotator = zip_rotator
        handler.namer = zip_namer

        self._logger.addHandler(handler)

        # create debug file handler and set level to error, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "logs/client_error.log"), "w",
                                               100 * 1024 * 1024, 100)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        handler.rotator = zip_rotator
        handler.namer = zip_namer
        self._logger.addHandler(handler)

    @property
    def sub(self):
        return self._sub

    @property
    def logger(self):
        return self._logger


def zip_namer(name):
    return name + ".gz"


def _gzip_file(source, dest):
    with open(source, 'rb') as f_in:
        with gzip.open(dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
    os.remove(source)


def zip_rotator(source, dest):
        _gzip_file(source, dest)
