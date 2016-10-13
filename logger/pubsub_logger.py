import logging
from logging import handlers
import os
import zmq
import sys
import socket
from zmq.log.handlers import PUBHandler
import config

__author__ = 'samuels'


class PUBLogger:
    def __init__(self, ip, port=config.PUBSUB_LOGGER_PORT):
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)
        self.ctx = zmq.Context()
        self.pub = self.ctx.socket(zmq.PUB)
        self.pub.connect('tcp://{0}:{1}'.format(ip, port))
        # create console handler and set level to info
        # handler = logging.StreamHandler(sys.stdout)
        handler = PUBHandler(self.pub)
        handler.setLevel(logging.DEBUG)
        formatters = {
            logging.DEBUG: logging.Formatter("%(asctime)s %(hostname)s; %(levelname)s - %(message)s"),
            logging.INFO: logging.Formatter("%(asctime)s %(hostname)s; %(levelname)s - %(message)s"),
            logging.WARN: logging.Formatter("%(asctime)s %(hostname)s; %(levelname)s - %(message)s"),
            logging.ERROR: logging.Formatter("%(asctime)s %(hostname)s; %(levelname)s - %(message)s"),
            logging.CRITICAL: logging.Formatter("%(asctime)s %(hostname)s; %(levelname)s - %(message)s")
                      }
        handler.formatter = formatters
        self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger


class SUBLogger:
    def __init__(self, ip, output_dir="", port=config.PUBSUB_LOGGER_PORT):
        self.output_dir = output_dir
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)

        self.ctx = zmq.Context()
        self._sub = self.ctx.socket(zmq.SUB)
        self._sub.bind('tcp://{0}:{1}'.format(ip, port))
        self._sub.setsockopt(zmq.SUBSCRIBE, "")
        # create console handler and set level to info
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        # create debug file handler and set level to debug, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "client_debug.log"), "a", 100 * 1024 * 1024, 10)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        # create debug file handler and set level to error, file will rotate each 100MB
        handler = handlers.RotatingFileHandler(os.path.join(output_dir, "client_error.log"), "a", 100 * 1024 * 1024, 10)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    @property
    def sub(self):
        return self._sub

    @property
    def logger(self):
        return self._logger
