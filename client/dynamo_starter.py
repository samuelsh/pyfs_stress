#!/usr/bin/env python
import traceback
from multiprocessing import Event
from multiprocessing import Process

import time

import sys

from client.dynamo import Dynamo
from logger import Logger


def run_worker(logger, event):
    worker = Dynamo(logger, event)
    worker.run()


def run():
    stop_event = Event()
    logger = Logger(mp=True).logger
    processes = []
    # Start a few worker processes
    for i in range(10):
        processes.append(Process(target=run_worker, args=(logger, stop_event,)))
    for p in processes:
        p.start()
    try:
        time.sleep(5)
        # The controller will set the stop event when it's finished, just
        # idle until then
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
    logger.info('waiting for processes to die...')
    for p in processes:
        p.join()
    print('all done')


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
