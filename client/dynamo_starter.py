#!/usr/bin/env python
import argparse
import os
import traceback
from multiprocessing import Event
from multiprocessing import Process

import time

import sys

from dynamo import Dynamo
from logger import Logger
from config import DYNAMO_PATH, MAX_WORKERS_PER_CLIENT, CLIENT_MOUNT_POINT
from utils import shell_utils


def run_worker(logger, event, controller, server, proc_id):
    worker = Dynamo(logger, event, controller, server, proc_id)
    worker.run()


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='Test Runner script')
    parser.add_argument('-c', '--controller', type=str, required=True, help='Controller host name')
    parser.add_argument('-s', '--server', type=str, required=True, help='Cluster Server hostname')
    parser.add_argument('-e', '--export', type=str, help='NFS Export Name', default="vol0")
    parser.add_argument('-m', '--mtype', type=int, help='Mount Type', default=3)
    args = parser.parse_args()
    return args


def run():
    stop_event = Event()
    logger = Logger(output_dir=DYNAMO_PATH, mp=True).logger
    processes = []
    args = get_args()
    logger.info("Making {0}".format(CLIENT_MOUNT_POINT))
    os.mkdir(CLIENT_MOUNT_POINT)
    logger.info("Mounting work path...")
    if not shell_utils.mount(args.server, args.export, CLIENT_MOUNT_POINT, args.mtype):
        logger.error("Mount failed. Exiting...")
        return
    # Start a few worker processes
    for i in range(MAX_WORKERS_PER_CLIENT):
        processes.append(Process(target=run_worker, args=(logger, stop_event, args.controller, args.server, i,)))
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
