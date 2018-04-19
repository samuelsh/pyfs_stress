#!/usr/bin/env python3.6
import argparse
import os
import traceback
from multiprocessing import Event
from multiprocessing import Process

import time

import sys

from generic_mounter import Mounter
from dynamo import Dynamo
from logger import pubsub_logger
from config import MAX_WORKERS_PER_CLIENT


def run_worker(event, mounter, controller, server, nodes, domains, proc_id):
    worker = Dynamo(event, mounter, controller, server, nodes, domains, proc_id)
    worker.run()


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='Test Runner script')
    parser.add_argument('-c', '--controller', type=str, required=True, help='Controller host name')
    parser.add_argument('-s', '--server', type=str, required=True, help='Cluster Server hostname')
    parser.add_argument('-e', '--export', type=str, help='NFS Export Name', default="/")
    parser.add_argument('-n', '--nodes', type=int, help='Number of active nodes', default=0)
    parser.add_argument('-d', '--domains', type=int, help='Number of fs domains', default=0)
    parser.add_argument('-m', '--mtype', type=str, help='Mount Type', choices=['nfs3', 'nfs4', 'nfs4.1', 'smb1', 'smb2',
                                                                               'smb3'], default="nfs3")
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    args = parser.parse_args()
    return args


def run():
    stop_event = Event()
    processes = []
    args = get_args()
    logger = pubsub_logger.PUBLogger(args.controller).logger
    time.sleep(10)
    try:
        os.chdir('/qa/dynamo/client')
        logger.info("Mounting work path...")
        mounter = Mounter(args.server, args.export, args.mtype, 'DIRSPLIT', logger=logger, nodes=args.nodes,
                          domains=args.domains, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
        try:
            mounter.mount_all_vips()
        except AttributeError:
            logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
            mounter.mount()
    except Exception as error_on_init:
        logger.error(str(error_on_init) + " WorkDir: {0}".format(os.getcwd()))
        raise
    # Start a few worker processes
    for i in range(MAX_WORKERS_PER_CLIENT):
        processes.append(Process(target=run_worker,
                                 args=(stop_event, mounter, args.controller, args.server, args.nodes,
                                       args.domains, i,)))
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
    else:
        logger.exception()
        raise Exception
    logger.info('waiting for processes to die...')
    for p in processes:
        p.join()
    logger.info('all done')


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
