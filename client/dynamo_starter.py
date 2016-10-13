#!/usr/bin/env python
import argparse
import os
import traceback
from multiprocessing import Event
from multiprocessing import Process

import time

import sys
from random import randint

from dynamo import Dynamo
from logger import pubsub_logger
from config import DYNAMO_PATH, MAX_WORKERS_PER_CLIENT, CLIENT_MOUNT_POINT
from utils import shell_utils


def run_worker(logger, event, controller, server, nodes, domains, proc_id):
    worker = Dynamo(logger, event, controller, server, nodes, domains, proc_id)
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
    parser.add_argument('-n', '--nodes', type=int, help='Number of active nodes', required=True)
    parser.add_argument('-d', '--domains', type=int, help='Number of fs domains', required=True)
    parser.add_argument('-m', '--mtype', type=int, help='Mount Type', default=3)
    args = parser.parse_args()
    return args


def run():
    stop_event = Event()
    processes = []
    args = get_args()
    logger = pubsub_logger.PUBLogger(args.controller, output_dir=DYNAMO_PATH).logger
    logger.info("Making {0}".format(CLIENT_MOUNT_POINT))
    if not os.path.exists(CLIENT_MOUNT_POINT):
        os.mkdir(CLIENT_MOUNT_POINT)
    else:  # if folder already created, umounting just in case ....
        try:
            shell_utils.umount(CLIENT_MOUNT_POINT)
        except Exception as syserr:
            logger.error(syserr)
    # if not shell_utils.mount(args.server, args.export, CLIENT_MOUNT_POINT, args.mtype):
    #     logger.error("Mount failed. Exiting...")
    #     return
    # multidomain nfs  mount
    try:
        os.chdir('/qa/dynamo/client')
        logger.info("Setting passwordless SSH connection")
        shell_utils.ShellUtils.run_shell_script("/zebra/qa/qa-util-scripts/set-ssh-python", args.server, False)
        logger.info("Getting cluster params...")
        active_nodes = shell_utils.FSUtils.get_active_nodes_num(args.server)
        logger.debug("Active Nodes: %s" % active_nodes)
        domains = shell_utils.FSUtils.get_domains_num(args.server)
        logger.debug("FSD domains: %s" % domains)
        logger.info("Mounting work path...")
        shell_utils.FSUtils.mount_fsd(args.server, '/' + args.export, active_nodes, domains, 'nfs3', 'DIRSPLIT', '6')
        # /mnt/DIRSPLIT-node0.g8-5
        for i in range(active_nodes):
            for j in range(domains):
                if not os.path.ismount('/mnt/%s-node%d.%s-%d' % ('DIRSPLIT', i, args.server, j)):
                    logger.error('mount_fsd failed!')
                    raise RuntimeError
    except Exception as error_on_init:
        logger.error("".join(error_on_init) + " WorkDir: {0}".format(os.getcwd()))
        raise
    # Start a few worker processes
    for i in range(MAX_WORKERS_PER_CLIENT):
        processes.append(Process(target=run_worker,
                                 args=(logger, stop_event, args.controller, args.server, args.nodes, args.domains, i,)))
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
        raise
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
