#!/usr/bin/env python3.6
"""
author: samuels (c) 2018
"""
import argparse
import time
import os
import sys
from queue import Queue
from threading import Thread
sys.path.append(os.path.join(os.path.join('../')))
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger

logger = None


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    return parser.parse_args()


def open_file_for_n_sec(q1, n, path):
    global logger
    i = 0
    while True:
        try:
            filename = q1.get()
            file_path = os.path.join(path, filename)
            i += 1
            if i % 1000:
                sys.stdout.write('.')
                sys.stdout.flush()
            f = open(file_path, 'w')
            f.write('abcd')
            f.flush()

            os.unlink(file_path)
            # time.sleep(.01)
            f.close()
        except (IOError, OSError) as err:
            logger.error("Thread raised error: {}".format(err))
            raise err


def main():
    global logger
    logger = ConsoleLogger('bmp_split_stress').logger
    q1 = Queue(maxsize=10)
    num_threads = 100
    num_files = 100000

    args = get_args()
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'OPEN_CREATE_STRESS', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()
    for i in range(num_threads):
        path = mounter.get_random_mountpoint()
        worker = Thread(target=open_file_for_n_sec, args=(q1, 1, path))
        worker.setDaemon(True)
        worker.start()

    for i in range(num_files):
        q1.put('t%d' % i)

    time.sleep(2)


if __name__ == "__main__":
    main()
