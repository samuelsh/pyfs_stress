#!/usr/bin/env python3.6
"""
author: samuels
"""

import argparse
import hashlib
import os
import queue
import sys

import random
import threading

sys.path.append(os.path.join(os.path.join('../')))
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger
from utils.shell_utils import StringUtils

logger = None
stop_event = None
files_counter = 0
dir_counter = 0

KB1 = 1024
MB1 = KB1 * 1024

DATA_BUF = os.urandom(KB1 * 8)

data_array = []


class StatsCollector(threading.Timer):
    def __init__(self, func, interval=60):
        super().__init__(interval, func)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)


def print_stats_worker():
    global logger, dir_counter, files_counter
    logger.info("#### Stats >>> Created Directories: {} Created Files: {}".format(dir_counter, files_counter))


def init_data_array():
    global data_array

    for _ in range(100000):
        buf = os.urandom(KB1 * 4)
        buf = buf[0:random.randint(KB1 * 4 - 1, KB1 * 4)]
        checksum = hashlib.md5(buf).hexdigest()
        data_array.append({'filename': f"{checksum}_{len(buf)}", 'data': buf})


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("-f", "--files_num", help="Total to to create per directory", default=10000, type=int)
    parser.add_argument("-t", "--threads", help="Number of files producer threads", default=16, type=int)
    parser.add_argument("--duration", type=int, help="Test duration (in minutes)", default=10)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    return parser.parse_args()


def futures_validator(futures, raise_on_error=True):
    global logger
    for future in futures:
        try:
            future.result()
        except Exception as e:
            logger.error("ThreadPool raised exception: {}. Exiting with error.".format(e))
            raise e


def files_producer_worker(mp, test_dir, repeats=10000):
    global stop_event, logger, data_array
    try:
        for _ in range(repeats):
            if stop_event.is_set():
                break
            file_entry = random.choice(data_array)
            file_path = os.path.join(mp, test_dir, file_entry['filename'])
            try:
                with open(file_path, "wb") as f:
                    f.write(file_entry['data'])
            except FileExistsError:
                pass
    except (IOError, OSError) as err:
        logger.error(f"Files produces worker {threading.get_ident()} raised stop event due to error {err}")
        stop_event.set()
        raise err
    except queue.Empty:
        pass


def main():
    global logger, stop_event
    logger = ConsoleLogger('msrsync_sim').logger
    stats_collector = StatsCollector(print_stats_worker)
    stop_event = Event()
    args = get_args()
    test_dir = args.test_dir
    files_num = args.files_num

    logger.info("Initialising DataSet ...")
    init_data_array()

    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'MSRSYNC_SIM', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to single mount")
        mounter.mount()
    mount_point = mounter.get_random_mountpoint()
    try:
        os.mkdir(os.path.join(mount_point, test_dir))
    except FileExistsError as e:
        logger.warn(f"{e}")
    logger.info(f"Test directory {test_dir} created on {mount_point}")

    futures = []
    logger.info(f"Going to produce {files_num * 100} files")
    with ThreadPoolExecutor() as executor:
        for _ in range(100):
            futures.append(executor.submit(files_producer_worker, mounter.get_random_mountpoint(), test_dir, files_num))
    futures_validator(futures, True)

    logger.info("Done writing dataset, verifying...")
    scandir_iterator = os.scandir(os.path.join(mount_point, test_dir))
    for file_entry in scandir_iterator:
        file_name = file_entry.name
        stored_checksum, stored_length = file_name.split('_')
        if int(stored_length) != os.stat(file_entry.path).st_size:
            raise RuntimeError(f"File {file_entry.path} length mismatch!"
                               f" {int(stored_length)} != {os.stat(file_entry.path).st_size}")
        with open(file_entry.path, 'rb') as f:
            buf = f.read()
            checksum = hashlib.md5(buf).hexdigest()
            if stored_checksum != checksum:
                raise RuntimeError(f"File {file_entry.path} checksum mismatch!"
                                   f" {stored_checksum} != {checksum}")

    logger.info("### Workload is Done. Come back tomorrow.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Test stopped by user. See ya!")
        stop_event.set()
    except Exception as generic_error:
        logger.exception(generic_error)
        sys.exit(1)
