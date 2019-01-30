#!/usr/bin/env python3.6
"""
author: samuels
"""
from _md5 import md5

import argparse
import hashlib
import io
import os
import queue
import sys
import uuid
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


def get_random_buf(size):
    return os.urandom(size)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("-f", "--files_num", help="Total to to create per directory", default=10000, type=int)
    parser.add_argument("-n", "--number", help="Total to to create per directory", default=10000, type=int)
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
    logger = ConsoleLogger('patterns_sim').logger
    stop_event = Event()
    args = get_args()
    test_dir = args.test_dir

    logger.info("Initialising DataSet ...")
    init_data_array()

    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'PATTERNS', logger=logger, nodes=0,
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

    file_name = f"hhmi_tstfile-{uuid.uuid4()}"
    with open(os.path.join(mount_point, test_dir, file_name), "wb") as f, \
            open(os.path.join('/vast', file_name), "wb") as f2:
        for i in range(1024 * 1):
            if round(random.random() * 10) % 2:
                buf_size = KB1 * 4 - 2
                hole_size = 2
            else:
                buf_size = KB1 * 4 - 1
                hole_size = 1
            skip_size = hole_size + random.choice([0, KB1 * 4, KB1 * 8, KB1 * 16])
            buf = get_random_buf(buf_size)
            f.write(buf)
            f2.write(buf)
            offset = f.tell()
            logger.info(f"Going to write buf_size={buf_size} at offset={offset}")
            f.seek(offset + skip_size)
            f2.seek(offset + skip_size)
            logger.info(f"Offset after seek={f.tell()}")

    logger.info("Comparing VAST vs Local client before fsync:")
    with open(os.path.join(mount_point, test_dir, file_name), "rb") as f:
        vast_checksum = hashlib.md5()
        for chunk in iter(lambda: f.read(MB1), b""):
            vast_checksum.update(chunk)
        vast_checksum = vast_checksum.hexdigest()
    with open(os.path.join('/vast', file_name), "rb") as f:
        local_checksum = hashlib.md5()
        for chunk in iter(lambda: f.read(MB1), b""):
            local_checksum.update(chunk)
        local_checksum = local_checksum.hexdigest()
    logger.info(f"Local checksum={local_checksum}, Vast checksum={vast_checksum}")
    assert vast_checksum == local_checksum
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
