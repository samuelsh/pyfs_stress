#!/usr/bin/env python3.6
"""
author: samuels
"""

import argparse
import os
import queue
import sys

import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger
from utils.shell_utils import StringUtils

logger = None
stop_event = None
files_counter = None
dir_counter = None


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


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("-n", "--dirs_num", help="Total directories to create", default=10000, type=int)
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


def dir_producer_worker(mounter, test_dir, num_dirs, dirs_queue):
    global stop_event, logger, dir_counter
    lock = threading.Lock()
    try:
        for _ in range(num_dirs):
            if stop_event.is_set():
                break
            i = dirs_queue.get_nowait()
            mp = mounter.get_random_mountpoint()
            dir_path = os.path.join(mp, test_dir, "dir-{}".format(i))
            os.mkdir(dir_path)
            with lock:
                dir_counter += 1
    except (IOError, OSError) as err:
        logger.error("Directories produces worker raised stop event due to error {}".format(err))
        stop_event.set()
        raise err
    except queue.Empty:
        pass


def files_producer_worker(mounter, dirs_queue, test_dir, num_files):
    global stop_event, logger, files_counter
    lock = threading.Lock()
    try:
        while not stop_event.is_set():
            index = dirs_queue.get()
            dir_path = os.path.join(mounter.get_random_mountpoint(), test_dir, "".join(["dir-", index]))
            file_path = os.path.join(dir_path, StringUtils.get_random_string_nospec(16))
            with open(file_path, "w") as f:
                f.flush()
                os.fsync(f.fileno())
            dirs_queue.put(index)
            with lock:
                files_counter += 1
            if files_counter > num_files:
                break
    except (IOError, OSError) as err:
        logger.error("Files produces worker raised stop event due to error {}".format(err))
        stop_event.set()
        raise err
    except queue.Empty:
        pass
    except Exception as err:
        logger.error("Unhandled exception: {}".format(err))
        stop_event.set()
        raise err


def main():
    global logger, stop_event
    logger = ConsoleLogger('bmp_split_stress').logger
    stats_collector = StatsCollector(print_stats_worker)
    stop_event = Event()
    dirs_queue = queue.Queue()
    args = get_args()
    test_dir = args.test_dir
    dirs_num = args.dirs_num
    files_num = args.files_num
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'BM_SPLIT_DIR', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()
    for i in range(dirs_num):
        dirs_queue.put(str(i))
    os.mkdir(os.path.join(mounter.get_random_mountpoint(), test_dir))
    logger.info("Test directory created on {}".format(mounter.get_random_mountpoint(), test_dir))
    stats_collector.start()

    futures = []
    logger.info("Going to produce {} Directories".format(dirs_num))
    with ThreadPoolExecutor() as executor:
        for _ in range(16):
            futures.append(executor.submit(dir_producer_worker, mounter, test_dir, dirs_num, dirs_queue))
    futures_validator(futures, True)

    for i in range(dirs_num):
        dirs_queue.put(str(i))
    stop_event = Event()
    logger.info("Starting Producing Files...")
    with ThreadPoolExecutor() as executor:
        for _ in range(args.threads):
            futures.append(executor.submit(files_producer_worker, mounter, dirs_queue, test_dir, files_num))
    futures_validator(futures)
    logger.info("### Workload is Done. Come back tomorrow.")
    stats_collector.cancel()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Test stopped by user. See ya!")
        stop_event.set()
    except Exception as generic_error:
        logger.exception(generic_error)
        sys.exit(1)
