#!/usr/bin/env python3.6
"""
author: samuels
"""

import argparse
import os
import queue
import sys

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger
from utils.shell_utils import StringUtils

logger = None
stop_event = None


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("-n", "--dirs_num", help="Total directories to create", default=10000, type=int)
    parser.add_argument("-f", "--files_num", help="Total to to create per directory", default=1000, type=int)
    parser.add_argument("-t", "--threads", help="Number of files producer threads", default=16, type=int)
    parser.add_argument("--duration", type=int, help="Test duration (in minutes)", default=10)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    return parser.parse_args()


def futures_validator(futures):
    global logger
    for future in futures:
        try:
            future.result()
        except Exception as e:
            logger.error("ThreadPool raised exception: {}. Exiting with error.".format(e))


def dir_producer_worker(mounter, test_dir, num_dirs):
    global stop_event, logger
    try:
        for _ in range(num_dirs):
            if stop_event.is_set():
                break
            mp = mounter.get_random_mountpoint()
            dir_path = os.path.join(mp, test_dir, StringUtils.get_random_string_nospec(16))
            os.mkdir(dir_path)
    except (IOError, OSError) as err:
        logger.error("Directories produces worker raised stop event due to error {}".format(err))
        stop_event.set()
        raise err


def dir_scanner_worker(mounter, test_dir, dirs_queue):
    global stop_event, logger
    test_dir = os.path.join(mounter.get_random_mountpoint(), test_dir)
    logger.info("Start Directory {} Scan.".format(test_dir))
    try:
        for dir_path, dir_names, _ in os.walk(test_dir):
            for d in dir_names:
                full_path = "/".join([dir_path, d])
                dirs_queue.put(full_path)
                if stop_event.is_set():
                    break
        logger.info("Done Directory Scan. ")
    except (IOError, OSError) as err:
        logger.error("dir_scanner_worker raising stop event due to error! {}".format(err))
        stop_event.set()
        raise err


def files_producer_worker(dirs_queue):
    global stop_event, logger
    try:
        while not stop_event.is_set():
            dir_path = dirs_queue.get(timeout=1)
            file_path = os.path.join(dir_path, StringUtils.get_random_string_nospec(16))
            with open(file_path, "w") as f:
                f.flush()
                os.fsync(f.fileno())
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
    stop_event = Event()
    dirs_queue = queue.Queue()

    args = get_args()
    test_dir = args.test_dir
    dirs_num = args.dirs_num // 16 + args.dirs_num % 16
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'BM_SPLIT_DIR', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()
    os.mkdir(os.path.join(mounter.get_random_mountpoint(), test_dir))
    logger.info("Test directory created on {}".format(mounter.get_random_mountpoint(), test_dir))
    futures = []
    logger.info("Going to produce {} Directories per thread".format(dirs_num))
    with ThreadPoolExecutor() as executor:
        for _ in range(16):
            futures.append(executor.submit(dir_producer_worker, mounter, test_dir, dirs_num))
    futures_validator(futures)
    cycle = args.files_num
    for i in range(cycle):
        stop_event = Event()
        logger.info("Starting Directories Scan cycle {}".format(i))
        with ThreadPoolExecutor() as executor:
            futures.append(executor.submit(dir_scanner_worker, mounter, test_dir, dirs_queue))
            for _ in range(16):
                futures.append(executor.submit(files_producer_worker, dirs_queue))
        futures_validator(futures)
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
