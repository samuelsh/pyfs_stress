#!/usr/bin/env python3.6
"""
author: samuels
"""
import argparse
import os
import queue
import sys

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Timer
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger
from utils.shell_utils import StringUtils

logger = None
stop_event = None
dirs_counter = 0
files_counter = 0
delete_counter = 0


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("--duration", type=int, help="Test duration (in minutes)", default=10)
    parser.add_argument("--timeout", type=int, help="Set mount timeout option (in seconds)", default=600)
    parser.add_argument("--retrans", type=int, help="Set mount retries number", default=3)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    return parser.parse_args()


def futures_validator(futures, raise_on_error=True):
    global logger
    for future in futures:
        try:
            future.result()
        except Exception as e:
            logger.error("Future raised exception: {}. Exiting with error.".format(e))
            if raise_on_error:
                raise e


def dir_producer_worker(mounter, dirs_queue, dirs_to_delete):
    global stop_event, logger, dirs_counter
    lock = threading.Lock()
    try:
        while not stop_event.is_set():
            mp = mounter.get_random_mountpoint()
            dir_path = os.path.join(mp, StringUtils.get_random_string_nospec(16))
            os.mkdir(dir_path)
            dirs_queue.put(dir_path)
            dirs_to_delete.put(dir_path)
            with lock:
                dirs_counter += 1
    except (IOError, OSError) as err:
        logger.error("Directories produces worker raised error {}".format(err))
        stop_event.set()
        raise err


def files_producer_worker(dirs_queue):
    global stop_event, logger, files_counter
    lock = threading.Lock()
    try:
        while not stop_event.is_set():
            dir_path = dirs_queue.get(timeout=1)
            file_path = os.path.join(dir_path, StringUtils.get_random_string_nospec(16))
            with open(file_path, "w"):
                pass
            with lock:
                files_counter += 1
    except (IOError, OSError) as err:
        logger.error("Files produces worker raised error {}".format(err))
        stop_event.set()
        raise err
    except queue.Empty:
        logger.warn("File producer queue is empty... Moving on")


def dirs_delete_worker(dirs_to_delete):
    global stop_event, logger, delete_counter
    lock = threading.Lock()
    try:
        while not stop_event.is_set():
            dir_path = dirs_to_delete.get(timeout=1)
            shutil.rmtree(dir_path)
            with lock:
                delete_counter += 1
    except (IOError, OSError) as err:
        logger.error("Deleter worker raised error {}".format(err))
        stop_event.set()
        raise err
    except queue.Empty:
        logger.info("Done deleting files.")


def workload_stopper():
    global logger, stop_event
    logger.info("### Stopping  workload ###")
    stop_event.set()


def main():
    global logger, stop_event
    logger = ConsoleLogger('bmp_split_stress').logger
    stop_event = Event()
    dirs_queue = queue.Queue()
    dirs_to_delete = queue.Queue()

    args = get_args()
    stopper_thread = Timer(60 * args.duration, workload_stopper)
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'BM_SPLIT_DIR', logger=logger,
                      sudo=True, timeout=int(args.timeout * 0.1), retrans=args.retrans,
                      start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()
    logger.info("Done mounting. Starting workload ...")
    futures = []
    stopper_thread.start()
    with ThreadPoolExecutor() as executor:
        for _ in range(64):
            futures.append(executor.submit(dir_producer_worker, mounter, dirs_queue, dirs_to_delete))
        for _ in range(64):
            futures.append(executor.submit(files_producer_worker, dirs_queue))
    futures_validator(futures)
    logger.info("#### Totally created >>> Directories: {} Files: {}".format(dirs_counter, files_counter))
    logger.info("#### Deleting Workload DataSet ####")
    stop_event = Event()
    futures = []
    with ThreadPoolExecutor() as executor:
        for _ in range(64):
            futures.append(executor.submit(dirs_delete_worker, dirs_to_delete))
    futures_validator(futures)
    logger.info("#### Totally deleted directories: {}".format(delete_counter))
    logger.info("Workload is Done. Come back tomorrow.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Test stopped by user. See ya!")
        stop_event.set()
    except Exception as generic_error:
        logger.exception(generic_error)
        sys.exit(1)
