#!/usr/bin/env python3.6
"""
author: samuels
"""
import argparse
import os
import queue
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger

logger = None
stop_event = None


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    parser.add_argument('--action', type=str, choices=['rename', 'delete', 'all'], help="Action to select",
                        default="all")
    return parser.parse_args()


def dir_scanner_worker(dirs_queue, test_dir):
    global stop_event
    try:
        for dir_path, _, file_names in os.walk(test_dir):
            for f in file_names:
                full_path = "/".join([dir_path, f])
                dirs_queue.put(full_path)
        logger.info("Done Directory Scan. ")
    except Exception as e:
        logger.error("dir_scanner raising stop event due to error! {}".format(e))
        stop_event.set()


def rename_worker(dirs_queue):
    global stop_event
    timeout_counter = 0
    while not stop_event.is_set():
        try:
            full_path = dirs_queue.get(timeout=0.1)
            try:
                os.rename(full_path, "/".join([os.path.dirname(full_path), "".join(["renamed_", str(time.time())])]))
            except Exception as e:
                logger.error("Renamer Error: {}".format(e))
        except queue.Empty:
            time.sleep(1)
            timeout_counter += 1
            if timeout_counter > 10:
                logger.info("Files queue is empty for too long. Assuming tree scan is completed")
                return


def delete_worker(dirs_queue):
    global stop_event
    timeout_counter = 0
    while not stop_event.is_set():
        try:
            full_path = dirs_queue.get(timeout=0.1)
            try:
                os.remove(full_path)
            except Exception as e:
                logger.error("Deleter Error: {}".format(e))
        except queue.Empty:
            time.sleep(1)
            timeout_counter += 1
            if timeout_counter > 10:
                logger.info("Files queue is empty for too long. Assuming tree scan is completed")
                return


def main():
    global logger, stop_event
    logger = ConsoleLogger('md_stress').logger
    stop_event = Event()
    dirs_queue = queue.Queue()

    args = get_args()
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'CREATE_MOVE_DIR', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()

    results = []
    mp = mounter.get_random_mountpoint()
    test_dir = os.path.join(mp, args.test_dir)
    logger.info("Selected mountpoint to be scanned: {}".format(test_dir))
    logger.info("Workers ThreadPool started")
    with ThreadPoolExecutor() as executor:
        results.append(executor.submit(dir_scanner_worker, dirs_queue, test_dir))
        if args.action == "all" or args.action == "rename":
            for _ in range(16):
                results.append(executor.submit(rename_worker, dirs_queue))
        if args.action == "all" or args.action == "delete":
            for _ in range(16):
                results.append(executor.submit(delete_worker, dirs_queue))
    for result in results:
        try:
            logger.info("{}".format("Job OK" if not result.result() else ""))
        except Exception as e:
            logger.error("ThreadPool raised exception {}".format(e))
            raise e


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Test stopped by user. See ya!")
        stop_event.set()
    except Exception as generic_error:
        logger.exception(generic_error)
        sys.exit(1)
