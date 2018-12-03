#!/usr/bin/env python3.6
"""
author: samuels
"""
import errno
import argparse
import os
import queue
import sys
import time
import threading

sys.path.append(os.path.join(os.path.join('../')))
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger

logger = None
stop_event = None

MAX_SCANNING_THREADS = 100
threads_count = 0
total_scanned_files = 0
deleted_files = 0
renamed_files = 0

lock = threading.Lock()


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


class TestStatsCollector(threading.Timer):
    def __init__(self, func, args=None, interval=60):
        """
        :param func: func
        :param args: set
        :param interval: int
        """
        super().__init__(interval, func, args=args)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)


def print_stats(dirs_queue):
    logger.info(f"### Total spawned workers: {threads_count} Queue depth: {dirs_queue.qsize()} ###")
    logger.info(f"### Deleted Files: {deleted_files} Renamed Files: {renamed_files}")


def dir_scanner(files_queue, root_dir):
    global stop_event, threads_count, total_scanned_files
    try:
        logger.debug(f"Scanner Worker {threading.get_ident()}: Scanning {root_dir}")
        with lock:
            threads_count += 1
        with os.scandir(root_dir) as dirs_iterator:
            for entry in dirs_iterator:
                if stop_event.is_set():
                    return
                if entry.is_dir():
                    if threads_count >= MAX_SCANNING_THREADS:
                        logger.info(f"Number of threads reached {MAX_SCANNING_THREADS}. "
                                    f"Waiting any thread to finish...")
                        while threads_count >= MAX_SCANNING_THREADS:
                            time.sleep(0.1)
                    logger.debug(f"Entry {entry.name} is Directory. Spawning new thread...")
                    scan_thread = threading.Thread(target=dir_scanner, args=(files_queue, entry.path))
                    scan_thread.start()
                else:
                    files_queue.put(entry.path)
                    with lock:
                        total_scanned_files += 1
            logger.debug(f"Scanner Worker {threading.get_ident()}: Done Scanning {root_dir}")
            with lock:
                threads_count -= 1
        if threads_count <= 0:
            logger.info(f"Scanner Worker {threading.get_ident()}: Spawned threads {threads_count}. "
                        f"Done scanning, waiting for all worker threads to complete")
    except OSError as e:
        if e.errno == errno.EACCES:
            logger.warn(f"Scanner Woker {threading.get_ident()} failed due to {e} and will be stopped")
        else:
            logger.exception(f"Scanner Worker {threading.get_ident()} Error: {e}. Shutting down...")
            stop_event.set()


def rename_worker(dirs_queue):
    global stop_event, renamed_files
    logger.info(f"Renamer Worker {threading.get_ident()} started...")
    while not stop_event.is_set():
        try:
            full_path = dirs_queue.get(timeout=10)
            os.rename(full_path, "/".join([os.path.dirname(full_path), "".join(["renamed_", str(time.time())])]))
            with lock:
                renamed_files += 1
        except OSError as e:
            logger.error(f"Renamer Worker {threading.get_ident()} Error: {e}")
        except queue.Empty:
            return


def delete_worker(dirs_queue):
    global stop_event, deleted_files
    logger.info(f"Deleter Worker {threading.get_ident()} started...")
    while not stop_event.is_set():
        try:
            full_path = dirs_queue.get(timeout=10)
            os.remove(full_path)
            with lock:
                deleted_files += 1
        except OSError as e:
            logger.error(f"Deleter Worker {threading.get_ident()} Error: {e}")
        except queue.Empty:
            return


def main():
    global logger, stop_event
    logger = ConsoleLogger('md_massdel').logger
    stop_event = Event()
    dirs_queue = queue.Queue()

    args = get_args()
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'MASSDEL_RENAME', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()

    futures = []
    mp = mounter.get_random_mountpoint()
    test_dir = os.path.join(mp, args.test_dir)
    logger.info("Selected mountpoint to be scanned: {}".format(test_dir))
    stats_collector = TestStatsCollector(print_stats, args=[dirs_queue, ])
    stats_collector.start()
    logger.info("Workers ThreadPool started")
    with ThreadPoolExecutor() as executor:
        futures.append(executor.submit(dir_scanner, dirs_queue, test_dir))
        if args.action == "all" or args.action == "rename":
            for _ in range(100):
                futures.append(executor.submit(rename_worker, dirs_queue))
        if args.action == "all" or args.action == "delete":
            for _ in range(100):
                futures.append(executor.submit(delete_worker, dirs_queue))
    for future in futures:
        try:
            logger.info(f'{"Job Done OK" if not future.result() else ""}')
        except Exception as e:
            logger.error(f"ThreadPool raised exception: {e}")
            raise e
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
