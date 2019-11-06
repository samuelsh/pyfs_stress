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

MAX_SCANNING_THREADS = 100
threads_count = 0
total_scanned_files = 0
total_scanned_dirs = 0
read_files = 0
deleted_files = 0
renamed_files = 0

scan_lock = threading.Lock()
dirs_cnt_lock = threading.Lock()
files_cnt_lock = threading.Lock()
delete_lock = threading.Lock()
rename_lock = threading.Lock()

logger = None
stop_event = None
scan_threads_semaphore = threading.BoundedSemaphore(MAX_SCANNING_THREADS)


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
    logger.info(f"### Total spawned scanner workers: {threads_count} Read Queue depth: {dirs_queue.qsize()} ###")
    logger.info(f"### Total Read Files: {read_files} ###")
    logger.info(f"### Total Scanned Files: {total_scanned_files} ###")
    logger.info(f"### Total Scanned Dirs: {total_scanned_dirs} ###")
    logger.info(f"### Deleted Files: {deleted_files} Renamed Files: {renamed_files} ###")


def dir_scanner(files_queue, mount_points, root_dir, next_mp):
    global stop_event, threads_count, total_scanned_files, total_scanned_dirs
    ident = threading.get_ident()
    try:
        with scan_lock:
            threads_count += 1
        full_root_path = "/".join([mount_points[next_mp], root_dir])
        logger.info(f"Scanner Worker {ident}: Scanning: {full_root_path}")
        with os.scandir(full_root_path) as dirs_iterator:
            for entry in dirs_iterator:
                if stop_event.is_set():
                    return
                if entry.is_dir():
                    with dirs_cnt_lock:
                        total_scanned_dirs += 1
                    if threads_count >= MAX_SCANNING_THREADS:
                        logger.debug(f"Number of threads reached {MAX_SCANNING_THREADS}. "
                                     f"Waiting any thread to finish...")
                    with scan_threads_semaphore:
                        logger.debug(f"Entry {entry.name} is Directory. Spawning new thread...")
                        next_mp += 1
                        if next_mp >= len(mount_points):
                            next_mp = 0
                        new_entry_path = list(filter(None, entry.path.split("/")))[2:]
                        new_entry_path = "/".join(new_entry_path)
                        scan_thread = threading.Thread(target=dir_scanner, args=(files_queue, mount_points,
                                                                                 new_entry_path, next_mp))
                        scan_thread.start()
                else:
                    files_queue.put(entry.path)
                    with files_cnt_lock:
                        total_scanned_files += 1
            logger.debug(f"Scanner Worker {ident}: Done Scanning {root_dir}")
            with scan_lock:
                threads_count -= 1
        if threads_count <= 0:
            logger.info(f"Scanner Worker {ident}: Spawned threads {threads_count}. "
                        f"Done scanning, waiting for all worker threads to complete")
    except OSError as e:
        if e.errno == errno.EACCES:
            logger.warn(f"Scanner Worker {ident} failed due to {e} and will be stopped")
        else:
            logger.exception(f"Scanner Worker {ident} Error: {e}. Shutting down...")
            stop_event.set()


def rename_worker(dirs_queue):
    global stop_event, renamed_files
    ident = threading.get_ident()
    src_path = ""
    dst_path = ""
    logger.info(f"Renamer Worker {ident} started...")
    while not stop_event.is_set():
        try:
            src_path = dirs_queue.get(timeout=10)
            dst_path = "/".join([os.path.dirname(src_path), "".join(["renamed_", str(time.time())])])
            os.rename(src_path, dst_path)
            with rename_lock:
                renamed_files += 1
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(f"Renamer Worker {ident} Error: {e}, Src: {src_path}, Dst: {dst_path}")
        except queue.Empty:
            logger.error(f"Empty queue: Renamer Worker {ident} done and exits.")
            return


def delete_worker(dirs_queue):
    global stop_event, deleted_files
    ident = threading.get_ident()
    full_path = ""
    logger.info(f"Deleter Worker {ident} started...")
    while not stop_event.is_set():
        try:
            full_path = dirs_queue.get(timeout=60)
            os.remove(full_path)
            with delete_lock:
                deleted_files += 1
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(f"Deleter Worker {ident} Error: {e}, Path: {full_path}")
        except queue.Empty:
            logger.error(f"Empty queue: Deleter Worker {ident} done and exits.")
            return


def main():
    global logger, stop_event, threads_count
    logger = ConsoleLogger('md_massdel').logger
    stop_event = Event()
    files_queue = queue.Queue()

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
    mount_points = mounter.mount_points
    stats_collector = TestStatsCollector(print_stats, args=[files_queue, ])
    stats_collector.start()
    logger.info("Workers ThreadPool started")
    with ThreadPoolExecutor() as executor:
        futures.append(executor.submit(dir_scanner, files_queue, mount_points, args.test_dir, 0))
        if args.action == "all" or args.action == "rename":
            for _ in range(64):
                futures.append(executor.submit(rename_worker, files_queue))
        if args.action == "all" or args.action == "delete":
            for _ in range(64):
                futures.append(executor.submit(delete_worker, files_queue))
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
