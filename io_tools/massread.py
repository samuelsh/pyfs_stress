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

MAX_SCANNING_THREADS = 64
KB1 = 1024
MB1 = KB1 * 1024
COLLECTOR_INTERVAL = 60

threads_count = 0
total_scanned_files = 0
total_scanned_dirs = 0
total_scanned_snapshots = 0
read_files = 0

scan_lock = threading.Lock()
dirs_cnt_lock = threading.Lock()
files_cnt_lock = threading.Lock()
read_lock = threading.Lock()
snap_cnt_lock = threading.Lock()

logger = None
stop_event = None
scan_threads_semaphore = threading.BoundedSemaphore(MAX_SCANNING_THREADS)


class TestStatsCollector(threading.Timer):
    def __init__(self, func, args=None, interval=COLLECTOR_INTERVAL):
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


def read_file(path, chunk_size=MB1):
    with open(path, 'rb') as f:
        for _ in iter(lambda: f.read(chunk_size), b""):
            pass


def open_file(path):
    with open(path, 'rb'):
        pass


def stat_file(path):
    os.stat(path)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("-s", "--skipread", help="Skip read. Open only", action="store_true")
    parser.add_argument("-t", "--threads", help="Max scanning threads", type=int, default=MAX_SCANNING_THREADS)
    parser.add_argument("--snapshots", help="Allow read snapshots directory", action="store_true")
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    return parser.parse_args()


def print_stats(dirs_queue):
    logger.info(f"### Total spawned scanner workers: {threads_count} Read Queue depth: {dirs_queue.qsize()} ###")
    logger.info(f"### Total Read Files: {read_files} ###")
    logger.info(f"### Total Scanned Files: {total_scanned_files} ###")
    logger.info(f"### Total Scanned Dirs: {total_scanned_dirs} ###")
    logger.info(f"### Total Scanned Snapshots: {total_scanned_snapshots} ###")


def dir_scanner(files_queue, mount_points, root_dir, next_mp, max_scanning_threads=MAX_SCANNING_THREADS,
                read_snapshots=False):
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
                    if threads_count >= max_scanning_threads:
                        logger.debug(f"Number of threads reached {max_scanning_threads}. "
                                     f"Waiting any thread to finish...")
                    with scan_threads_semaphore:
                        logger.debug(f"Entry {entry.name} is Directory. Spawning new thread...")
                        next_mp += 1
                        if next_mp >= len(mount_points):
                            next_mp = 0
                        new_entry_path = list(filter(None, entry.path.split("/")))[2:]
                        new_entry_path = "/".join(new_entry_path)
                        scan_thread = threading.Thread(target=dir_scanner, args=(files_queue, mount_points,
                                                                                 new_entry_path, next_mp,
                                                                                 read_snapshots))
                        scan_thread.start()
                    if read_snapshots:
                        with scan_threads_semaphore:
                            snap_path = list(filter(None, full_root_path.split("/")))[2:]
                            snap_path = "/".join(snap_path + [".snapshot"])
                            snap_thread = threading.Thread(target=snap_scanner, args=(files_queue, mount_points,
                                                                                      snap_path, next_mp,
                                                                                      read_snapshots))
                            snap_thread.start()

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


def snap_scanner(files_queue, mount_points, root_dir, next_mp, max_scanning_threads=MAX_SCANNING_THREADS,
                 read_snapshots=False):
    global stop_event, threads_count, total_scanned_files, total_scanned_dirs, total_scanned_snapshots
    ident = threading.get_ident()
    try:
        with scan_lock:
            threads_count += 1
        full_root_path = "/".join([mount_points[next_mp], root_dir])
        if not os.path.exists(full_root_path):
            return
        logger.info(f"Snap Scanner Worker {ident}: Scanning: {full_root_path}")
        with os.scandir(full_root_path) as dirs_iterator:
            for entry in dirs_iterator:
                if stop_event.is_set():
                    return
                if entry.is_dir():
                    with dirs_cnt_lock:
                        total_scanned_dirs += 1
                    if threads_count >= max_scanning_threads:
                        logger.debug(f"Number of threads reached {max_scanning_threads}. "
                                     f"Waiting any thread to finish...")
                    with scan_threads_semaphore:
                        logger.debug(f"Entry {entry.name} is Directory. Spawning new thread...")
                        next_mp += 1
                        if next_mp >= len(mount_points):
                            next_mp = 0
                        new_entry_path = list(filter(None, entry.path.split("/")))[2:]
                        new_entry_path = "/".join(new_entry_path)
                        scan_thread = threading.Thread(target=dir_scanner, args=(files_queue, mount_points,
                                                                                 new_entry_path, next_mp, False))
                        scan_thread.start()
                else:
                    files_queue.put(entry.path)
                    with files_cnt_lock:
                        total_scanned_files += 1
            with snap_cnt_lock:
                total_scanned_snapshots += 1
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


def reader_worker(files_queue, skip_read):
    global stop_event, read_files
    ident = threading.get_ident()
    full_path = ""
    logger.info(f"Reader Worker {ident} started...")
    while not stop_event.is_set():
        try:
            full_path = files_queue.get(timeout=0.1)
            if skip_read:
                stat_file(full_path)
            else:
                read_file(full_path)
            with read_lock:
                read_files += 1
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(f"Reader Worker {ident} Error: {e}, Path: {full_path}")
        except queue.Empty:
            logger.info(f"Empty queue: Reader Worker {ident} done and exits.")
            return


def main():
    global logger, stop_event
    logger = ConsoleLogger('mass_reader').logger
    stop_event = Event()
    files_queue = queue.Queue()

    args = get_args()
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'MASSREAD', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()

    futures = []
    mount_points = mounter.mount_points
    scanning_threads = MAX_SCANNING_THREADS if not args.snapshots else MAX_SCANNING_THREADS // 2
    stats_collector = TestStatsCollector(print_stats, args=[files_queue, ])
    stats_collector.start()
    logger.info("Workers ThreadPool started")
    with ThreadPoolExecutor() as executor:
        futures.append(executor.submit(dir_scanner, files_queue, mount_points, args.test_dir, 0,
                                       read_snapshots=args.snapshots, max_scanning_threads=scanning_threads))
        for _ in range(64):
            futures.append(executor.submit(reader_worker, files_queue, args.skipread))
    for future in futures:
        try:
            logger.info("{}".format("Job Done OK" if not future.result() else ""))
        except Exception as e:
            logger.error(f"ThreadPool raised exception {e}")
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
