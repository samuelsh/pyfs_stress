#!/usr/bin/env python3.6

"""
NLM File Locking Stress
author: samuels - 2018 (c)
"""
import errno
import fcntl
import mmap
import contextlib
import multiprocessing
import argparse
import os
import sys
import random
import string
import threading
from enum import Enum
from timeit import default_timer as timer

sys.path.append(os.path.join(os.path.join('../')))
from concurrent.futures import ProcessPoolExecutor
from client.generic_mounter import Mounter
from logger.server_logger import ConsoleLogger

TEST_DIR = "test_dir_{}".format
KB1 = 1024
KB4 = KB1 * 4
MB1 = KB1 * 1024
TB1 = MB1 * 1024 * 1024
LOCKING_RANGES = [1, 4, 8, 16, 32, 64, 128, 256, 512, KB1, KB4, KB1 * 8, KB1 * 16, KB1 * 64, KB1 * 128, KB1 * 256,
                  KB1 * 512]

logger = None
stop_event = None
successful_locks = multiprocessing.Value('i', 0)
failed_locks = multiprocessing.Value('i', 0)
total_write_ops = multiprocessing.Value('i', 0)
total_read_ops = multiprocessing.Value('i', 0)
total_time_spent_in_lock = multiprocessing.Value('f', 0)
total_time_spent_in_unlock = multiprocessing.Value('f', 0)
total_time_spent_in_random_write = multiprocessing.Value('f', 0)
total_time_spent_in_random_read = multiprocessing.Value('f', 0)


class LockType(Enum):
    EXCLUSIVE = fcntl.LOCK_EX
    SHARED = fcntl.LOCK_SH
    EXCLUSIVE_NB = fcntl.LOCK_EX | fcntl.LOCK_NB
    SHARED_NB = fcntl.LOCK_SH | fcntl.LOCK_NB
    UNLOCK = fcntl.LOCK_UN


class StatsCollector(threading.Timer):
    def __init__(self, func, interval=60):
        super().__init__(interval, func)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)


def print_stats_worker():
    global logger, successful_locks, failed_locks, total_time_spent_in_lock, total_time_spent_in_unlock, \
        total_time_spent_in_random_read, total_time_spent_in_random_write, total_read_ops, total_write_ops
    logger.info("#### NLM LOCKING STRESS STATS ####")
    logger.info(f"Successful Locks: {successful_locks.value} Failed Locks: {failed_locks.value}")
    logger.info(f"Average lock time: {total_time_spent_in_lock.value / (successful_locks.value + failed_locks.value)}")
    logger.info(f"Total random write ops: {total_write_ops.value}")
    logger.info(f"Total random read ops: {total_read_ops.value}")
    logger.info(f"Average random write time: {total_time_spent_in_random_write.value / total_write_ops.value}")
    logger.info(f"Average random read time: {total_time_spent_in_random_read.value / total_read_ops.value}")


def write_profiler(f):
    """
    Profiler decorator to measure duration of file operations
    """

    def wrapper(fh, data_buf):
        global total_time_spent_in_random_write
        lock = multiprocessing.Lock()
        start = timer()
        f(fh, data_buf)
        end = timer()
        with lock:
            total_time_spent_in_random_write.value += end - start

    return wrapper


def read_profiler(f):
    """
    Profiler decorator to measure duration of file operations
    """

    def wrapper(fh):
        global total_time_spent_in_random_read
        lock = multiprocessing.Lock()
        start = timer()
        f(fh)
        end = timer()
        with lock:
            total_time_spent_in_random_read.value += end - start

    return wrapper


def lock_profiler(f):
    """
    Profiler decorator to measure duration of file operations
    """

    def wrapper(fh, file_name, **kwargs):
        global total_time_spent_in_lock
        lock = multiprocessing.Lock()
        start = timer()
        f(fh, file_name, **kwargs)
        end = timer()
        with lock:
            total_time_spent_in_lock.value += end - start

    return wrapper


def unlock_profiler(f):
    """
    Profiler decorator to measure duration of file operations
    """

    def wrapper(fh, file_name, **kwargs):
        global total_time_spent_in_unlock
        lock = multiprocessing.Lock()
        start = timer()
        f(fh, file_name, **kwargs)
        end = timer()
        with lock:
            total_time_spent_in_unlock.value += end - start

    return wrapper


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument("-f", "--file_name", help="File name under test", default="nlm_test_file", type=str)
    parser.add_argument("-s", "--size", help="File size under test", default=MB1, type=int)
    parser.add_argument("-p", "--processes", help="Number of file locking processes",
                        default=multiprocessing.cpu_count(), type=int)
    parser.add_argument("--withio", help="Run I/O thread while locking", action="store_true")
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
            raise e


@lock_profiler
def lockf(fh, file_name, lock_type=LockType.EXCLUSIVE_NB.value, start=0, length=0, whence=os.SEEK_SET):
    logger.debug(f"lock method: lock_type: {LockType(lock_type).name}({lock_type})")
    logger.debug(f"Going to lock {file_name} Range: {start}:{length} Inode: {hex(os.fstat(fh.fileno()).st_ino)}")
    fcntl.lockf(fh.fileno(), lock_type, length, start, whence)
    logger.debug(f"{file_name} is locked")


@unlock_profiler
def unlockf(fh, file_name, start=0, length=0, whence=os.SEEK_SET):
    logger.debug(f"Going to unlock {file_name} Range: {start}:{length} Inode: {hex(os.fstat(fh.fileno()).st_ino)}")
    fcntl.lockf(fh.fileno(), LockType.UNLOCK.value, length, start, whence)


@contextlib.contextmanager
def direct_read_open(file_path):
    fd = None
    try:
        fd = os.open(file_path, os.O_RDONLY | os.O_DIRECT)
        with mmap.mmap(fd, 0, access=mmap.ACCESS_READ) as mm:
            yield mm
    finally:
        if fd:
            os.close(fd)


@contextlib.contextmanager
def direct_write_open(file_path):
    fd = None
    try:
        fd = os.open(file_path, os.O_RDWR | os.O_DIRECT)
        with mmap.mmap(-1, KB4, access=mmap.ACCESS_WRITE) as mm:
            yield mm
    finally:
        if fd:
            os.close(fd)


@read_profiler
def direct_random_read(fd):
    os.read(fd, KB4)


@write_profiler
def direct_random_write(fd, mm):
    os.write(fd, mm)


@write_profiler
def random_write(fh, data_buf):
    fh.write(data_buf)


@read_profiler
def random_read(fh):
    fh.read(KB4)


def file_writer_worker(mount_points, test_dir, file_name, file_size):
    global stop_event, total_write_ops
    lock = multiprocessing.Lock()
    mp = random.choice(mount_points)
    fd = None
    try:
        fd = os.open(os.path.join(mp, test_dir, file_name), os.O_RDWR | os.O_DIRECT)
        while not stop_event.is_set():
            data_buf = random.choice(string.printable).encode() * KB4
            offset = random.randint(0, file_size - KB4)
            offset //= KB4  # Aligning to 4K to be able do direct read
            os.lseek(fd, offset, os.SEEK_SET)
            direct_random_write(fd, data_buf)
            with lock:
                total_write_ops.value += 1
    except Exception as e:
        logger.error(f"Writer Thread {threading.get_ident()} failed due to {e}")
        raise
    finally:
        if fd:
            os.close(fd)


def file_reader_worker(mount_points, test_dir, file_name, file_size):
    global stop_event, total_read_ops
    lock = multiprocessing.Lock()
    mp = random.choice(mount_points)
    fd = None
    try:
        fd = os.open(os.path.join(mp, test_dir, file_name), os.O_RDWR | os.O_DIRECT)
        with direct_read_open(os.path.join(mp, test_dir, file_name)) as f:
            while not stop_event.is_set():
                offset = random.randint(0, file_size - KB4)
                offset //= KB4  # Aligning to 4K to be able do direct read
                f.seek(offset)
                direct_random_read(fd)
                with lock:
                    total_read_ops.value += 1
    except Exception as e:
        logger.error(f"Reader Thread {threading.get_ident()} failed due to {e}")
        raise
    finally:
        if fd:
            os.close(fd)


def file_locker_worker(mount_points, test_dir, file_name):
    global stop_event, logger, successful_locks, failed_locks
    lock = multiprocessing.Lock()
    mp = random.choice(mount_points)
    file_path = os.path.join(mp, test_dir, file_name)
    lock_types = [LockType.EXCLUSIVE_NB.value, LockType.SHARED_NB.value]
    fh = open(file_path, "r+b")
    while not stop_event.is_set():
        try:
            lock_type = random.choice(lock_types)
            length = random.choice(LOCKING_RANGES)
            start = random.randint(0, MB1 - 1)
            if start + length > MB1:
                length = MB1 - (start + length)
            lockf(fh, file_name, lock_type=lock_type, start=start, length=length)
            with lock:
                successful_locks.value += 1
            unlockf(fh, file_name, start=start, length=length)
        except OSError as err:
            if err.errno == errno.EAGAIN:
                with lock:
                    failed_locks.value += 1
            elif err.errno == errno.ENOLCK:
                with lock:
                    failed_locks.value += 1
                logger.warn(f"Process {os.getpid()} run out of locks")
            else:
                logger.error(f"File Locker Worker {os.getpid()} raised stop event due to error {err}")
                stop_event.set()
                fh.close()
                raise err
    fh.close()


def main():
    global logger, stop_event
    logger = ConsoleLogger('NLM_STRESS').logger
    stats_collector = StatsCollector(print_stats_worker)
    stop_event = multiprocessing.Event()
    writer_worker = None
    reader_worker = None
    args = get_args()
    test_dir = args.test_dir
    file_name = args.file_name
    file_size = args.size
    locking_processes = args.processes
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'NLM_STRESS', logger=logger, nodes=0,
                      domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to single mount")
        mounter.mount()
    test_dir_path = os.path.join(mounter.get_random_mountpoint(), test_dir)
    try:
        os.mkdir(test_dir_path)
    except FileExistsError as e:
        logger.warn("{}".format(e))
    logger.info(f"Test directory created on {test_dir_path}")
    test_file_path = os.path.join(test_dir_path, file_name)
    if not os.path.exists(test_file_path):
        with open(test_file_path, "w+b") as fh:
            fh.write(os.urandom(file_size))
    logger.info(f"Test file created on {test_file_path}")
    futures = []
    stats_collector.start()
    if args.withio:
        writer_worker = threading.Thread(target=file_writer_worker, args=(mounter.mount_points, test_dir, file_name,
                                                                          file_size))
        reader_worker = threading.Thread(target=file_reader_worker, args=(mounter.mount_points, test_dir, file_name,
                                                                          file_size))
        writer_worker.start()
        reader_worker.start()
    logger.info(f"Going to fork {locking_processes} locking processes")
    with ProcessPoolExecutor(locking_processes) as executor:
        for _ in range(locking_processes):
            futures.append(executor.submit(file_locker_worker, mounter.mount_points, test_dir, file_name))
    futures_validator(futures)
    writer_worker.join()
    reader_worker.join()
    stats_collector.cancel()
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
