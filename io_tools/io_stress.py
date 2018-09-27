#!/usr/bin/env python3.6
"""
    IO Stress test - 2018 (c) samuel
"""
import multiprocessing
import sys

import os

import argparse
import queue
import random
from concurrent.futures import ProcessPoolExecutor

sys.path.append(os.path.join(os.path.join('../')))
from client.generic_mounter import Mounter
from data_operations import data_tools
from data_operations.data_tools import DATA_PATTERNS
from io_tools.uitls import futures_validator
from logger.server_logger import ConsoleLogger
from server.test_stats_collector import TestStatsCollector, MPCounters

logger = None
stop_event = None
io_counters = MPCounters()
test_stats_collector = None

TEST_DIR = "test_dir_{}".format
KB1 = 1024
KB4 = KB1 * 4
MB1 = KB1 * 1024
TB1 = MB1 * 1024 * 1024


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="", type=str)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    return parser.parse_args()


def print_chunks_stats():
    logger.info("### Chunks in queue: {} Total chunks on disk: {} ###".
                format(io_counters.chunks_in_queue.value, io_counters.chunks_on_disk.value))


def data_chunks_generator_worker(data_queue, chunks_number, file_size):
    sizes = [7, 15, 63, 125, 255, 511, 1023]  # size in 1K chunks
    lock = multiprocessing.Lock()
    for _ in range(chunks_number):
        if stop_event.is_set():
            return
        data = random.choice(DATA_PATTERNS)
        data_pattern = data_tools.handle_data_type(data['type'], data['data'])
        try:
            data_pattern = data_pattern.encode()
        except AttributeError:
            pass
        offset = random.randint(0, file_size)
        size = random.choice(sizes)
        data_queue.put({
            'data_pattern': data_pattern[:KB1],  # By default datasets are 4K so we truncate them to 1K before put
            'offset': offset,
            'chunk_size': size + (size % 8),  # chunk  size + alignment
        })
        with lock:
            io_counters.chunks_in_queue.value += 1


def singe_file_random_writes_worker(mount_point, dir_name, file_name, data_queue, mode):
    write_mode = f"{mode}b"
    file_path = os.path.join(mount_point, dir_name, file_name)
    lock = multiprocessing.Lock()
    logger.info(f"Test path: {file_path}")
    try:
        if not os.path.exists(os.path.join(file_path)):
            with open(os.path.join(file_path), 'wb'):
                pass
        while True:
            if stop_event.is_set():
                return
            data = data_queue.get(timeout=60)
            with open(file_path, write_mode) as f:
                f.write(data['data_pattern'] * (data['chunk_size']))
            with lock:
                io_counters.chunks_on_disk.value += 1
    except queue.Empty:
        logger.info(f"Random Writer Worker [{os.getpid()}]: Queue is empty. Exiting...")
        stop_event.set()
    except (OSError, IOError) as e:
        logger.error(f"Random Writer Worker [{os.getpid()}] stopped on error: {e}. "
                     f"File: {file_name} Offset: {data['offset']} Inode: {hex(os.stat(file_path).st_ino)}")
        stop_event.set()
        raise e
    except Exception as e:
        logger.error(f"Unhandled Exception {e}")
        stop_event.set()
        raise e


def main():
    global logger, stop_event, test_stats_collector
    logger = ConsoleLogger('io_stress').logger
    data_queue = multiprocessing.Manager().Queue()
    stop_event = multiprocessing.Event()
    file_name = "io_test_huge_file.bin"
    chunks_number = 100000000
    test_stats_collector = TestStatsCollector(print_chunks_stats)
    mode = 'a+'

    args = get_args()
    dir_name = args.test_dir
    logger.info("Mounting work path...")
    mounter = Mounter(args.cluster, args.export, 'nfs3', 'IO_STRESS', logger=logger,
                      sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
    try:
        mounter.mount_all_vips()
    except AttributeError:
        logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
        mounter.mount()
    logger.info("Done mounting. Starting workload ...")
    dir_path = os.path.join(mounter.get_random_mountpoint(), dir_name)
    try:
        os.mkdir(dir_path)
    except FileExistsError:
        pass
    futures = []
    test_stats_collector.start()
    cores = multiprocessing.cpu_count()
    with ProcessPoolExecutor() as executor:
        for _ in range(10):
            futures.append(executor.submit(data_chunks_generator_worker, data_queue,
                                           chunks_number, TB1))
        for i in range(cores - 10):
            futures.append(executor.submit(singe_file_random_writes_worker, mounter.get_random_mountpoint(), dir_name,
                                           f'{file_name}-{i}', data_queue, mode))
    futures_validator(futures, logger)
    for i in range(cores - 10):
        logger.info(f"Test completed. Deleting the HUGE file {file_name}-{i}")
        os.remove(os.path.join(mounter.get_random_mountpoint(), dir_name, f'{file_name}-{i}'))
    test_stats_collector.cancel()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped by user. Bye-Bye ...")
        test_stats_collector.cancel()
        stop_event.set()
    except Exception as app_error:
        logger.exception(app_error)
