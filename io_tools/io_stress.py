#!/usr/bin/env python3.6
"""
    IO Stress test - 2018 (c) samuel
"""
import sys

import os

import argparse
import queue
import random
import threading
from concurrent.futures import ThreadPoolExecutor
sys.path.append(os.path.join(os.path.join('../')))
from client.generic_mounter import Mounter
from data_operations import data_tools
from data_operations.data_tools import DATA_PATTERNS
from io_tools.uitls import futures_validator
from logger.server_logger import ConsoleLogger
from server.test_stats_collector import TestStatsCollector, Counters

logger = None
io_counters = Counters()

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
                format(io_counters.chunks_in_queue, io_counters.chunks_on_disk))


def data_chunks_generator_worker(data_queue, chunks_number, file_size, stop_event, io_counters):
    chunks = [KB1, KB1 * 4, KB1 * 8, KB1 * 16, KB1 * 32, KB1 * 64, KB1 * 126, KB1 * 256, KB1 * 512, MB1]
    lock = threading.Lock()
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
        chunk = random.choice(chunks)
        data_queue.put({
            'data_pattern': data_pattern[:KB1],  # By default datasets are 4K so we truncate them to 1K before put
            'offset': offset,
            'chunk_size': chunk,
        })
        with lock:
            io_counters.chunks_in_queue += 1


def singe_file_random_writes_worker(mounter, dir_name, file_name, data_queue, mode, stop_event, io_counters):
    write_mode = f"{mode}b"
    mp = mounter.get_random_mountpoint()
    file_path = os.path.join(mp, dir_name, file_name)
    lock = threading.Lock()
    try:
        while True:
            if stop_event.is_set():
                return
            data = data_queue.get(timeout=60)
            with open(file_path, write_mode) as f:
                f.seek(data['offset'])
                f.write(data['data_pattern'] * data['chunk_size'])
                f.flush()
                os.fsync(f.fileno())
            with lock:
                io_counters.chunks_on_disk += 1
    except queue.Empty:
        logger.info(f"Random Writer Worker [{threading.get_ident()}]: Queue is empty. Exiting...")
        stop_event.set()
    except (OSError, IOError) as e:
        logger.error(f"Random Writer Worker [{threading.get_ident()}] stopped on error: {e}. "
                     f"File: {file_name} Offset: {data['offset']} Inode: {hex(os.stat(file_path).st_ino)}")
        stop_event.set()
        raise e
    except Exception as e:
        logger.error(f"Unhandled Exception {e}")
        stop_event.set()
        raise e


def main():
    global logger
    logger = ConsoleLogger('io_stress').logger
    data_queue = queue.Queue()
    stop_event = threading.Event()
    file_name = "io_test_huge_file.bin"
    chunks_number = 1000000
    mode = 'r+'

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
    logger.info(f"Test path: {dir_path}/{file_name}")
    with open(os.path.join(dir_path, file_name), 'wb'):
        pass
    test_stats_collector = TestStatsCollector(print_chunks_stats)
    test_stats_collector.start()
    with ThreadPoolExecutor() as executor:
        for _ in range(2):
            futures.append(executor.submit(data_chunks_generator_worker, data_queue,
                                           chunks_number // 2, TB1, stop_event, io_counters))
        for _ in range(64):
            futures.append(executor.submit(singe_file_random_writes_worker, mounter, dir_name,
                                           file_name, data_queue, mode, stop_event, io_counters))
    futures_validator(futures, logger)
    logger.info(f"Test completed. Deleting the HUGE file {file_name}")
    os.remove(os.path.join(mounter.get_random_mountpoint(), dir_name, file_name))
    test_stats_collector.cancel()


if __name__ == '__main__':
    try:
        main()
    except Exception as app_error:
        logger.exception(app_error)
