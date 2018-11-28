#!/usr/bin/env python3.6
"""
author: samuels
"""

import argparse
import errno
import multiprocessing
import os
import shutil
import signal
import string
import sys
import threading
import time
import traceback
from random import choice
sys.path.append(os.path.join(os.path.join('../')))
from client.generic_mounter import Mounter
from logger.server_logger import Logger

MAX_PROCESSES = 160
MAX_FILES = 10000

files_queue = multiprocessing.Manager().Queue()
stop_event = multiprocessing.Event()
file_creator_pool = None
file_renamer_pool = None
file_create_lock = None
total_files = None
stopped_processes_count = None
user_exit_request = False
mounter = None


def get_random_unicode(length):
    get_char = chr

    # Update this to include code point ranges to be sampled
    include_ranges = [
        (0x0021, 0x0021),
        (0x0023, 0x0026),
        (0x0028, 0x007E),
        (0x00A1, 0x00AC),
        (0x00AE, 0x00FF),
        (0x0100, 0x017F),
        (0x0180, 0x024F),
        (0x2C60, 0x2C7F),
        (0x16A0, 0x16F0),
        (0x0370, 0x0377),
        (0x037A, 0x037E),
        (0x0384, 0x038A),
        (0x038C, 0x038C),
    ]

    alphabet = [
        get_char(code_point) for current_range in include_ranges
        for code_point in range(current_range[0], current_range[1] + 1)
    ]
    return ''.join(choice(alphabet) for _ in range(length))


def get_random_string(length):
    return ''.join(choice(string.ascii_lowercase + string.digits) for _ in range(length))


def key_monitor(logger) -> Logger().logger:
    """
    :type logger: logging
    :return:
    """
    global stop_event, user_exit_request
    try:

        logger.info('Key monitor started')

        while not stop_event.is_set():
            try:
                key = input()  # waiting for input from user
                if key == 'q':
                    logger.warning('User Exit requested')
                    user_exit_request = True
                    stop_event.set()
            except EOFError:
                break
            time.sleep(1)

    except Exception as e:
        if stop_event.is_set():
            return 0
        logger.exception(e)
        return -1


def signal_handler_main(sig, frame):
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def init_creator_pool(filenum):
    global total_files
    total_files = filenum


def init_test(args, logger):
    global total_files, mounter
    try:
        if init_test.first_run is True:
            init_test.first_run = False
    except AttributeError:
        init_test.first_run = True

    if init_test.first_run:
        logger.info("Mounting work path...")
        mounter = Mounter(args.cluster, args.export, 'nfs3', 'CREATE_MOVE_DIR', logger=logger, nodes=0,
                          domains=0, sudo=True, start_vip=args.start_vip, end_vip=args.end_vip)
        try:
            mounter.mount_all_vips()
        except AttributeError:
            logger.warn("VIP range is bad or None. Falling back to mounting storage server IP")
            mounter.mount()

        # Starting key_monitor thread --- should be only one instance
        logger.info("Starting Key monitor --- Press q <Enter> to exit test")
        key_monitor_thread = threading.Thread(target=key_monitor, args=(logger,))
        key_monitor_thread.start()

    logger.info("Creating test folder on cluster %s" % args.cluster)
    mkdir_success = False
    while not mkdir_success:
        try:
            os.mkdir('{}/{}'.format(mounter.get_random_mountpoint(), args.test_dir))
            mkdir_success = True
        except (OSError, FileExistsError) as e:
            logger.error(e)
            args.test_dir = get_random_unicode(64)

    logger.info("Done Init, starting the test")


def file_creator_worker(path, proc_id, max_files):
    global total_files, file_create_lock, stop_event
    print("Starting file creator %s" % proc_id)
    try:
        while total_files.value < max_files and not stop_event.is_set():
            filenum = total_files.value
            print("Creating %s/file_created_client_#%d_file_number_#%d" % (
                path, proc_id, filenum))
            touch('%s/file_created_client_#%d_file_number_#%d' % (path, proc_id, filenum))
            print("### DEBUG: %s -- going to lock total_files" % proc_id)
            file_create_lock.acquire()
            print("### DEBUG: %s -- lock aquired on total_files" % proc_id)
            total_files.value += 1
            print("### DEBUG: %s -- going to release total_files" % proc_id)
            file_create_lock.release()
            print("### DEBUG: %s -- total_files released" % proc_id)
    except Exception:
        traceback.print_exc()
    if total_files.value >= max_files:
        print("%s -- Done Creating files! total: %d. Stop moving ..." % (int(total_files.value), proc_id))
        stop_event.set()


def file_creator(args, path, logger):
    global file_creator_pool, file_create_lock
    if not os.path.isdir(path):
        raise IOError("Base path not found: " + path)
    file_create_lock = multiprocessing.Manager().Lock()
    logger.info("write lock created %s for creating flies" % file_create_lock)
    filenum = multiprocessing.Manager().Value('i', 0)
    # Initialising process pool + thread safe "flienum" value
    file_creator_pool = multiprocessing.Pool(MAX_PROCESSES, initializer=init_creator_pool, initargs=(filenum,))

    # acquire the list of all paths inside base path
    for i in range(MAX_PROCESSES):
        file_creator_pool.apply_async(file_creator_worker, args=(path, i, args.files))
    file_creator_pool.close()


def renamer_worker(args, proc_id):
    global stop_event
    proc_name = 'process-%d' % proc_id
    while not stop_event.is_set():
        try:
            # Getting all file in folder
            mount_point = mounter.get_random_mountpoint()
            test_files = os.scandir("%s/%s" % (mount_point, args.test_dir))
            for test_file in test_files:
                if stop_event.is_set():
                    break

                if "created" in test_file.name:
                    new_file_name = test_file.name.replace('created', 'moved')
                    print(
                        "%s -- moving %s to %s at path %s/%s" % (
                            proc_name, test_file.name, new_file_name, mount_point, args.test_dir))
                    shutil.move("%s/%s/%s" % (mount_point, args.test_dir, test_file.name),
                                "%s/%s/%s" % (mounter.get_random_mountpoint(), args.test_dir, new_file_name))
                elif "moved" in test_file.name:
                    new_file_name = test_file.name.replace('moved', 'created')
                    print(
                        "%s -- renaming %s to %s at path %s/%s" % (
                            proc_name, test_file.name, new_file_name, mount_point, args.test_dir))
                    os.rename("%s/%s/%s" % (mount_point, args.test_dir, test_file.name),
                              "%s/%s/%s" % (mount_point, args.test_dir, new_file_name))

        except (OSError, IOError) as e:
            print("Error: \"{}\" in process: {} -- skipping ...".format(e, proc_name))
        else:
            raise RuntimeError
    print("Test stopped!")
    file_renamer_pool.terminate()
    file_creator_pool.terminate()


def run_test(args, logger, results_q):
    global stop_event, file_creator_pool, file_renamer_pool
    logger.info("Starting file creator workers ...")
    file_creator(args, "%s/%s" % (mounter.get_random_mountpoint(), args.test_dir), logger)
    futures = []
    rename_lock = multiprocessing.Manager().Lock()
    logger.info("write lock created %s for removing flies" % rename_lock)
    file_renamer_pool = multiprocessing.Pool(MAX_PROCESSES)
    # Starting rename workers in parallel
    logger.info("Starting renamer workers in parallel ...")
    for i in range(MAX_PROCESSES):
        futures.append(file_renamer_pool.apply_async(renamer_worker, args=(args, i)))
    file_renamer_pool.close()
    logger.info("Test running! Press CTRL + C to stop")
    file_renamer_pool.join()
    for future in futures:
        try:
            logger.info("{}".format(future.get()))
        except Exception as e:
            logger.exception(e)

    while not results_q.empty():
        q = results_q.get()
        if q is True:  # if 'True', there is a problem
            return q


def main():
    global stop_event, user_exit_request
    results_q = multiprocessing.Queue()
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export", help="NFS Export", default="/", type=str)
    parser.add_argument("-m", "--mount_point", help="Path to mountpoint", default="/mnt/test", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="test_dir", type=str)
    parser.add_argument("-n", "--files", help="Max files number to create", default=10000, type=int)
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    parser.add_argument("--scenario", help="Select desired scenario", choices=['domains', 'multidir'], type=str)
    args = parser.parse_args()

    # Capturing CTRL+C Event for clean exit
    signal.signal(signal.SIGINT, signal_handler_main)

    logger = Logger().logger
    logger.debug("Logger Initialised %s" % logger)

    while not user_exit_request:
        init_test(args, logger)

        run_test(args, logger, results_q)

        logger.info("Test completed, deleting files ....")
        mp = mounter.get_random_mountpoint()
        for the_file in os.scandir("%s/%s" % (mp, args.test_dir)):
            file_path = os.path.join("%s/%s" % (mp, args.test_dir), the_file.name)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                logger.exception(e)

        logger.info("All files deleted, checking that directory is empty....")
        try:
            os.rmdir("%s/%s" % (mounter.get_random_mountpoint(), args.test_dir))
        except OSError as ex:
            if ex.errno == errno.ENOTEMPTY:
                logger.error(ex)
                sys.exit(1)
        logger.info("Directory is Empty. Exiting...")
        # args.test_dir = get_random_string(255)
        args.test_dir = get_random_unicode(64)
        logger.info('Restarting test with new test directory {}'.format(args.test_dir))
        if not user_exit_request:
            stop_event = multiprocessing.Event()  # resetting stop event

    stop_event.is_set()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as stop_test_exception:
        print(" CTRL+C pressed. Stopping test....")
        stop_event.set()
    except Exception:
        traceback.print_exc()
    sys.exit(0)
