"""
author: samuels
"""

import argparse
import os
import threading
import traceback

import sys

import multiprocessing

import signal

import time

import errno
from random import randint, choice

from logger import Logger
from shell_utils import ShellUtils, FSUtils

MAX_PROCESSES = 16
MAX_FILES = 10000

files_queue = multiprocessing.Manager().Queue()
stop_event = multiprocessing.Event()
file_creator_pool = None
file_renamer_pool = None
file_create_lock = None
total_files = None
stopped_processes_count = None
user_exit_request = False


def key_monitor(logger):
    global stop_event, user_exit_request
    try:

        logger.info('Key monitor started')

        while not stop_event.is_set():
            try:
                key = raw_input()  # waiting for input from user
                if key == 'q':
                    logger.warning('User Exit requested')
                    user_exit_request = True
                    stop_event.set()
            except EOFError:
                break
            time.sleep(1)

    except Exception, e:
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
    global total_files
    logger.info("Setting passwordless SSH connection")
    ShellUtils.run_shell_script("/zebra/qa/qa-util-scripts/set-ssh-python", args.cluster, False)
    logger.info("Getting cluster params...")
    active_nodes = FSUtils.get_active_nodes_num(args.cluster)
    logger.debug("Active Nodes: %s" % active_nodes)
    domains = FSUtils.get_domains_num(args.cluster)
    logger.debug("FSD domains: %s" % domains)

    logger.info("Mounting  %s to %s" % (args.mount_point, args.export_dir))
    if not os.path.isfile('/' + args.mount_point):
        os.mkdir('/' + args.mount_point)
    if os.path.ismount(args.mount_point):
        ShellUtils.run_shell_command("umount", "-fl %s" % args.mount_point)
    ShellUtils.run_shell_command("mount", "-o nfsvers=3 %s:/%s %s" % (args.cluster, args.export_dir, args.mount_point))

    logger.info("Creating test folder on cluster %s" % args.cluster)
    try:
        os.mkdir('%s/%s' % (args.mount_point, args.test_dir))
    except OSError:
        logger.exception("")
    logger.info("Starting Key monitor --- Press q <Enter> to exit test")
    key_monitor_thread = threading.Thread(target=key_monitor, args=(logger,))
    key_monitor_thread.start()
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
            test_files = os.listdir("%s/%s" % (args.mount_point, args.test_dir))
            for test_file in test_files:
                if stop_event.is_set():
                    break

                if "create" in test_file:
                    new_file_name = test_file.replace('created', 'moved')
                    print(
                        "%s -- renaming %s to %s at path %s/%s" % (
                            proc_name, test_file, new_file_name, args.mount_point, args.test_dir))
                    os.rename("%s/%s/%s" % (args.mount_point, args.test_dir, test_file),
                              "%s/%s/%s" % (args.mount_point, args.test_dir, new_file_name))
                elif "moved" in test_file:
                    new_file_name = test_file.replace('moved', 'created')
                    print(
                        "%s -- renaming %s to %s at path %s/%s" % (
                            proc_name, test_file, new_file_name, args.mount_point, args.test_dir))
                    os.rename("%s/%s/%s" % (args.mount_point, args.test_dir, test_file),
                              "%s/%s/%s" % (args.mount_point, args.test_dir, new_file_name))

        except OSError:
            print("%s -- Can't find file, skipping ..." % proc_name)
        else:
            raise RuntimeError()
    print("Test stopped!")
    file_renamer_pool.terminate()
    file_creator_pool.terminate()


def run_test(args, logger, results_q):
    global stop_event, file_creator_pool, file_renamer_pool
    logger.info("Starting file creator workers ...")
    file_creator(args, "%s/%s" % (args.mount_point, args.test_dir), logger)
    p = None
    rename_lock = multiprocessing.Manager().Lock()
    logger.info("write lock created %s for removing flies" % rename_lock)
    file_renamer_pool = multiprocessing.Pool(MAX_PROCESSES)
    # Starting rename workers in parallel
    logger.info("Starting renamer workers in parallel ...")
    for i in range(MAX_PROCESSES):
        p = file_renamer_pool.apply_async(renamer_worker, args=(args, i))
    file_renamer_pool.close()
    logger.info("Test running! Press CTRL + C to stop")
    file_renamer_pool.join()

    while not stop_event.is_set():
        pass

    # p.get()

    while not results_q.empty():
        q = results_q.get()
        if q is True:  # if 'True', there is a problem
            return q


def main():
    global stop_event, user_exit_request
    results_q = multiprocessing.Queue()
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export_dir", help="NFS Export", default="vol0", type=str)
    parser.add_argument("-m", "--mount_point", help="Path to mountpoint", default="/mnt/test", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="test_dir", type=str)
    parser.add_argument("-n", "--files", help="Max files number to create", default=10000, type=int)
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
        for the_file in os.listdir("%s/%s" % (args.mount_point, args.test_dir)):
            file_path = os.path.join("%s/%s" % (args.mount_point, args.test_dir), the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                logger.exception(e)

        logger.info("All files deleted, checking that directory is empty....")
        try:
            os.rmdir("%s/%s" % (args.mount_point, args.test_dir))
        except OSError as ex:
            if ex.errno == errno.ENOTEMPTY:
                logger.error("directory is not empty!")
                sys.exit(1)
        logger.info("Directory is Empty. Exiting...")
        args.test_dir = "my_dir%d" % (randint(1, 100000))
        logger.info('Restarting test with new test directory %s ' % args.test_dir)
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
