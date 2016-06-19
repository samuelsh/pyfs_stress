"""
author: samuels
"""

import argparse
import os
import traceback

import sys

import multiprocessing

from logger import Logger
from shell_utils import ShellUtils, FSUtils

MAX_PROCESSES = 16
MAX_FILES = 10000

files_queue = multiprocessing.Manager().Queue()
stop_event = multiprocessing.Event()
dir_scanner_pool = None
total_files = None
stopped_processes_count = None


def init_scanner_pool(filenum):
    global total_files
    total_files = filenum


def init_test(args, logger):
    logger.info("Setting passwordless SSH connection")
    ShellUtils.run_shell_script("/zebra/qa/qa-util-scripts/set-ssh-python", args.cluster, False)
    logger.info("Getting cluster params...")
    active_nodes = FSUtils.get_active_nodes_num(args.cluster)
    logger.debug("Active Nodes: %s" % active_nodes)
    domains = FSUtils.get_domains_num(args.cluster)
    logger.debug("FSD domains: %s" % domains)

    logger.info("Mounting  %s to %s" % (args.mount_point, args.export_dir))
    ShellUtils.run_shell_command("mount", "-o nfsvers=3 %s:/%s %s" % (args.cluster, args.export_dir, args.mount_point))

    logger.info("Creating test folder on cluster %s" % args.cluster)
    ShellUtils.run_shell_command('mkdir' '%s/%s' % (args.mount_point, 'test_dir'))
    logger.info("Done Init, starting the test")


def file_creator_worker(path, proc_id, logger):
    global total_files
    while total_files < MAX_FILES:
        logger.info("Creating %s/file_created_client_#%d_file_number_total_files_#%d" % (
            path, proc_id, total_files))
        ShellUtils.run_shell_command('touch', '%s/file_created_client_#%d_file_number_total_files_#%d' % (
            path, proc_id, total_files))


def file_creator(path, logger):
    global dir_scanner_pool
    dir_scanner_pool = multiprocessing.Pool(MAX_PROCESSES)
    if not os.path.isdir(path):
        raise IOError("Base path not found: " + path)

    # acquire the list of all paths inside base path
    for i in range(MAX_PROCESSES):
        logger.info("Starting file creator process-%d" % i)
        dir_scanner_pool.apply_async(file_creator_worker, args=(path, i, logger))
    dir_scanner_pool.close()


def renamer_worker(args, logger, lock, i):
    while not stop_event.is_set():
        try:
            # Getting all file in folder
            files_list = os.listdir("%s/%s" % (args.mount_point, args.test_dir))
            for test_file in files_list:
                if "create" in test_file:
                    logger.info("renaming %s " % test_file)
                    new_file_name = test_file.replace('created', 'moved')
                    os.rename("%s/%s/%s" % (args.mount_point, args.test_dir, test_file),
                              "%s/%s/%s" % (args.mount_point, args.test_dir, new_file_name))
                elif "moved" in test_file:
                    logger.info("renaming %s " % test_file)
                    new_file_name = test_file.replace('moved', 'created')
                    os.rename("%s/%s/%s" % (args.mount_point, args.test_dir, test_file),
                              "%s/%s/%s" % (args.mount_point, args.test_dir, new_file_name))

        except Exception as rename_worker_exception:
            raise rename_worker_exception


def run_test(args, logger, results_q):
    logger("Starting file creator workers ...")
    file_creator("%s/%s" % (args.mount_point, args.test_dir))
    filenum = multiprocessing.Manager().Value('filenum', 0)
    lock = multiprocessing.Manager().Lock()
    process_pool = multiprocessing.Pool(MAX_PROCESSES, initializer=init_scanner_pool, initargs=(filenum,logger))
    p = None

    # Starting rename workers in parallel
    logger("Starting renamer workers in parallel ...")
    for i in range(MAX_PROCESSES):
        p = process_pool.apply_async(renamer_worker, args=(args, logger, lock, ("process-%d" % i)))
    p.get()

    logger.info("Test running! Press CTRL + C to stop")
    process_pool.close()
    process_pool.join()

    while not results_q.empty():
        q = results_q.get()
        if q is True:  # if 'True', there is a problem
            return q


def main():
    results_q = multiprocessing.Queue()
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-e", "--export_dir", help="NFS Export", default="vol0", type=str)
    parser.add_argument("-m", "--mount_point", help="Path to mountpoint", default="/mnt/test", type=str)
    parser.add_argument("-d", "--test_dir", help="Directory under test", default="test_dir", type=str)
    parser.add_argument("-n", "--files", help="Max files number to create", default=10000, type=int)
    parser.add_argument("--scenario", help="Select desired scenario", choice="", type=str)
    args = parser.parse_args()

    logger = Logger().logger
    logger.debug("Logger Initialised %s" % logger)

    init_test(args, logger)

    if run_test(args, logger, results_q) is True:
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as stop_test_exception:
        print(" Stopping test....")
        stop_event.set()
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
