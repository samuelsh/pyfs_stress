#!/usr/bin/python2.6

"""
Created on Jan 29, 2015

@author: samuels
"""
from hashlib import md5
import sys
import os
import os.path
import threading
import queue
import logging
import traceback
import argparse
#import hanging_threads

WORKERS = 1
MAX_HASH_WORKER_THREADS = 25
MAX_REDAHEAD_BUF_SIZE = 1024 * 1024  # 1 MB Readahead buffer
BLOCK_SIZE = 1024 * 1024  # 1MB
CHECKSUM_FILE = "checksum.dat"
readahead_buf = b''  # Preallocating readahead buffer
hash_buf = readahead_buf


def initialize_logger(output_dir):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create error file handler and set level to error
    handler = logging.FileHandler(os.path.join(output_dir, "error.log"), "w", encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler(os.path.join(output_dir, "general.log"), "w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def handle_error():
    traceback.print_stack()


class Bcolors:
    def __init__(self):
        pass

    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class FileHasherWorker(threading.Thread):
    def __init__(self, stop_event, tree_walker_thread, data_queue, status_queue, hasher, logger=None, blocksize=65536):
        threading.Thread.__init__(self)
        self.blocksize = blocksize
        self.tree_walker_thread = tree_walker_thread
        self.data_queue = data_queue
        self.status_queue = status_queue
        self.stop_event = stop_event
        self.inner_offset = 0
        self.logger = logger or logging.getLogger(__name__)
        self.exception = None

    def run(self):
        if self.tree_walker_thread.isAlive():
            self.logger.debug("%s is alive", self.tree_walker_thread.getName())
            while self.tree_walker_thread.isAlive():
                self.logger.debug("%s Items in queue: %d", self.getName(), self.data_queue.qsize())
                if not self.data_queue.empty():
                    break

            while not self.data_queue.empty():

                try:
                    node = self.data_queue.get_nowait()
                    if node is None:
                        self.data_queue.task_done()
                        break
                    self.logger.debug("%s Getting the node: %s", self.getName(), str(node))

                    if CHECKSUM_FILE in node.files:  # if checksum file already exists
                        node.files.remove(CHECKSUM_FILE)  # removing checksum file from a file list
                        if os.path.isfile(node.path + os.sep + CHECKSUM_FILE):
                            self.logger.debug("%s is removing old checksum file at %s", self.getName(), node.path
                                              + os.sep + CHECKSUM_FILE)
                            os.remove(node.path + os.sep + CHECKSUM_FILE)

                    for the_file in node.files:
                        # self.logger.debug(the_file)
                        hasher = md5()  # new hashing object     
                        file_path = node.path + os.sep + str(the_file)
                        try:
                            with open(file_path, 'rb', self.blocksize) as f:
                                self.logger.debug("%s::%s starting File Reader at path: %s ", self.__class__.__name__,
                                                  self.getName(), file_path)

                                if os.stat(file_path).st_size != 0:
                                    buf = f.read(MAX_REDAHEAD_BUF_SIZE)

                                    while buf:
                                        hasher.update(buf)
                                        buf = f.read(MAX_REDAHEAD_BUF_SIZE)

                            self.logger.info("%s::%s -- Done writing hash of %s", self.__class__.__name__,
                                             self.getName(),
                                             file_path)

                            with open(node.path + os.sep + CHECKSUM_FILE, "a+t") as f:
                                f.write(file_path + " " + hasher.hexdigest() + "\n")

                        except Exception as e1:
                            self.logger.exception(e1)
                            self.status_queue.put(e1)
                            #self.stop_event.set()

                    self.logger.info("%s::%s -- Task finished. Items, still in queue: %d", self.__class__.__name__,
                                     self.getName(), self.data_queue.qsize())
                    self.data_queue.task_done()

                except queue.Empty:
                    pass

        self.logger.debug("%s is finished", self.getName())

        # self.queue.task_done()

    #    def join(self, timeout=None):
    #        if not self.stop_event.set():
    #            super(FileHasherWorker, self).join(timeout)

    def get_status_queue(self):
        return self.status_queue

    def get_exception(self):
        return self.exception


class DirectoryTreeWorker(threading.Thread):
    def __init__(self, stop_event, data_queue, comm_queue, path, logger=None):
        threading.Thread.__init__(self)
        self.data_queue = data_queue
        self.comm_queue = comm_queue
        self.path = path
        self.stop_event = stop_event
        self.logger = logger or logging.getLogger(__name__)

    def run(self):
        startinglevel = self.path.count(os.sep)
        for path, dirs, files in os.walk(self.path):
            if self.stop_event.is_set():
                self.logger.error("Program unexpectedly aborted...exiting thread")
                return

            depth = path.count(os.sep) - startinglevel
            node = Node(path, depth, dirs, files)
            self.logger.debug(node.path)
            self.logger.debug(node.depth)
            self.logger.debug(node.dirs)
            self.logger.debug(node.files)
            self.data_queue.put(node)
            self.logger.debug("%s::%s - Items in queue %d", self.__class__.__name__, self.getName(),
                              self.data_queue.qsize())
        self.logger.info(" %s::%s is finished", self.__class__.__name__, self.getName())

        # Object that signals shutdown
        _sentinel = None
        self.data_queue.put(_sentinel)


class FileHasherCheckerWorker(threading.Thread):
    def __init__(self, stop_event, tree_walker_thread, data_queue, status_queue, hasher, logger=None, blocksize=65536):
        threading.Thread.__init__(self)
        self.blocksize = blocksize
        self.tree_walker_thread = tree_walker_thread
        self.data_queue = data_queue
        self.status_queue = status_queue
        self.stop_event = stop_event
        self.inner_offset = 0
        self.logger = logger or logging.getLogger(__name__)
        self.exception = None

    def run(self):

        if self.tree_walker_thread.isAlive():
            self.logger.debug("%s is alive", self.tree_walker_thread.getName())
            while self.tree_walker_thread.isAlive():
                self.logger.debug("%s Items in queue: %d", self.getName(), self.data_queue.qsize())
                if not self.data_queue.empty():
                    break

            while not self.data_queue.empty():

                try:
                    node = self.data_queue.get_nowait()
                    if node is None:
                        self.data_queue.task_done()
                        break

                    self.logger.debug("%s Getting the node: %s", self.getName(), str(node))

                    if CHECKSUM_FILE not in node.files:
                        # we will report error if there's no checksum.dat file in folder
                        self.logger.error("Thread %s::%s -- Checksum file is missing at %s", self.__class__.__name__,
                                          self.getName(), node.path)

                    else:
                        for the_file in node.files:
                            if the_file == CHECKSUM_FILE:  # Skipping checksum.dat itself
                                continue
                            hasher = md5()  # new hashing object
                            file_path = node.path + os.sep + str(the_file)
                            try:
                                with open(file_path, 'rb', self.blocksize) as f:
                                    self.logger.debug("%s starting File Reader at path: %s ", self.getName(), file_path)

                                    if os.stat(file_path).st_size != 0:
                                        buf = f.read(MAX_REDAHEAD_BUF_SIZE)

                                        while buf:
                                            hasher.update(buf)
                                            buf = f.read(MAX_REDAHEAD_BUF_SIZE)

                                self.logger.debug("%s::%s -- Done calculating hash of %s", self.__class__.__name__,
                                                  self.getName(), file_path)

                                with open(node.path + os.sep + CHECKSUM_FILE, "r") as f:
                                    for line in f:
                                        checksum_data = line.split(" ")  # Splitting filename and checksum
                                        stored_file = checksum_data[0].split('\\')
                                        #checksum_data[0] = checksum_data[0][-1]  # getting last element in the path
                                        if the_file in stored_file:
                                            self.logger.debug("File %s is found", checksum_data[0])
                                            self.logger.debug(
                                                "%s::%s -- Comparing file on disk %s with stored checksum of %s",
                                                self.__class__.__name__, self.getName(), file_path, checksum_data[0])
                                            if hasher.hexdigest() == checksum_data[1].strip():
                                                self.logger.info('%s md5 is' + Bcolors.OK_GREEN + ' OK' + Bcolors.ENDC,
                                                                 file_path)
                                                break
                                            else:
                                                self.logger.error('%s md5 is' + Bcolors.FAIL + ' FAILED' + Bcolors.ENDC,
                                                                  file_path)
                                                self.logger.info('Calculated hash is %s -- Stored hash is %s',
                                                                 hasher.hexdigest(), checksum_data[1].strip())
                                                break
                                    else:
                                        self.logger.error(Bcolors.FAIL + '%s not found! -- stored file: %s' + Bcolors.ENDC,
                                                          file_path, stored_file)

                            except Exception as e1:
                                self.logger.exception(e1)
                                self.status_queue.put_nowait(e1)
                                #self.stop_event.set()

                    self.logger.info("%s::%s -- Task is finished. Items, still in queue: %d", self.__class__.__name__,
                                     self.getName(), self.data_queue.qsize())
                    self.data_queue.task_done()

                except queue.Empty:
                    pass

        self.logger.debug("%s::%s is finished", self.__class__.__name__, self.getName())


class Node:
    path = ""
    depth = ""
    dirs = []
    files = []

    def __init__(self, path, depth, dirs, files):
        self.path = path
        self.depth = depth
        self.dirs = dirs
        self.files = files


def main():
    # logging.basicConfig(level=logging.INFO)
    # logger = logging.getLogger(__name__)
    # logging.Handler.handleError = handle_error
    logger = initialize_logger("")
    stop_event = threading.Event()

    parser = argparse.ArgumentParser(description='Extended md5sum (c) Dell 2015.')
    parser.add_argument('path', help='Path to parent folder')
    parser.add_argument('--do_verify', help='flags to perform MD5 validity check', action="store_true")
    args = parser.parse_args()

    filepath = args.path
    filepath = filepath.rstrip(os.sep)  # removing last slash from psth to prevent double '//' in path names
    do_verify = args.do_verify

    if not filepath:
        raise Exception("Please enter path to parent folder")
    if not os.path.isdir(filepath):
        raise Exception("Path %s not found", filepath)

    data_queue = queue.Queue()  # queue for passing data from DirectoryTreeWorker thread to FileHasherWorker threads
    status_queue = queue.Queue()  # queue for passing statuses form FileHasherWorker to DirectoryTreeWorker

    # Find floders in path and push 'em to queue
    tree_walker = DirectoryTreeWorker(stop_event, data_queue, status_queue, filepath, logger)
    logger.info('Starting %s::%s execution....', tree_walker.__class__.__name__, tree_walker.getName())
    tree_walker.start()

    if not do_verify:  # starting hasher threads
        # noinspection PyUnusedLocal
        fileHashWorkers = [FileHasherWorker(stop_event, tree_walker, data_queue, status_queue, BLOCK_SIZE, logger) for i
                           in

                           range(MAX_HASH_WORKER_THREADS)]
        logger.debug("Total items in queue: %d", data_queue.qsize())
        for i in range(MAX_HASH_WORKER_THREADS):
            logger.info("Starting %s::%s execution...", fileHashWorkers[i].__class__.__name__,
                        fileHashWorkers[i].getName())
            fileHashWorkers[i].start()

        for i in range(MAX_HASH_WORKER_THREADS):
            fileHashWorkers[i].join()

        data_queue.join()

        for i in range(status_queue.qsize()):
            error_status = status_queue.get()
            logger.exception(error_status)
            if e:
                raise e

    else:  # We'll compare files to stored checksums
        # noinspection PyUnusedLocal
        fileHashCheckerWorkers = [FileHasherCheckerWorker(stop_event, tree_walker, data_queue, status_queue, BLOCK_SIZE,
                                                          logger) for i in range(MAX_HASH_WORKER_THREADS)]
        logger.info("Total items in queue: %d", data_queue.qsize())
        for i in range(MAX_HASH_WORKER_THREADS):
            logger.info("Starting %s::%s execution...", fileHashCheckerWorkers[i].__class__.__name__,
                        fileHashCheckerWorkers[i].getName())
            fileHashCheckerWorkers[i].start()

        for i in range(MAX_HASH_WORKER_THREADS):
            fileHashCheckerWorkers[i].join()

        data_queue.join()

        for i in range(status_queue.qsize()):
            e2 = status_queue.get_nowait()
            logger.exception(e2)
            if e2:
                raise e2


if __name__ == '__main__':

    try:
        main()
        print("Test completed")
    except Exception as e:
        print(e)
        sys.exit(1)
