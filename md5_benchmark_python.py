#!/usr/bin/python2.6

"""
Created on Jan 29, 2015

@author: samuels
"""
import datetime
import heapq
import os
import platform
import threading
import time
import xxhash
from hashlib import md5
from sys import stdout
import pyhashxx

WORKERS = 16


class Worker(threading.Thread):
    def __init__(self, heap, pindex, test_file, worker_offset, bytes_to_read, hasher, blocksize=65536):
        threading.Thread.__init__(self)
        self.heap = heap
        self.test_file = test_file
        self.worker_offset = worker_offset
        self.bytes_to_read = bytes_to_read
        self.blocksize = blocksize
        self.hasher = hasher
        self.stop_event = threading.Event()
        self.pindex = pindex

    def run(self):
        with open(self.test_file, 'rb') as f:
            print "Starting Worker ", self.getName()
            # buf = self.test_file.read(self.blocksize)
            buffers = []
            remain_bytes = self.bytes_to_read % self.blocksize
            total_read = 0
            f.seek(self.worker_offset)

            while (True):
                # f.seek(self.worker_offset + total_read)
                buf = f.read(self.blocksize)
                buffers.append(buf)
                # self.hasher.update(buf)
                pos = f.tell()
                if pos >= (self.worker_offset + self.bytes_to_read - remain_bytes):
                    break
            buf = f.read(remain_bytes)
            buffers.append(buf)
            heapq.heappush(self.heap, (self.pindex, b''.join(buffers)))
            print "Worker ", self.getName(), " is finished -- ", f.tell(), " -- offset: ", self.worker_offset + self.bytes_to_read


def python_hash(afile, hasher, blocksize=65536):
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
        # print "Read %d MB" % (afile.tell() / 1024 / 1024)
    return hasher.hexdigest()


def init_offsets(filesize):
    work_range = filesize / WORKERS
    workers_work_ranges_offsets = [i * work_range for i in range(WORKERS)]

    return workers_work_ranges_offsets


def main():
    if (platform.system() == "Windows"):
        filepath = "\\\\vip1.f5\\vol0slash\\test.dd"
    elif (platform.system() == "Linux"):
        filepath = "/mnt/test/not_aligned"
    else:
        raise Exception("Can't detect your OS!")

    blocksize = 256 * 1024
    filesize = os.path.getsize(filepath)
    md5hasher = md5()
    xxhasher = xxhash.xxh64()
    heap = []
    workers_offsets = init_offsets(filesize)
    hasher = md5hasher

    Workers = [Worker(heap, i, filepath, workers_offsets[i], filesize / WORKERS, hasher, blocksize) for i in
               range(WORKERS)]
    print Workers

    start = time.time()
    for i in range(WORKERS):
        Workers[i].start()

    for i in range(WORKERS):
        Workers[i].join()

    for i in range(WORKERS):
        priority, data = heapq.heappop(heap)
        hasher.update(data)

    end = time.time()
    print "MD5 took: %s " % str(datetime.timedelta(seconds=int(end - start)))
    print hasher.hexdigest()

    print workers_offsets


if __name__ == '__main__':

    try:
        main()
    except KeyboardInterrupt, e:
        stdout.flush()
        print 'Bye Bye'
