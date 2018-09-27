"""
Byte range locking implementation for vfs_stress test suite - 2018 (c) samuel
"""
import fcntl

import json
import os
import errno
import socket
import xxhash
from enum import Enum


class LockType(Enum):
    EXCLUSIVE = fcntl.LOCK_EX
    SHARED = fcntl.LOCK_SH
    EXCLUSIVE_NB = fcntl.LOCK_EX | fcntl.LOCK_NB
    SHARED_NB = fcntl.LOCK_SH | fcntl.LOCK_NB
    UNLOCK = fcntl.LOCK_UN


class LockException(OSError):
    pass


class FLock(object):
    def __init__(self, locking_db, locking_type="native"):
        self.locking_db = locking_db
        self.pid = os.getpid()
        self.host = socket.gethostname()

        if locking_type == "native":
            self.lockf = fcntl.lockf
        elif locking_type == "application":
            self.lockf = self._lock
        else:
            self.lockf = self._lock_stub

    def _lock(self, fd, lock_type, length, offset, whence=0, flags=None, expiration=None):
        """

        :param fid: str
        :param offset: int
        :param length: int
        :param flags: str
        :param expiration: Date
        :return:
        """
        #  lock_id is unique hash of file_id + pid
        file_handle = os.fstat(fd).st_ino
        locks_dict = self.locking_db.hgetall(file_handle)
        if locks_dict:  # there's already lock on same file by other process, lets check if there's ranges collision
            for entry in locks_dict.values():
                lock = json.loads(entry.decode())
                """
                Does the range (offset1, length1) overlap with (offset2, length2)?
                https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap/325964#325964
                """
                if is_overlap(offset, length, lock['offset'], lock['length']):
                    raise LockException(errno.EAGAIN, "Resource temporarily unavailable")
        # No overlapping locks found. Locking the file
        lock_id = xxhash.xxh64("".join(map(str, [fd, self.host, self.pid, offset, length]))).intdigest()
        self.locking_db.hmset(file_handle, {
            lock_id: json.dumps({
                "host": self.host,
                "pid": self.pid,
                "offset": offset,
                "length": length,
                "flags": flags,
                "expiration": expiration
            })
        })

    def release(self, fid, length, offset):
        """

        :param fid: str
        :param offset: int
        :param length: int
        :return:
        """
        lock_id = xxhash.xxh64("".join(map(str, [fid, self.host, self.pid, offset, length]))).intdigest()
        self.locking_db.hdel(os.fstat(fid).st_ino, lock_id)

    def _lock_stub(self, fd, lock_type, length, offset, whence=0):
        pass


def is_overlap(start1, end1, start2, end2):
    return end1 >= start2 and end2 >= start1
