"""
Byte range locking implementation for vfs_stress test suite - 2018 (c) samuel
"""
import json
import os
import errno
import socket
import xxhash


class LockException(OSError):
    pass


class FLock(object):
    def __init__(self, locking_db):
        self.locking_db = locking_db
        self.pid = os.getpid()
        self.host = socket.gethostname()

    def lock(self, fid, offset, length, flags=None, expiration=None):
        """

        :param fid: str
        :param offset: int
        :param length: int
        :param flags: str
        :param expiration: Date
        :return:
        """
        #  lock_id is unique hash of file_id + pid
        file_handle = os.fstat(fid).st_ino
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
        lock_id = xxhash.xxh64("".join(map(str, [fid, self.host, self.pid, offset, length]))).hexdigest()
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

    def release(self, fid, offset, length):
        """

        :param fid: str
        :param offset: int
        :param length: int
        :return:
        """
        lock_id = xxhash.xxh64("".join(map(str, [fid, self.host, self.pid, offset, length]))).hexdigest()
        self.locking_db.hdel(os.fstat(fid).st_ino, lock_id)


def is_overlap(start1, end1, start2, end2):
    return end1 >= start2 and end2 >= start1
