from timeit import default_timer as timer

import xxhash
import os
import random
import shutil

import errno
import fcntl
# import data_operations.data_generators
import sys

import mmap

sys.path.append('/qa/dynamo')
from config import error_codes

__author__ = "samuels"

MAX_DIR_SIZE = 128 * 1024
INLINE = INLINE_MAX_SIZE = 3499
KB1 = 1024
KB4 = KB1 * 4
MB1 = (1024 * 1024)
GB1 = (1024 * 1024 * 1024)
TB1 = (1024 * 1024 * 1024 * 1024)
MB512 = (MB1 * 512)  # level 1 can map up to 512MB
GB256 = (GB1 * 256)  # level 2 can map up to 256GB
GB512 = (GB1 * 512)  # level 2 can map up to 256GB
TB128 = (TB1 * 128)  # level 3 can map up to 128TB
ZERO_PADDING_START = 128 * MB1  # 128MB
MAX_FILE_SIZE = TB1 + ZERO_PADDING_START
DATA_PATTERN_A = {'pattern': b'A', 'repeats': 1, 'checksum': xxhash.xxh64(b'A' * 1).hexdigest()}
DATA_PATTERN_B = {'pattern': b'B', 'repeats': 3, 'checksum': xxhash.xxh64(b'B' * 3).hexdigest()}
DATA_PATTERN_C = {'pattern': b'C', 'repeats': 17, 'checksum': xxhash.xxh64(b'C' * 17).hexdigest()}
DATA_PATTERN_D = {'pattern': b'D', 'repeats': 33, 'checksum': xxhash.xxh64(b'D' * 33).hexdigest()}
DATA_PATTERN_E = {'pattern': b'E', 'repeats': 65, 'checksum': xxhash.xxh64(b'E' * 65).hexdigest()}
DATA_PATTERN_F = {'pattern': b'F', 'repeats': 129, 'checksum': xxhash.xxh64(b'F' * 129).hexdigest()}
DATA_PATTERN_G = {'pattern': b'G', 'repeats': 257, 'checksum': xxhash.xxh64(b'G' * 257).hexdigest()}
DATA_PATTERN_H = {'pattern': b'H', 'repeats': 1025, 'checksum': xxhash.xxh64(b'H' * 1025).hexdigest()}
DATA_PATTERN_J = {'pattern': b'J', 'repeats': 64 * KB1 + 1, 'checksum': xxhash.xxh64(b'J' * (64 * KB1 + 1)).hexdigest()}
DATA_PATTERN_I = {'pattern': b'I', 'repeats': 128 * KB1 + 1,
                  'checksum': xxhash.xxh64(b'I' * (128 * KB1 + 1)).hexdigest()}
DATA_PATTERN_K = {'pattern': b'K', 'repeats': 256 * KB1 + 1,
                  'checksum': xxhash.xxh64(b'K' * (256 * KB1 + 1)).hexdigest()}
DATA_PATTERN_L = {'pattern': b'L', 'repeats': 512 * KB1 + 1,
                  'checksum': xxhash.xxh64(b'L' * (512 * KB1 + 1)).hexdigest()}
DATA_PATTERN_M = {'pattern': b'M', 'repeats': MB1 + 1, 'checksum': xxhash.xxh64(b'M' * (MB1 + 1)).hexdigest()}

PADDING = [0, ZERO_PADDING_START]
OFFSETS_LIST = [0, INLINE, KB1, KB4, MB1, MB512, GB1, GB256, GB512, TB1]
DATA_PATTERNS_LIST = [DATA_PATTERN_A, DATA_PATTERN_B, DATA_PATTERN_C, DATA_PATTERN_D, DATA_PATTERN_E, DATA_PATTERN_F,
                      DATA_PATTERN_G, DATA_PATTERN_H, DATA_PATTERN_I, DATA_PATTERN_J, DATA_PATTERN_K, DATA_PATTERN_L,
                      DATA_PATTERN_M]


class DynamoException(EnvironmentError):
    pass


class DataPatterns:
    def __init__(self):
        self.data_patterns_dict = {}

        for _ in range(1000):
            pass


def profiler(f):
    """
    Profiler decorator ot measure duration of file operations
    """

    def wrapper(action, mount_point, incoming_data, **kwargs):
        start = timer()
        response = f(action, mount_point, incoming_data, **kwargs)
        end = timer()
        response['duration'] = end - start
        return response

    return wrapper


@profiler
def response_action(action, mount_point, incoming_data, **kwargs):
    return {
        "mkdir": mkdir,
        "list": list_dir,
        "delete": delete,
        "touch": touch,
        "stat": stat,
        "read": read,
        "write": write,
        "rename": rename,
        "rename_exist": rename_exist,
        "truncate": truncate
    }[action](mount_point, incoming_data, **kwargs)


def mkdir(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    os.mkdir('/'.join([mount_point, incoming_data['target']]))
    outgoing_data['dirsize'] = 0  # os.stat('/'.join([mount_point, incoming_data['target']])).st_size
    return outgoing_data


def list_dir(mount_point, incoming_data, **kwargs):
    os.listdir(''.join([mount_point, incoming_data['target']]))


def delete(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    flock = kwargs['flock']
    dirpath = incoming_data['target'].split('/')[1]
    fname = incoming_data['target'].split('/')[2]
    f_path = ''.join([mount_point, incoming_data['target']])
    with open(f_path, 'rb') as fp:
        flock.release(fp.fileno(), 0, os.path.getsize(f_path))
    os.remove('/'.join([mount_point, dirpath, fname]))
    outgoing_data['uuid'] = incoming_data['uuid']


def touch(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    # File will be only created if not exists otherwise EEXIST error returned
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(''.join([mount_point, incoming_data['target']]), flags)
    os.write(fd, b'\0')
    os.fsync(fd)
    os.close(fd)
    outgoing_data['dirsize'] = 4096  # This field is deprecated since we're counting dir size on server side
    # outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def stat(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    os.stat(''.join([mount_point, incoming_data['target']]))
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def read(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    flock = kwargs['flock']
    offset = incoming_data['offset']
    chunk_size = incoming_data['repeats']
    f_path = ''.join([mount_point, incoming_data['target']])
    with open(f_path, 'rb') as f:
        f.seek(offset)
        flock.lock(f.fileno(), incoming_data['offset'], incoming_data['repeats'])
        buf = f.read(chunk_size)
        flock.release(f.fileno(), incoming_data['offset'], incoming_data['repeats'])
        outgoing_data['hash'] = xxhash.xxh64(buf).hexdigest()
        outgoing_data['offset'] = offset
        outgoing_data['chunk_size'] = incoming_data['repeats']
        outgoing_data['uuid'] = incoming_data['uuid']
        # outgoing_data['buffer'] = buf[:256].decode()
        return outgoing_data


def write(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    io_mode = 'rb+'
    flock = kwargs['flock']
    if incoming_data['io_type'] == 'sequential':
        offset = incoming_data['offset'] + incoming_data['data_pattern_len']
    else:
        padding = random.choice(PADDING)
        base_offset = random.choice(OFFSETS_LIST) + padding
        offset = base_offset + random.randint(base_offset, MAX_FILE_SIZE)
    data_pattern = random.choice(DATA_PATTERNS_LIST)
    pattern_to_write = data_pattern['pattern'] * data_pattern['repeats']
    data_hash = data_pattern['checksum']
    file_path = ''.join([mount_point, incoming_data['target']])
    if not os.path.exists(file_path):
        io_mode = 'w+b'
    with open(file_path, io_mode) as f:
        # fcntl.lockf(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, data_pattern['repeats'], offset, 0)
        flock.lock(f.fileno(), offset, data_pattern['repeats'])
        f.seek(offset)
        f.write(pattern_to_write)
        f.flush()
        os.fsync(f.fileno())
        #  Checking if original data pattern and pattern on disk are the same
        f.seek(offset)
        buf = f.read(data_pattern['repeats'])
        hasher = xxhash.xxh64()
        hasher.update(buf)
        read_hash = hasher.hexdigest()
        if read_hash != data_hash:
            outgoing_data['dynamo_error'] = error_codes.HASHERR
            outgoing_data['bad_hash'] = read_hash
        # fcntl.lockf(fp.fileno(), fcntl.LOCK_UN)
        flock.release(f.fileno(), offset, data_pattern['repeats'])
    outgoing_data['data_pattern'] = data_pattern['pattern'].decode()
    outgoing_data['chunk_size'] = data_pattern['repeats']
    outgoing_data['hash'] = data_hash
    outgoing_data['offset'] = offset
    outgoing_data['uuid'] = incoming_data['uuid']
    outgoing_data['io_type'] = incoming_data['io_type']
    return outgoing_data


def rename(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    fullpath = incoming_data['target'].split('/')[1:]
    dirpath = fullpath[0]
    fname = fullpath[1]
    dst_mount_point = kwargs['dst_mount_point']
    outgoing_data['rename_dest'] = incoming_data['rename_dest']
    os.rename('/'.join([mount_point, dirpath, fname]),
              '/'.join([dst_mount_point, dirpath, incoming_data['rename_dest']]))
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def rename_exist(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    src_path = incoming_data['rename_source']
    dst_path = incoming_data['rename_dest']
    src_dirpath = src_path.split('/')[1]
    src_fname = src_path.split('/')[2]
    dst_dirpath = dst_path.split('/')[1]
    dst_fname = dst_path.split('/')[2]
    if src_fname == dst_fname:
        raise DynamoException(error_codes.SAMEFILE, "Error: Trying to move file into itself.", src_path)
    dst_mount_point = kwargs['dst_mount_point']
    shutil.move('/'.join([mount_point, src_dirpath, src_fname]),
                '/'.join([dst_mount_point, dst_dirpath, dst_fname]))
    outgoing_data['rename_source'] = src_path
    outgoing_data['rename_dest'] = dst_path
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def truncate(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    flock = kwargs['flock']
    padding = random.choice(PADDING)
    offset = random.choice(OFFSETS_LIST) + padding
    fp = None
    try:
        fp = open(''.join([mount_point, incoming_data['target']]), 'r+b')
        # fcntl.lockf(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        # flock.lock(fp.fileno(), 0, os.fstat(fp.fileno()).st_size)
        fp.truncate(offset)
        fp.flush()
        os.fsync(fp.fileno())
        # fcntl.lockf(fp.fileno(), fcntl.LOCK_UN)
        # flock.release(fp.fileno(), 0, os.fstat(fp.fileno()).st_size)
        fp.close()
    except (IOError, OSError) as env_error:
        if fp:
            try:
                fcntl.lockf(fp.fileno(), fcntl.LOCK_UN)
            except OSError as os_err:
                if os_err.errno == errno.ENOLCK:
                    pass
                else:
                    fp.close()
                    raise os_err
            fp.close()
        raise env_error
    outgoing_data['size'] = offset
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def read_direct(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    flock = kwargs['flock']
    fd = None
    try:
        f_path = "{0}{1}".format(mount_point, incoming_data['target'])
        fd = os.open(f_path, os.O_RDONLY | os.O_DIRECT)
        mmap_buf = mmap.mmap(fd, 0, prot=mmap.PROT_READ)
        offset = incoming_data['offset']
        chunk_size = incoming_data['repeats']
        mmap_buf.seek(offset)
        flock.lock(fd, incoming_data['offset'], incoming_data['repeats'])
        buf = mmap_buf.read(chunk_size)
        flock.release(fd, incoming_data['offset'], incoming_data['repeats'])
        os.close(fd)
    except (IOError, OSError) as env_error:
        if fd:
            os.close(fd)
        raise env_error
    hasher = xxhash.xxh64()
    hasher.update(buf)
    outgoing_data['hash'] = hasher.hexdigest()
    outgoing_data['offset'] = incoming_data['offset']
    outgoing_data['chunk_size'] = incoming_data['repeats']
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def write_direct(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    hasher = xxhash.xxh64()
    fp = None
    if incoming_data['io_type'] == 'sequential':
        offset = incoming_data['offset'] + incoming_data['data_pattern_len']
    else:
        padding = random.choice(PADDING)
        base_offset = random.choice(OFFSETS_LIST) + padding
        offset = base_offset + random.randint(base_offset, MAX_FILE_SIZE)
    data_pattern = random.choice(DATA_PATTERNS_LIST)
    pattern_to_write = data_pattern['pattern'] * data_pattern['repeats']
    hasher.update(pattern_to_write)
    data_hash = hasher.hexdigest()
    try:
        fp = os.open("{0}{1}".format(mount_point, incoming_data['target']), os.O_WRONLY | os.O_DIRECT)
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB, data_pattern['repeats'], offset, 0)
        os.lseek(fp, offset, os.SEEK_SET)
        pattern_len = data_pattern['repeats']
        aligned_pattern_len = pattern_len if not pattern_len % 2 else pattern_len + 1  # write pattern needs to be
        #  stored in allgned memory buffer
        mmap_buf = mmap.mmap(-1, int(aligned_pattern_len), prot=mmap.PROT_WRITE)
        mmap_buf.write(data_pattern['pattern'] * aligned_pattern_len)
        os.write(fp, mmap_buf)
        os.fsync(fp)
        #  Checking if original data pattern and pattern on disk are the same
        os.lseek(fp, offset, os.SEEK_SET)
        buf = os.read(fp, data_pattern['repeats'])
        hasher = xxhash.xxh64()
        hasher.update(buf)
        read_hash = hasher.hexdigest()
        if read_hash != data_hash:
            outgoing_data['dynamo_error'] = error_codes.HASHERR
            outgoing_data['bad_hash'] = read_hash
        fcntl.lockf(fp, fcntl.LOCK_UN)
        os.close(fp)
    except (IOError, OSError) as env_error:
        if fp:
            try:
                fcntl.lockf(fp, fcntl.LOCK_UN)
            except OSError as os_err:
                if os_err.errno == errno.ENOLCK:
                    pass
                else:
                    os.close(fp)
                    raise os_err
            os.close(fp)
        raise env_error
    outgoing_data['data_pattern'] = data_pattern['pattern']
    outgoing_data['chunk_size'] = data_pattern['repeats']
    outgoing_data['hash'] = data_hash
    outgoing_data['offset'] = offset
    outgoing_data['uuid'] = incoming_data['uuid']
    outgoing_data['io_type'] = incoming_data['io_type']
    return outgoing_data
