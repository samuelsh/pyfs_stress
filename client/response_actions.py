import hashlib
import os
import random
import shutil

import errno
import fcntl
# import data_operations.data_generators
import sys

from utils import shell_utils

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
DATA_PATTERN_A = {'pattern': 'A', 'repeats': 1}
DATA_PATTERN_B = {'pattern': 'B', 'repeats': 3}
DATA_PATTERN_C = {'pattern': 'C', 'repeats': 17}
DATA_PATTERN_D = {'pattern': 'D', 'repeats': 33}
DATA_PATTERN_E = {'pattern': 'E', 'repeats': 65}
DATA_PATTERN_F = {'pattern': 'F', 'repeats': 129}
DATA_PATTERN_G = {'pattern': 'G', 'repeats': 257}
DATA_PATTERN_H = {'pattern': 'H', 'repeats': 1025}
DATA_PATTERN_I = {'pattern': 'I', 'repeats': 128 * KB1 + 1}
DATA_PATTERN_J = {'pattern': 'J', 'repeats': 64 * KB1 + 1}

PADDING = [0, ZERO_PADDING_START]
OFFSETS_LIST = [0, INLINE, KB1, KB4, MB1, MB512, GB1, GB256, GB512, TB1]
DATA_PATTERNS_LIST = [DATA_PATTERN_A, DATA_PATTERN_B, DATA_PATTERN_C, DATA_PATTERN_D, DATA_PATTERN_E, DATA_PATTERN_F,
                      DATA_PATTERN_G, DATA_PATTERN_H, DATA_PATTERN_I, DATA_PATTERN_J]


class DynamoException(EnvironmentError):
    pass


class DataPatterns:
    def __init__(self):
        self.data_patterns_dict = {}

        for _ in range(1000):
            pass


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
    os.mkdir("{0}/{1}".format(mount_point, incoming_data['target']))
    outgoing_data['dirsize'] = os.stat("{0}/{1}".format(mount_point, incoming_data['target'])).st_size
    return outgoing_data


def list_dir(mount_point, incoming_data, **kwargs):
    os.listdir('{0}/{1}'.format(mount_point, incoming_data['target']))


def delete(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    dirpath = incoming_data['target'].split('/')[1]
    fname = incoming_data['target'].split('/')[2]
    os.remove('{0}/{1}/{2}'.format(mount_point, dirpath, fname))
    outgoing_data['uuid'] = incoming_data['uuid']


def touch(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    dirsize = os.stat("{0}/{1}".format(mount_point, incoming_data['target'].split('/')[1])).st_size
    if dirsize > MAX_DIR_SIZE:  # if Directory entry size > 128K, we'll stop writing new files
        outgoing_data['target_path'] = incoming_data['target']
        raise DynamoException(error_codes.MAX_DIR_SIZE, "Directory Entry reached {0} size limit".format(MAX_DIR_SIZE),
                              incoming_data['target'])
    # File will be only created if not exists otherwise EEXIST error returned
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open('{0}{1}'.format(mount_point, incoming_data['target']), flags)
    os.fsync(fd)
    os.close(fd)
    outgoing_data['dirsize'] = os.stat("{0}/{1}".format(mount_point, incoming_data['target'].split('/')[1])).st_size
    # outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def stat(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    os.stat("{0}{1}".format(mount_point, incoming_data['target']))
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def read(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    with open("{0}{1}".format(mount_point, incoming_data['target']), 'r') as f:
        f.seek(incoming_data['offset'])
        buf = f.read(incoming_data['repeats'])
        hasher = hashlib.md5()
        hasher.update(buf)
        outgoing_data['hash'] = hasher.hexdigest()
        outgoing_data['offset'] = incoming_data['offset']
        outgoing_data['chunk_size'] = incoming_data['repeats']
        outgoing_data['uuid'] = incoming_data['uuid']
        return outgoing_data


def write(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    hasher = hashlib.md5()
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
    with open("{0}{1}".format(mount_point, incoming_data['target']), 'r+') as f:
        fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, data_pattern['repeats'], offset, 0)
        f.seek(offset)
        f.write(pattern_to_write)
        f.flush()
        os.fsync(f.fileno())
        #  Checking if original data pattern and pattern on disk are the same
        f.seek(offset)
        buf = f.read(data_pattern['repeats'])
        hasher = hashlib.md5()
        hasher.update(buf)
        read_hash = hasher.hexdigest()
        if read_hash != data_hash:
            outgoing_data['dynamo_error'] = error_codes.HASHERR
            outgoing_data['bad_hash'] = read_hash
        fcntl.lockf(f.fileno(), fcntl.LOCK_UN)
    outgoing_data['data_pattern'] = data_pattern['pattern']
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
    shutil.move("{0}/{1}/{2}".format(mount_point, dirpath, fname),
                "{0}/{1}/{2}".format(dst_mount_point, dirpath, incoming_data['rename_dest']))
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
    shutil.move("{0}/{1}/{2}".format(mount_point, src_dirpath, src_fname),
                "{0}/{1}/{2}".format(dst_mount_point, dst_dirpath, dst_fname))
    outgoing_data['rename_source'] = src_path
    outgoing_data['rename_dest'] = dst_path
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data


def truncate(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    padding = random.choice(PADDING)
    offset = random.choice(OFFSETS_LIST) + padding
    try:
        with open("{0}{1}".format(mount_point, incoming_data['target']), 'r+') as f:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.truncate(offset)
            f.flush()
            os.fsync(f.fileno())
            fcntl.lockf(f.fileno(), fcntl.LOCK_UN)
    except (IOError, OSError) as env_error:
        raise env_error
    outgoing_data['size'] = offset
    outgoing_data['uuid'] = incoming_data['uuid']
    return outgoing_data
