import hashlib
import os
import random
import shutil
import fcntl
# import data_operations.data_generators
import sys

from utils import shell_utils

sys.path.append('/qa/dynamo')
from config import error_codes

__author__ = "samuels"

MAX_DIR_SIZE = 128 * 1024
INLINE_MAX_SIZE = 3499
KB1 = 1024
KB4 = KB1 * 4
MB1 = (1024 * 1024)
GB1 = (1024 * 1024 * 1024)
TB1 = (1024 * 1024 * 1024 * 1024)
MB512 = (MB1 * 512)  # level 1 can map up to 512MB
GB256 = (GB1 * 256)  # level 2 can map up to 256GB
TB128 = (TB1 * 128)  # level 3 can map up to 128TB
ZERO_PADDING_START = 128 * MB1  # 128MB
DATA_PATTERN_A = {'pattern': 'A', 'repeats': 1}
DATA_PATTERN_B = {'pattern': 'B', 'repeats': 3}
DATA_PATTERN_C = {'pattern': 'C', 'repeats': 17}
DATA_PATTERN_D = {'pattern': 'D', 'repeats': 33}
DATA_PATTERN_E = {'pattern': 'E', 'repeats': 65}
DATA_PATTERN_F = {'pattern': 'F', 'repeats': 129}
DATA_PATTERN_G = {'pattern': 'G', 'repeats': 257}
DATA_PATTERN_H = {'pattern': 'H', 'repeats': 1025}

OFFSETS_LIST = [KB1, KB4, MB1, GB1, TB1, MB512, GB256]
DATA_PATTERNS_LIST = [DATA_PATTERN_A, DATA_PATTERN_B, DATA_PATTERN_C, DATA_PATTERN_D, DATA_PATTERN_E, DATA_PATTERN_F,
                      DATA_PATTERN_G, DATA_PATTERN_H]


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
        "rename_exist": rename_exist
    }[action](mount_point, incoming_data, **kwargs)


def mkdir(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    os.mkdir("{0}/{1}".format(mount_point, incoming_data['target']))
    outgoing_data['dirsize'] = os.stat("{0}/{1}".format(mount_point, incoming_data['target'])).st_size
    return outgoing_data


def list_dir(mount_point, incoming_data, **kwargs):
    os.listdir('{0}/{1}'.format(mount_point, incoming_data['target']))


def delete(mount_point, incoming_data, **kwargs):
    dirpath = incoming_data['target'].split('/')[1]
    fname = incoming_data['target'].split('/')[2]
    os.remove('{0}/{1}/{2}'.format(mount_point, dirpath, fname))


def touch(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    dirsize = os.stat("{0}/{1}".format(mount_point, incoming_data['target'].split('/')[1])).st_size
    if dirsize > MAX_DIR_SIZE:  # if Directory entry size > 128K, we'll stop writing new files
        outgoing_data['target_path'] = incoming_data['target']
        raise DynamoException(error_codes.MAX_DIR_SIZE, "Directory Entry reached {0} size limit".format(MAX_DIR_SIZE),
                              incoming_data['target'])
    # shell_utils.touch('{0}{1}'.format(mount_point, work['target']))
    with open('{0}{1}'.format(mount_point, incoming_data['target']), 'w'):
        pass
    outgoing_data['dirsize'] = os.stat("{0}/{1}".format(mount_point, incoming_data['target'].split('/')[1])).st_size
    return outgoing_data


def stat(mount_point, incoming_data, **kwargs):
    os.stat("{0}{1}".format(mount_point, incoming_data['target']))


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
        return outgoing_data


def write(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    hasher = hashlib.md5()
    offset = random.choice(OFFSETS_LIST)
    data_pattern = random.choice(DATA_PATTERNS_LIST)
    pattern_to_write = data_pattern['pattern'] * data_pattern['repeats']
    hasher.update(pattern_to_write)
    data_hash = hasher.hexdigest()
    try:
        with open("{0}{1}".format(mount_point, incoming_data['target']), 'r+') as f:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.seek(ZERO_PADDING_START + offset)
            f.write(pattern_to_write)
            f.flush()
            os.fsync(f.fileno())
            fcntl.lockf(f.fileno(), fcntl.LOCK_UN)
    except (IOError, OSError) as env_error:
        raise env_error
    outgoing_data['data_pattern'] = data_pattern['pattern']
    outgoing_data['repeats'] = data_pattern['repeats']
    outgoing_data['hash'] = data_hash
    outgoing_data['offset'] = ZERO_PADDING_START + offset
    return outgoing_data


def rename(mount_point, incoming_data, **kwargs):
    outgoing_data = {}
    fullpath = incoming_data['target'].split('/')[1:]
    dirpath = fullpath[0]
    fname = fullpath[1]
    dst_mount_point = "".join(
        "/mnt/DIRSPLIT-node{0}.{1}-{2}".format(random.randint(0, kwargs['nodes'] - 1), kwargs['server'],
                                               random.randint(0, kwargs['domains'] - 1)))
    outgoing_data['rename_dest'] = incoming_data['rename_dest']
    shutil.move("{0}/{1}/{2}".format(mount_point, dirpath, fname),
                "{0}/{1}/{2}".format(dst_mount_point, dirpath, incoming_data['rename_dest']))
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
    dst_mount_point = "".join(
        "/mnt/DIRSPLIT-node{0}.{1}-{2}".format(random.randint(0, kwargs['nodes'] - 1), kwargs['server'],
                                               random.randint(0, kwargs['domains'] - 1)))
    shutil.move("{0}/{1}/{2}".format(mount_point, src_dirpath, src_fname),
                "{0}/{1}/{2}".format(dst_mount_point, dst_dirpath, dst_fname))
    outgoing_data['rename_source'] = src_path
    outgoing_data['rename_dest'] = dst_path
    return outgoing_data
