import os
import random
import shutil

import sys

sys.path.append('/qa/dynamo/client')
from dynamo import MAX_DIR_SIZE, DynamoException
from utils import shell_utils

__author__ = "samuels"


def response_action(action, mount_point, target, **kwargs):
    return {
        "mkdir": mkdir,
        "list": list_dir,
        "delete": delete,
        "touch": touch,
        "stat": stat,
        "read": read,
        "rename": rename,
        "rename_exist": rename_exist
    }[action](mount_point, target, **kwargs)


def mkdir(mount_point, target):
    data = {}
    os.mkdir("{0}/{1}".format(mount_point, target))
    data['dirsize'] = os.stat("{0}/{1}".format(mount_point, target)).st_size
    return data


def list_dir(mount_point, target):
    os.listdir('{0}/{1}'.format(mount_point, target))


def delete(mount_point, target):
    dirpath = target.split('/')[1]
    fname = target.split('/')[2]
    os.remove('{0}/{1}/{2}'.format(mount_point, dirpath, fname))


def touch(mount_point, target):
    data = {}
    dirsize = os.stat("{0}/{1}".format(mount_point, target.split('/')[1])).st_size
    if dirsize > MAX_DIR_SIZE:  # if Directory entry size > 128K, we'll stop writing new files
        data['target_path'] = target
        raise DynamoException("Directory Entry reached {0} size limit".format(MAX_DIR_SIZE))
    # shell_utils.touch('{0}{1}'.format(mount_point, work['target']))
    with open('{0}{1}'.format(mount_point, target), 'w'):
        pass
    data['dirsize'] = os.stat("{0}/{1}".format(mount_point, target.split('/')[1])).st_size
    return data


def stat(mount_point, target):
    os.stat("{0}{1}".format(mount_point, target))


def read(mount_point, target):
    with open("{0}{1}".format(mount_point, target), 'r') as f:
        f.read()


def rename(mount_point, target, **kwargs):
    data = {}
    dirpath = target.split('/')[1]
    fname = target.split('/')[2]
    dst_mount_point = "".join(
        "/mnt/DIRSPLIT-node{0}.{1}-{2}".format(random.randint(0, kwargs['nodes'] - 1), kwargs['server'],
                                               random.randint(0, kwargs['domains'] - 1)))
    data['rename_dest'] = shell_utils.StringUtils.get_random_string_nospec(64)
    shutil.move("{0}/{1}/{2}".format(mount_point, dirpath, fname),
                "{0}/{1}/{2}".format(dst_mount_point, dirpath, data['rename_dest']))
    return data


def rename_exist(mount_point, target, **kwargs):
    data = {}
    src_path = target.split(' ')[0]
    dst_path = target.split(' ')[1]
    src_dirpath = src_path.split('/')[1]
    src_fname = src_path.split('/')[2]
    dst_dirpath = dst_path.split('/')[1]
    dst_fname = dst_path.split('/')[2]
    dst_mount_point = "".join(
        "/mnt/DIRSPLIT-node{0}.{1}-{2}".format(random.randint(0, kwargs['nodes'] - 1), kwargs['server'],
                                               random.randint(0, kwargs['domains'] - 1)))
    data['rename_dest'] = "{0}".format(dst_fname)
    shutil.move("{0}/{1}/{2}".format(mount_point, src_dirpath, src_fname),
                "{0}/{1}/{2}".format(dst_mount_point, dst_dirpath, dst_fname))
    return data
