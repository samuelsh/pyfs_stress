import os

from utils import shell_utils

__author__ = "samuels"


def request_action(action, logger, dir_tree, **kwargs):
    return {
        "mkdir": mkdir_request,
        "list": list_request,
        "delete": delete_request,
        "touch": touch_request,
        "stat": stat_request,
        "read": read_request,
        "write": write_request,
        "rename": rename_request,
        "rename_exist": rename_exist_request,
        "truncate": truncate_request
    }[action](logger, dir_tree, **kwargs)


def mkdir_request(logger, dir_tree, **kwargs):
    data = {}
    target = 'None'
    if len(dir_tree.synced_nodes) > 10 or len(dir_tree.nids) > 10:
        return None
    logger.debug("DEBUG NIDS: {}".format(dir_tree.nids))
    logger.debug("DEBUG SYNCED_DIRS: {}".format(dir_tree.synced_nodes))
    dir_tree.append_node()
    logger.debug(
        "Controller: New dir appended to list {0}".format(dir_tree.get_last_node_tag()))
    target_dir = dir_tree.get_random_dir_not_synced()
    if target_dir:
        target = target_dir.data.name
        logger.debug(
            "Controller: Dir {0} current size is {1}".format(target, dir_tree.get_last_node_data().size))
    data['target'] = target
    return data


def list_request(logger, dir_tree, **kwargs):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        return None
    target = rdir.data.name
    data['target'] = "/".join(['', target])
    return data


def delete_request(logger, dir_tree, **kwargs):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        return None
    file_to_delete = rdir.data.get_random_file()
    if not file_to_delete:
        return None
    fname = file_to_delete.name
    target = "/".join(['', rdir.tag, fname])
    uuid = file_to_delete.uuid
    data['target'] = target
    data['uuid'] = uuid
    return data


def touch_request(logger, dir_tree, **kwargs):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        return None
    fname = rdir.data.touch()
    target = "/".join(['', rdir.tag, fname])
    data['target'] = target
    return data


def stat_request(logger, dir_tree, **kwargs):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        return None
    rfile = rdir.data.get_random_file()
    if not rfile:
        return None
    fname = rfile.name
    target = "/".join(['', rdir.tag, fname])
    uuid = rfile.uuid
    data['target'] = target
    data['uuid'] = uuid
    return data


def read_request(logger, dir_tree, **kwargs):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        return None
    rfile = rdir.data.get_random_file()
    if not rfile:
        return None
    fname = rfile.name
    target = "/".join(['', rdir.tag, fname])
    data['target'] = target
    data['data_pattern'] = rfile.data_pattern
    data['repeats'] = rfile.data_pattern_len
    data['hash'] = rfile.data_pattern_hash
    data['offset'] = rfile.data_pattern_offset
    data['uuid'] = rfile.uuid
    return data


def write_request(logger, dir_tree, **kwargs):
    data = {}
    wdir = dir_tree.get_random_dir_synced()
    if not wdir:
        return None
    wfile = wdir.data.get_random_file()
    if not wfile:
        return None
    fname = wfile.name
    target = "/".join(['', wdir.tag, fname])
    data['target'] = target
    data['offset'] = wfile.data_pattern_offset
    data['data_pattern_len'] = wfile.data_pattern_len
    data['io_type'] = kwargs['io_type']
    data['uuid'] = wfile.uuid
    return data


def rename_request(logger, dir_tree, **kwargs):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        return None
    file_to_rename = rdir.data.get_random_file()
    if not file_to_rename:
        return None
    fname = file_to_rename.name
    target = "/".join(['', rdir.tag, fname])
    uuid = file_to_rename.uuid
    data['target'] = target
    data['uuid'] = uuid
    data['rename_dest'] = shell_utils.StringUtils.get_random_string_nospec(64)
    return data


def rename_exist_request(logger, dir_tree, **kwargs):
    data = {}
    rdir_src = dir_tree.get_random_dir_synced()
    rdir_dst = dir_tree.get_random_dir_synced()
    if not rdir_src or not rdir_dst:
        return None
    src_file_to_rename = rdir_src.data.get_random_file()
    dst_file = rdir_src.data.get_random_file()
    if not src_file_to_rename or not dst_file:
        return None
    src_fname = src_file_to_rename.name
    dst_fname = dst_file.name
    target = "/".join(['', rdir_src.tag, src_fname])
    data['rename_dest'] = "/".join(['', rdir_dst.tag, dst_fname])
    uuid = src_file_to_rename.uuid
    data['target'] = target
    data['uuid'] = uuid
    data['rename_source'] = target
    return data


def truncate_request(logger, dir_tree, **kwargs):
    data = {}
    tdir = dir_tree.get_random_dir_synced()
    if not tdir:
        return None
    file_to_truncate = tdir.data.get_random_file()
    if not file_to_truncate:
        return None
    fname = file_to_truncate.name
    target = "/".join(['', tdir.tag, fname])
    uuid = file_to_truncate.uuid
    data['target'] = target
    data['uuid'] = uuid
    return data
