from utils import shell_utils

__author__ = "samuels"


def request_action(action, logger, dir_tree):
    return {
        "mkdir": mkdir_request,
        "list": list_request,
        "delete": delete_request,
        "touch": touch_request,
        "stat": stat_request,
        "read": read_request,
        "write": write_request,
        "rename": rename_request,
        "rename_exist": rename_exist_request
    }[action](logger, dir_tree)


def mkdir_request(logger, dir_tree):
    data = {}
    target = 'None'
    if len(dir_tree.nids) < 100:
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


def list_request(logger, dir_tree):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        target = 'None'
    else:
        target = rdir.data.name
    data['target'] = target
    return data


def delete_request(logger, dir_tree):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        target = 'None'
    else:
        file_to_delete = rdir.data.get_random_file()
        if not file_to_delete:
            target = 'None'
        else:
            fname = file_to_delete.name
            target = "/{0}/{1}".format(rdir.tag, fname)
    data['target'] = target
    return data


def touch_request(logger, dir_tree):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        target = 'None'
    else:
        fname = rdir.data.touch()
        target = "/{0}/{1}".format(rdir.tag, fname)
    data['target'] = target
    return data


def stat_request(logger, dir_tree):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if rdir:
        rfile = rdir.data.get_random_file()
        if not rfile:
            target = 'None'
        else:
            fname = rfile.name
            target = "/{0}/{1}".format(rdir.tag, fname)
    else:
        target = 'None'
    data['target'] = target
    return data


def read_request(logger, dir_tree):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if rdir:
        rfile = rdir.data.get_random_file()
        if not rfile:
            target = 'None'
        else:
            fname = rfile.name
            target = "/{0}/{1}".format(rdir.tag, fname)
    else:
        target = 'None'
    data['target'] = target
    return data


def write_request(logger, dir_tree):
    data = {}
    wfile = None
    wdir = dir_tree.get_random_dir_synced()
    if wdir:
        wfile = wdir.data.get_random_file()
        if not wfile:
            target = 'None'
            return {'target': 'None'}
        else:
            fname = wfile.name
            target = "/{0}/{1}".format(wdir.tag, fname)
    else:
        return {'target': 'None'}
    data['target'] = target
    data['data_pattern'] = wfile.data_pattern
    data['repeats'] = wfile.data_pattern_len
    data['hash'] = wfile.data_pattern_hash
    data['offset'] = wfile.data_pattern_offset
    return data


def rename_request(logger, dir_tree):
    data = {}
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        target = 'None'
    else:
        file_to_rename = rdir.data.get_random_file()
        if not file_to_rename:
            target = 'None'
        else:
            fname = file_to_rename.name
            target = "/{0}/{1}".format(rdir.tag, fname)
    data['target'] = target
    data['rename_dest'] = shell_utils.StringUtils.get_random_string_nospec(64)
    return data


def rename_exist_request(logger, dir_tree):
    data = {}
    rdir_src = dir_tree.get_random_dir_synced()
    rdir_dst = dir_tree.get_random_dir_synced()
    if not rdir_src or not rdir_dst:
        target = 'None'
    else:
        src_file_to_rename = rdir_src.data.get_random_file()
        dst_file = rdir_src.data.get_random_file()
        if not src_file_to_rename or not dst_file:
            target = 'None'
        else:
            src_fname = src_file_to_rename.name
            dst_fname = dst_file.name
            target = "/{0}/{1}".format(rdir_src.tag, src_fname)
            data['rename_dest'] = "/{0}/{1}".format(rdir_dst.tag, dst_fname)
    data['target'] = target
    data['rename_source'] = target
    return data
