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
    return target


def list_request(logger, dir_tree):
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        target = 'None'
    else:
        target = rdir.data.name
    return target


def delete_request(logger, dir_tree):
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
    return target


def touch_request(logger, dir_tree):
    rdir = dir_tree.get_random_dir_synced()
    if not rdir:
        target = 'None'
    else:
        fname = rdir.data.touch()
        target = "/{0}/{1}".format(rdir.tag, fname)
    return target


def stat_request(logger, dir_tree):
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
    return target


def read_request(logger, dir_tree):
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
    return target


def write_request(logger, dir_tree):
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
    return target


def rename_request(logger, dir_tree):
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
    return target


def rename_exist_request(logger, dir_tree):
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
            target = "/{0}/{1} /{2}/{3}".format(rdir_src.tag, src_fname, rdir_dst.tag, dst_fname)
    return target
