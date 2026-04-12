import os

import datetime
import xxhash

import errno
import uuid

from treelib.tree import NodeIDAbsentError

from config import error_codes, MAX_FILES_PER_DIR

__author__ = "samuels"


def generic_error_handler(logger, incoming_message):
    """Log an unexpected error. Returns True to signal a critical failure."""
    rdir_name = incoming_message['target'].split('/')[3]
    try:
        rfile_name = incoming_message['target'].split('/')[4]
    except IndexError:
        logger.error(
            f"Operation {incoming_message['action']} FAILED UNEXPECTEDLY "
            f"on Directory {rdir_name} due to {incoming_message['error_message']}")
    else:
        logger.error(
            f"Operation {incoming_message['action']} FAILED UNEXPECTEDLY "
            f"on File {rdir_name}/{rfile_name} due to {incoming_message['error_message']}")
    return True


"""
Response action methods which will be called on arrived client message
"""


def response_action(logger, incoming_message, dir_tree):
    """Process a client response message. Returns True if a critical
    (unexpected) error was detected -- used by strict mode to stop the test.
    """
    if incoming_message['result'] == 'success':
        success_response_actions(incoming_message['action'])(logger, incoming_message, dir_tree)
        return False
    else:
        return failed_response_actions(incoming_message['action'])(logger, incoming_message, dir_tree)


def success_response_actions(action):
    """

    Args:
        action: str

    Returns: callback

    """
    return {
        'mkdir': mkdir_success,
        'touch': touch_success,
        'list': list_success,
        'stat': stat_success,
        'read': read_success,
        'write': write_success,
        'delete': delete_success,
        'rename': rename_success,
        'rename_exist': rename_exist_success,
        'truncate': truncate_success
    }[action]


def mkdir_success(logger, incoming_message, dir_tree):
    syncdir = dir_tree.get_dir_by_name(incoming_message['target'])
    syncdir.data.size = int(incoming_message['data']['dirsize'])
    syncdir.data.ondisk = True
    syncdir.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                       '%Y/%m/%d %H:%M:%S.%f')
    dir_hash = xxhash.xxh64(syncdir.data.name).hexdigest()
    dir_tree.add_synced_node(dir_hash, syncdir.data.name)
    logger.debug(
        f"Directory {syncdir.data.name} was created at: {syncdir.creation_time}")
    logger.debug(
        f"Directory {syncdir.data.name} is synced. Size is {int(incoming_message['data']['dirsize'])} bytes")


def touch_success(logger, incoming_message, dir_tree):
    logger.debug(f"Successful touch arrived {incoming_message['target']}")
    path = incoming_message['target'].split('/')[1:]  # folder:file
    syncdir = dir_tree.get_dir_by_name(path[0])
    dir_index = xxhash.xxh64(path[0]).hexdigest()
    if not syncdir:
        logger.debug(
            f"Directory {path[0]} already removed from active dirs list, dropping touch {path[1]}")
        return
    # There might be a raise when successful mkdir message will arrive after successful touch message
    # So we won't check here if dir is already synced

    f = syncdir.data.get_file_by_name(path[1])
    #  Now, when we got reply from client that file was created,
    #  we can mark it as synced
    syncdir.data.size += 1
    f.ondisk = True
    f.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                 '%Y/%m/%d %H:%M:%S.%f')
    f.uuid = uuid.uuid4().hex[-5:]  # Unique session ID, will be modified on each file modify action
    logger.debug(
        f"File {path[0]}/{path[1]} was created at: {f.creation_time}")
    logger.debug(
        f"File {path[0]}/{path[1]} is synced. Directory size updated to {syncdir.data.size} bytes")
    if syncdir.data.size > MAX_FILES_PER_DIR:
        try:
            logger.debug(f"Directory {path[0]} going to be removed from dir tree")
            dir_tree.remove_dir_by_name(path[0])
            dir_tree.remove_synced_node(dir_index)
            dir_tree.remove_nid(dir_index)
            logger.debug(
                f"Directory {path[0]} is reached its size limit and removed from active dirs list")
        except (NodeIDAbsentError, KeyError):
            logger.debug(
                f"Directory {path[0]} already removed from active dirs list, skipping....")


def list_success(logger, incoming_message, dir_tree):
    pass


def stat_success(logger, incoming_message, dir_tree):
    pass


def truncate_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    writedir = dir_tree.get_dir_by_name(path[0])
    if not writedir:
        logger.debug(
            f"Directory {path[0]} already removed from active dirs list, skipping....")
    else:
        logger.debug(f"Directory exists {writedir.data.name}, going to truncate file {path[1]}")
        if writedir.data.ondisk:
            wfile = writedir.data.get_file_by_name(path[1])
            if wfile and wfile.ondisk:
                logger.debug(f"File {path[0]}/{path[1]} is found, truncating")
                wfile.modify_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                               '%Y/%m/%d %H:%M:%S.%f')
                wfile.size = incoming_message['data']['size']
                # recalculating the offset after truncate:
                if wfile.data_pattern_offset + wfile.data_pattern_len >= wfile.size:
                    wfile.data_pattern_offset = wfile.size
                    wfile.data_pattern_hash = 'ef46db3751d8e999'
                    wfile.data_pattern_len = 0
                logger.debug(f"Truncating file {path[0]}/{path[1]} to {wfile.size} bytes")
            else:
                logger.debug(f"File {path[0]}/{path[1]} is not on disk, nothing to update")
        else:
            logger.debug(f"Directory {writedir.data.name} is not on disk, nothing to update")


def read_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    readdir = dir_tree.get_dir_by_name(path[0])
    if not readdir:
        logger.debug(
            f"Directory {path[0]} already removed from active dirs list, skipping....")
    else:
        logger.debug(f"Directory exists {readdir.data.name}, going to check file {path[1]} integrity")
        if readdir.data.ondisk:
            rfile = readdir.data.get_file_by_name(path[1])
            if rfile and rfile.ondisk:
                read_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if rfile.data_pattern_hash != incoming_message['data']['hash'] and read_time < rfile.modify_time:
                    logger.error(
                        f"Hash mismatch on Read! File {rfile.name} - "
                        f"stored hash: {rfile.data_pattern_hash} "
                        f"incoming hash: {incoming_message['data']['hash']} "
                        f"offset: {incoming_message['data']['offset']} "
                        f"chunk size: {incoming_message['data']['chunk_size']} ")
            else:
                logger.debug(f"File {path[0]}/{path[1]} is not on disk, nothing to update")
        else:
            logger.debug(f"Directory {readdir.data.name} is not on disk, nothing to update")


def write_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    writedir = dir_tree.get_dir_by_name(path[0])
    if not writedir:
        logger.debug(
            f"Directory {path[0]} already removed from active dirs list, skipping....")
    else:
        logger.debug(f"Directory exists {writedir.data.name}, going to update file {path[1]}")
        if writedir.data.ondisk:
            wfile = writedir.data.get_file_by_name(path[1])
            if wfile and wfile.ondisk:
                logger.debug(f"File {path[0]}/{path[1]} is found, writing")
                wfile.ondisk = True
                wfile.modify_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                               '%Y/%m/%d %H:%M:%S.%f')
                wfile.data_pattern = incoming_message['data']['data_pattern']
                wfile.data_pattern_len = incoming_message['data']['chunk_size']
                wfile.data_pattern_hash = incoming_message['data']['hash']
                wfile.data_pattern_offset = incoming_message['data']['offset']
                # recalculating file size
                if wfile.size < wfile.data_pattern_offset + wfile.data_pattern_len:
                    wfile.size = wfile.data_pattern_offset + wfile.data_pattern_len
                logger.debug(f"Write to file {path[0]}/{path[1]} at {wfile.data_pattern_offset}")
            # In case there is raise and write arrived before touch we'll sync the file here
            elif wfile:
                logger.debug(f"File {path[0]}/{path[1]} Write OP arrived before touch, syncing...")
                wfile.ondisk = True
                wfile.data_pattern = incoming_message['data']['data_pattern']
                wfile.data_pattern_len = incoming_message['data']['chunk_size']
                wfile.data_pattern_hash = incoming_message['data']['hash']
                wfile.data_pattern_offset = incoming_message['data']['offset']
                wfile.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                                 '%Y/%m/%d %H:%M:%S.%f')
                wfile.modify_time = wfile.creation_time
                # recalculating file size
                if wfile.size < wfile.data_pattern_offset + wfile.data_pattern_len:
                    wfile.size = wfile.data_pattern_offset + wfile.data_pattern_len
                logger.debug(f"Write to file {path[0]}/{path[1]} at {wfile.data_pattern_offset}")
            else:
                logger.debug(f"File {path[0]}/{path[1]} is not on disk, nothing to update")
        else:
            logger.debug(f"Directory {writedir.data.name} is not on disk, nothing to update")


def delete_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    deldir = dir_tree.get_dir_by_name(path[0])
    if not deldir:
        logger.debug(
            f"Directory {path[0]} already removed from active dirs list, skipping....")
    else:
        logger.debug(f"Directory exists {deldir.data.name}, going to delete {path[1]}")
        if deldir.data.ondisk:
            rfile = deldir.data.get_file_by_name(path[1])
            if rfile and rfile.ondisk:
                logger.debug(f"File {path[0]}/{path[1]} is found, removing")
                rfile.ondisk = False
                logger.debug(f"File {path[0]}/{path[1]} is removed form disk")
            else:
                logger.debug(f"File {path[0]}/{path[1]} is not on disk, nothing to update")
        else:
            logger.debug(f"Directory {deldir.data.name} is not on disk, nothing to update")


def rename_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    rename_dir = dir_tree.get_dir_by_name(path[0])
    if not rename_dir:
        logger.debug(
            f"Directory {path[0]} already removed from active dirs list, skipping....")
        return

    logger.debug(f"Directory exists {path[0]}, going to rename {path[1]}")
    if rename_dir.data.ondisk:
        rfile = rename_dir.data.get_file_by_name(path[1])
        if rfile:
            logger.debug(f"File {path[0]}/{path[1]} is found, renaming")
            rfile = rename_dir.data.rename_file(rfile.name, incoming_message['data']['rename_dest'])
            rfile.ondisk = True
            rfile.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                             '%Y/%m/%d %H:%M:%S.%f')
            logger.debug(f"File {path[0]}/{path[1]} is renamed to {rfile.name}")
        else:
            logger.debug(f"File {path[0]}/{path[1]} is not on disk, nothing to update")
    else:
        logger.debug(
            f"Directory {rename_dir.data.name} is not on disk, nothing to update")


def rename_exist_success(logger, incoming_message, dir_tree):
    src_path = incoming_message['data']['rename_source'].split('/')[1:]  # folder:file
    dst_path = incoming_message['data']['rename_dest'].split('/')[1:]  # folder:file
    src_rename_dir = dir_tree.get_dir_by_name(src_path[0])
    dst_rename_dir = dir_tree.get_dir_by_name(dst_path[0])
    if not src_rename_dir:
        logger.debug(
            f"Source directory {src_path[0]} already removed from active dirs list, skipping....")
        return
    logger.debug(
        f"Directory exists {src_path[0]}, going to delete renamed file {src_path[1]} from directory")
    #  Firs we delete the source file
    if src_rename_dir.data.ondisk:
        file_to_delete = src_rename_dir.data.get_file_by_name(src_path[1])
        if file_to_delete and file_to_delete.ondisk:
            logger.debug(f"File {src_path[0]}/{src_path[1]} is found, removing")
            file_to_delete.ondisk = False
            logger.debug(f"File {src_path[0]}/{src_path[1]} is removed form disk")
        else:
            logger.debug(f"File {src_path[0]}/{src_path[1]} is not on disk, nothing to update")
    else:
        logger.debug(
            f"Directory {src_path[0]} is not on disk, nothing to update")

    # Actual rename of destination file
    if not dst_rename_dir:
        logger.debug(
            f"Directory {dst_path[0]} already removed from active dirs list, skipping....")
        return

    logger.debug(f"Directory exists {dst_path[0]}, going to rename {src_path[1]} to {dst_path[1]}")
    if dst_rename_dir.data.ondisk:
        file_to_rename = dst_rename_dir.data.get_file_by_name(dst_path[1])
        if file_to_rename:
            logger.debug(f"File {dst_path[0]}/{dst_path[1]} is found, renaming")
            file_to_rename = dst_rename_dir.data.rename_file(file_to_rename.name, dst_path[1])
            file_to_rename.ondisk = True
            file_to_rename.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                                      '%Y/%m/%d %H:%M:%S.%f')
            logger.debug(f"File {src_path[0]}/{src_path[1]} is renamed to {dst_path[1]}")
        else:
            logger.debug(f"File {dst_path[0]}/{dst_path[1]} is not on disk, nothing to update")
    else:
        logger.debug(
            f"Directory {dst_path[0]} is not on disk, nothing to update")


BENIGN_ERRORS = {
    'mkdir':        {error_codes.NO_TARGET, errno.EEXIST},
    'touch':        {error_codes.NO_TARGET, errno.EEXIST, error_codes.MAX_DIR_SIZE},
    'list':         {error_codes.NO_TARGET, errno.EEXIST, errno.ENOENT},
    'stat':         {error_codes.NO_TARGET, errno.EEXIST, errno.ESTALE},
    'read':         {error_codes.NO_TARGET, errno.EEXIST, error_codes.ZERO_SIZE, errno.ESTALE},
    'write':        {error_codes.NO_TARGET, errno.EEXIST, errno.ESTALE, errno.EAGAIN},
    'delete':       {error_codes.NO_TARGET, errno.EEXIST, errno.ESTALE},
    'rename':       {error_codes.NO_TARGET, errno.EEXIST, errno.ESTALE},
    'rename_exist': {error_codes.NO_TARGET, errno.EEXIST, errno.ESTALE, error_codes.SAMEFILE},
    'truncate':     {error_codes.NO_TARGET, errno.EEXIST, errno.ESTALE, errno.EAGAIN},
}


def _verify_enoent_file(logger, incoming_message, dir_tree):
    """Common ENOENT verification for file-level operations.
    Checks whether the file was expected on disk and invalidates if so.
    Returns True if the error was an unexpected verification failure.
    """
    rdir_name = incoming_message['target'].split('/')[3]
    rfile_name = incoming_message['target'].split('/')[4]

    rdir = dir_tree.get_dir_by_name(rdir_name)
    if not rdir:
        logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
        return False

    rfile = rdir.data.get_file_by_name(rfile_name)
    if rfile and rfile.ondisk:
        error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
        if error_time > rfile.creation_time:
            logger.error(
                f"Result Verify FAILED: Operation {incoming_message['action']} "
                f"failed on file {rdir_name}/{rfile_name} which is on disk. Invalidating")
            rfile.ondisk = False
            return True
    else:
        logger.debug(f"Result verify OK: File {rfile_name} is not on disk")
    return False


def _verify_enoent_touch(logger, incoming_message, dir_tree):
    """ENOENT verification for touch -- checks directory level only since
    touch creates a new file."""
    rdir_name = incoming_message['target'].split('/')[3]
    rfile_name = incoming_message['target'].split('/')[4]

    rdir = dir_tree.get_dir_by_name(rdir_name)
    if rdir and rdir.data.ondisk:
        error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
        if error_time > rdir.creation_time:
            logger.error(
                f"Result Verify FAILED: Operation {incoming_message['action']} "
                f"failed on {rdir_name}/{rfile_name} which is on disk")
            return True
        else:
            logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
    return False


def failed_response_actions(action):
    """Returns the unified fail handler for all actions.
    The handler returns True if a critical (unexpected) error was detected.
    """
    def _fail(logger, incoming_message, dir_tree):
        code = incoming_message['error_code']
        if code in BENIGN_ERRORS.get(action, set()):
            return False
        if code == errno.ENOENT:
            if action == 'touch':
                return _verify_enoent_touch(logger, incoming_message, dir_tree)
            elif action == 'mkdir':
                return generic_error_handler(logger, incoming_message)
            else:
                return _verify_enoent_file(logger, incoming_message, dir_tree)
        else:
            return generic_error_handler(logger, incoming_message)
    return _fail
