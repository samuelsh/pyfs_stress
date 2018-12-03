import os

import datetime
import logging

import xxhash

import errno
import uuid

from treelib.tree import NodeIDAbsentError

from config import error_codes, MAX_FILES_PER_DIR

__author__ = "samuels"


def generic_error_handler(logger, incoming_message):
    """

    Args:
        logger: logger
        incoming_message: dict

    Returns:

    """
    rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
    try:
        rfile_name = incoming_message['target'].split('/')[4]
    except IndexError:
        logger.error(
            'Operation {0} FAILED UNEXPECTEDLY on Directory {1} due to {2}'.format(
                incoming_message['action'],
                rdir_name,
                incoming_message[
                    'error_message']))
    else:
        logger.error(
            'Operation {0} FAILED UNEXPECTEDLY on File {1}/{2} due to {3}'.format(
                incoming_message['action'],
                rdir_name,
                rfile_name,
                incoming_message[
                    'error_message']))


"""
Response action methods which will be called on arrived client message
"""


def response_action(logger, incoming_message, dir_tree):
    """

    Args:
        logger: logging
        incoming_message: dict
        dir_tree: dir_tree

    Returns:

    """
    if incoming_message['result'] == 'success':
        success_response_actions(incoming_message['action'])(logger, incoming_message, dir_tree)
    else:
        failed_response_actions(incoming_message['action'])(logger, incoming_message, dir_tree)


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
    dir_tree.synced_nodes[dir_hash] = syncdir.data.name
    logger.debug(
        f"Directory {syncdir.data.name} was created at: {syncdir.creation_time}")
    logger.debug(
        f"Directory {syncdir.data.name} is synced. Size is {int(incoming_message['data']['dirsize'])} bytes")


def touch_success(logger, incoming_message, dir_tree):
    logger.debug(f"Successful touch arrived incoming_message['target']")
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
            del dir_tree.synced_nodes[dir_index]
            del dir_tree.nids[dir_index]
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


def failed_response_actions(action):
    return {
        'mkdir': mkdir_fail,
        'touch': touch_fail,
        'list': list_fail,
        'stat': stat_fail,
        'read': read_fail,
        'write': write_fail,
        'delete': delete_fail,
        'rename': rename_fail,
        'rename_exist': rename_exist_fail,
        'truncate': truncate_fail
    }[action]


def mkdir_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST:
        return
    generic_error_handler(logger, incoming_message)


def touch_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST:
        return
    if incoming_message['error_code'] == error_codes.MAX_DIR_SIZE:
        pass
    elif incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path
        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir and rdir.data.ondisk:
            error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
            if error_time > rdir.creation_time:
                logger.error(
                    f"Result Verify FAILED: Operation {incoming_message['action']} "
                    f"failed on {rdir_name}/{rfile_name} which is on disk")
            else:
                logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
    else:
        generic_error_handler(logger, incoming_message)


def list_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        pass
    else:
        generic_error_handler(logger, incoming_message)


def stat_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == errno.ESTALE:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        f"Result Verify FAILED: Operation {incoming_message['action']} "
                        f"failed on file {rdir_name}{os.path.sep}{rfile_name} which is on disk. Invalidating")
                    rfile.ondisk = False
            else:
                logger.debug(f"Result verify OK: File {rfile_name} is not on disk")
        else:
            logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
    else:
        generic_error_handler(logger, incoming_message)


def truncate_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == errno.ESTALE or incoming_message['error_code'] == errno.EAGAIN:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        f"Result Verify FAILED: Operation {incoming_message['action']} "
                        f"failed on file {rdir_name}{os.path.sep}{rfile_name} which is on disk. Invalidating")
                    rfile.ondisk = False
            else:
                logger.debug(f"Result verify OK: File {rfile_name} is not on disk")
        else:
            logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
    else:
        generic_error_handler(logger, incoming_message)


def read_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == error_codes.ZERO_SIZE or \
            incoming_message['error_code'] == errno.ESTALE:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk. Invalidating".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
                    rfile.ondisk = False
            else:
                logger.debug('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.debug('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def write_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == errno.ESTALE or incoming_message['error_code'] == errno.EAGAIN:
        return

    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk. Invalidating".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
                    rfile.ondisk = False
            else:
                logger.debug('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.debug('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def delete_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == errno.ESTALE:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk. Invalidating".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
                    rfile.ondisk = False
            else:
                logger.debug('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.debug('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def rename_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == errno.ESTALE:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        f"Result Verify FAILED: Operation {incoming_message['action']} "
                        f"failed on file {rdir_name}{os.path.sep}{rfile_name} which is on disk. Invalidating")
                    rfile.ondisk = False
            else:
                logger.debug(f"Result verify OK: File {rfile_name} is not on disk")
        else:
            logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
    else:
        generic_error_handler(logger, incoming_message)


def rename_exist_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == error_codes.NO_TARGET or incoming_message['error_code'] == errno.EEXIST or \
            incoming_message['error_code'] == errno.ESTALE:
        return
    if incoming_message['error_code'] == error_codes.SAMEFILE:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir:
            rfile = rdir.data.get_file_by_name(rfile_name)
            if rfile and rfile.ondisk:
                error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
                if error_time > rfile.creation_time:
                    logger.error(
                        f"Result Verify FAILED: Operation {incoming_message['action']} "
                        f"failed on file {rdir_name}{os.path.sep}{rfile_name} which is on disk. Invalidating")
                    rfile.ondisk = False
            else:
                logger.debug(f"Result verify OK: File {rfile_name} is not on disk")
        else:
            logger.debug(f"Result verify OK: Directory {rdir_name} is not on disk")
    else:
        generic_error_handler(logger, incoming_message)


def handle_noent(dir_name, file_name, incoming_tid, timestamp, dir_tree):
    dir_entry = dir_tree.get_dir_by_name(dir_name)
    if dir_entry:
        file_entry = dir_entry.data.get_file_by_name(file_name)
        if file_entry and file_entry.ondisk:
            if file_entry.tid > incoming_tid:
                return error_codes.TIDERR
            error_time = datetime.datetime.strptime(timestamp, '%Y/%m/%d %H:%M:%S.%f')
            if error_time > file_entry.creation_time:
                return error_codes.ENOTONDISK
        else:
            return error_codes.FILE_NOTONDISK_OK
    else:
        return error_codes.DIR_NOTONDISK_OK


def method_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_code'] == errno.ENOENT:
        dir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        file_name = incoming_message['target'].split('/')[4]  # get target file name from path
        notondisk_error_mgs = "Result Verify FAILED: " \
                              "Operation {0} failed on file {1} which is on disk. Invalidating".format(
                                incoming_message['action'], dir_name + "/" + file_name)
        tid_error_msg = "Incoming tid: {} < current tid. "
        file_ok_msg = "Result verify OK: File {0} is not on disk".format(file_name)
        dir_ok_msg = "Result verify OK: Directory {0} is not on disk".format(dir_name)
        error_code = handle_noent(dir_name, file_name, incoming_message['tid'], incoming_message['timestamp'], dir_tree)
        error = {
            error_codes.TIDERR: {"severity": logging.WARN, "message": tid_error_msg},
            error_codes.ENOTONDISK: {"severity": logging.ERROR, "message": notondisk_error_mgs},
            error_codes.FILE_NOTONDISK_OK: {"severity": logging.DEBUG, "message": file_ok_msg},
            error_codes.DIR_NOTONDISK_OK: {"severity": logging.DEBUG, "message": dir_ok_msg},
        }[error_code]
        logger.log(error['severity'], error['message'])
