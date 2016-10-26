import datetime
import hashlib

import errno
from treelib.tree import NodeIDAbsentError

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
        'delete': delete_success,
        'rename': rename_success
    }[action]


def mkdir_success(logger, incoming_message, dir_tree):
    syncdir = dir_tree.get_dir_by_name(incoming_message['target'])
    syncdir.data.size = int(incoming_message['data']['dirsize'])
    syncdir.data.ondisk = True
    syncdir.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                       '%Y/%m/%d %H:%M:%S.%f')
    dir_tree.synced_nodes.append(hashlib.md5(syncdir.data.name).hexdigest())
    logger.debug(
        "Directory {0} was created at: {1}".format(syncdir.data.name, syncdir.creation_time))
    logger.info(
        'Directory {0} is synced. Size is {1}'.format(syncdir.data.name,
                                                      int(incoming_message['data']['dirsize'])))


def touch_success(logger, incoming_message, dir_tree):
    logger.debug("Successfull touch arrived {0}".format(incoming_message['target']))
    path = incoming_message['target'].split('/')[1:]  # folder:file
    syncdir = dir_tree.get_dir_by_name(path[0])
    if not syncdir:
        logger.debug(
            "Directory {0} already removed from active dirs list, dropping touch {1}".format(path[0],
                                                                                             path[1]))
    # There might be a raise when successful mkdir message will arrive after successful touch message
    # So we won't check here if dir is already synced
    else:
        for f in syncdir.data.files:
            if f.name == path[1]:  # Now, when we got reply from client that file was created,
                #  we can mark it as synced
                syncdir.data.size = int(incoming_message['data']['dirsize'])
                f.ondisk = True
                f.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                             '%Y/%m/%d %H:%M:%S.%f')
                logger.debug(
                    "File {0}/{1} was created at: {2}".format(path[0], path[1], f.creation_time))
                logger.info(
                    'File {0}/{1} is synced. Directory size updated to {2}'.format(path[0], path[1],
                                                                                   int(incoming_message[
                                                                                           'data'][
                                                                                           'dirsize'])))
                break


def list_success(logger, incoming_message, dir_tree):
    pass


def stat_success(logger, incoming_message, dir_tree):
    pass


def read_success(logger, incoming_message, dir_tree):
    pass


def delete_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    deldir = dir_tree.get_dir_by_name(path[0])
    if not deldir:
        logger.debug(
            "Directory {0} already removed from active dirs list, skipping....".format(path[0]))
    else:
        logger.debug('Directory exists {0}, going to delete {1}'.format(deldir.data.name, path[1]))
        if deldir.data.ondisk:
            rfile = deldir.data.get_file_by_name(path[1])
            if rfile and rfile.ondisk:
                logger.debug('File {0}/{1} is found, removing'.format(path[0], path[1]))
                rfile.ondisk = False
                logger.info('File {0}/{1} is removed form disk'.format(path[0], path[1]))
            else:
                logger.debug("File {0}/{1} is not on disk, nothing to update".format(path[0], path[1]))
        else:
            logger.debug("Directory {0} is not on disk, nothing to update".format(deldir.data.name))


def rename_success(logger, incoming_message, dir_tree):
    path = incoming_message['target'].split('/')[1:]  # folder:file
    rename_dir = dir_tree.get_dir_by_name(path[0])
    if not rename_dir:
        logger.debug(
            "Directory {0} already removed from active dirs list, skipping....".format(path[0]))
    else:
        logger.debug('Directory exists {0}, going to rename {1}'.format(rename_dir.data.name, path[1]))
        if rename_dir.data.ondisk:
            rfile = rename_dir.data.get_file_by_name(path[1])
            if rfile and rfile.ondisk:
                logger.debug('File {0}/{1} is found, renaming'.format(path[0], path[1]))
                rfile.name = incoming_message['data']['rename_dest']
                logger.info('File {0}/{1} is renamed to {2}'.format(path[0], path[1], rfile.name))
            else:
                logger.debug("File {0}/{1} is not on disk, nothing to update".format(path[0], path[1]))
        else:
            logger.debug(
                "Directory {0} is not on disk, nothing to update".format(rename_dir.data.name))


def failed_response_actions(action):
    return {
        'mkdir': mkdir_fail,
        'touch': touch_fail,
        'list': list_fail,
        'stat': stat_fail,
        'read': read_fail,
        'delete': delete_fail,
        'rename': rename_fail
    }[action]


def mkdir_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
        return
    generic_error_handler(logger, incoming_message)


def touch_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
        return
    if "size limit" in incoming_message['error_message']:
        rdir_name = incoming_message['target'].split('/')[1]  # get target folder name from path
        try:
            logger.info("Directory {0} going to be removed from dir tree".format(rdir_name))
            dir_tree.remove_dir_by_name(rdir_name)
            node_index = dir_tree.synced_nodes.index(hashlib.md5(rdir_name).hexdigest())
            del dir_tree.synced_nodes[node_index]
            node_index = dir_tree.nids.index(hashlib.md5(rdir_name).hexdigest())
            del dir_tree.nids[node_index]
            logger.info(
                "Directory {0} is reached its size limit and removed from active dirs list".format(rdir_name))
            dir_tree.append_node()
            logger.info(
                "New Directory node appended to tree {0}".format(dir_tree.get_last_node_tag()))
        except NodeIDAbsentError:
            logger.debug(
                "Directory {0} already removed from active dirs list, skipping....".format(rdir_name))

    elif incoming_message['error_code'] == errno.ENOENT:
        rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
        rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path
        rdir = dir_tree.get_dir_by_name(rdir_name)
        if rdir and rdir.data.ondisk:
            error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H:%M:%S.%f')
            if error_time > rdir.creation_time:
                logger.error(
                    "Result Verify FAILED: Operation {0} failed on {1}/{2} which is on disk".format(
                        incoming_message['action'], rdir_name, rfile_name))
            else:
                logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def list_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
        return
    if incoming_message['error_code'] == errno.ENOENT:
        pass
    else:
        generic_error_handler(logger, incoming_message)


def stat_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
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
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
            else:
                logger.info('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def read_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
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
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
            else:
                logger.info('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def delete_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
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
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
            else:
                logger.info('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)


def rename_fail(logger, incoming_message, dir_tree):
    if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
        'error_message']:
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
                        "Result Verify FAILED: Operation {0} failed on file {1} which is on disk".format(
                            incoming_message['action'], rdir_name + "/" + rfile_name))
            else:
                logger.info('Result verify OK: File {0} is not on disk'.format(rfile_name))
        else:
            logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
    else:
        generic_error_handler(logger, incoming_message)

