"""
Directory Tree integrity test runner
2016 samuelsh (c)
"""
import argparse
import hashlib
import socket
import traceback
import sys

from multiprocessing import Event
from multiprocessing import Process

import time

import treelib

import config
from logger import Logger
from server.controller import Controller
from shell_utils import ShellUtils, StringUtils


def build_recursive_tree(tree, base, depth, width):
    """
    Args:
        tree: Tree
        base: Node
        depth: int
        width: int
    """
    if depth >= 0:
        depth -= 1
        for i in xrange(width):
            directory = Directory()
            tree.create_node("{0}".format(directory.name), "{0}".format(hashlib.md5(directory.name)),
                             parent=base.identifier, data=directory)
        dirs_nodes = tree.children(base.identifier)
        for dir_node in dirs_nodes:
            newbase = tree.get_node(dir_node.identifier)
            build_recursive_tree(tree, newbase, depth, width)
    else:
        return


class Directory(object):
    def __init__(self):
        self._name = StringUtils.get_random_string_nospec(64)
        self.files = [File() for _ in xrange(config.MAX_FILES_PER_DIR)]  # Each directory contains 1000 files

    @property
    def name(self):
        return self._name


class File(object):
    def __init__(self):
        self._name = StringUtils.get_random_string_nospec(64)

    @property
    def name(self):
        return self._name


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='Test Runner script')
    parser.add_argument('-c', '--cluster', type=str, required=True, help='Cluster name')
    parser.add_argument('--clients', type=str, nargs='+', required=True, help="Space separated list of clients")
    parser.add_argument('-m', '--mtype', type=str, default="nfs3", help='Mount type')
    args = parser.parse_args()
    return args


def deploy_clients(clients):
    """
    Args:
        clients: list

    Returns:

    """
    for client in clients:
        ShellUtils.run_shell_script(config.SET_SSH_PATH, client)
        ShellUtils.run_shell_remote_command_no_exception(client, 'mkdir -p {0}'.format(config.DYNAMO_PATH))
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('client', client, '{0}'.format(config.DYNAMO_PATH)))
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('config', client, '{0}'.format(config.DYNAMO_PATH)))
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('logger', client, '{0}'.format(config.DYNAMO_PATH)))


def run_clients(clients):
    """

    Args:
        clients: list

    Returns:

    """
    controller = socket.gethostname()
    for client in clients:
        ShellUtils.run_shell_remote_command_background(client,
                                                       'python {0} --controller {1} &'.format(config.DYNAMO_BIN_PATH,
                                                                                              controller))


def run_controller(logger, event):
    Controller(logger, event).run()


def main():
    args = get_args()
    logger = Logger().logger
    stop_event = Event()
    dir_tree = treelib.Tree()
    logger.debug("Logger initialised {0}".format(logger))
    clients_list = args.clients
    logger.info("Building Directory Tree data structure, can tike a while...")
    tree_base = dir_tree.create_node('Root', 'root')
    build_recursive_tree(dir_tree, tree_base, 1, 10)
    logger.info("Building Directory Tree data structure is initialised, proceeding ....")
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, args=(logger, stop_event,))
    controller_process.start()
    logger.info("Controller started")
    deploy_clients(clients_list)
    logger.info("Done deploying clients: {0}".format(clients_list))
    run_clients(clients_list)
    logger.info("Dynamo started on all clients ....")

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
    print('waiting for Controller to stop...')
    controller_process.join()
    print('all done')


# Start program
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
