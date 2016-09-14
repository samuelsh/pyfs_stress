"""
Directory Tree integrity test runner
2016 samuelsh (c)
"""
import argparse
import socket
import sys
import time
import traceback
from multiprocessing import Event
from multiprocessing import Process

import config
from logger import Logger
from server.controller import Controller
from tree import dirtree
from utils.shell_utils import ShellUtils


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='Test Runner script')
    parser.add_argument('-c', '--cluster', type=str, required=True, help='Cluster name')
    parser.add_argument('--clients', type=str, nargs='+', required=True, help="Space separated list of clients")
    parser.add_argument('-e', '--export', type=str, default="vol0", help="Space separated list of clients")
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
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('utils', client, '{0}'.format(config.DYNAMO_PATH)))


def run_clients(cluster, clients, export):
    """

    Args:
        cluster: str
        clients: list

    Returns:

    """
    controller = socket.gethostname()
    for client in clients:
        ShellUtils.run_shell_remote_command_background(client,
                                                       'python {0} --controller {1} --server {2} --export {3} &'.format(
                                                           config.DYNAMO_BIN_PATH, controller, cluster, export))


def run_controller(logger, event, dir_tree):
    Controller(logger, event, dir_tree).run()


def main():
    args = get_args()
    logger = Logger().logger
    stop_event = Event()
    dir_tree = dirtree.DirTree()
    logger.debug("Logger initialised {0}".format(logger))
    clients_list = args.clients
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, args=(logger, stop_event, dir_tree,))
    controller_process.start()
    logger.info("Controller started")
    deploy_clients(clients_list)
    logger.info("Done deploying clients: {0}".format(clients_list))
    run_clients(args.cluster, clients_list, args.export)
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
