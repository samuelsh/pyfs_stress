"""
Directory Tree integrity test runner
2016 samuelsh (c)
"""
import argparse
import traceback
import sys

from multiprocessing import Event
from multiprocessing import Process

import time

from logger import Logger
from server.controller import Controller
from shell_utils import ShellUtils

SET_SSH_PATH = "/zebra/qa/qa-util-scripts/set-ssh-client"


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
        ShellUtils.run_shell_script(SET_SSH_PATH, client)
        ShellUtils.run_shell_remote_command_no_exception(client, 'mkdir -p /qa/dynamo')
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('client', client, '/qa'))
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('config', client, '/qa'))
        ShellUtils.run_shell_command('scp', '-r {0} {1}:{2}'.format('logger', client, '/qa'))


def run_clients(clients):
    """

    Args:
        clients: list

    Returns:

    """
    for client in clients:
        ShellUtils.run_shell_remote_command_background(client, 'python /qa{0}'.format('/client/dynamo_starter.py &'))


def run_controller(logger, event):
    Controller(logger, event).run()


def init_test(args):
    pass


def main():
    args = get_args()
    logger = Logger().logger
    stop_event = Event()
    logger.debug("Logger initialised {0}".format(logger))
    clients_list = args.clients
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, args=(logger, stop_event,))
    controller_process.start()
    logger.info("Controller started")
    #clients = [Dynamo(logger, stop_event) for _ in clients_list]
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
