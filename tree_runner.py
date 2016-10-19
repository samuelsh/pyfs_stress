"""
Directory Tree integrity test runner
2016 samuelsh (c)
"""
import argparse
import logging
import socket
import sys
import time
import traceback
from multiprocessing import Event
from multiprocessing import Process

import zmq

import config
from logger import server_logger
from logger.pubsub_logger import SUBLogger
from server.controller import Controller
from tree import dirtree
from utils import shell_utils
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


def run_clients(cluster, clients, export, active_nodes, domains):
    """

    Args:
        domains: int
        active_nodes: int
        export: str
        cluster: str
        clients: list

    Returns:

    """
    controller = socket.gethostname()
    for client in clients:
        ShellUtils.run_shell_remote_command_background(client,
                                                       'python {0} --controller {1} --server {2} --export {3}'
                                                       ' --nodes {4} --domains {5} &'.format(
                                                           config.DYNAMO_BIN_PATH, controller, cluster, export,
                                                           active_nodes, domains))


def run_controller(logger, event, dir_tree):
    Controller(logger, event, dir_tree).run()


def run_pubsub_logger(ip, event):
    sub_logger = SUBLogger(ip)
    while not event.is_set():
        try:
            topic, message = sub_logger.sub.recv_multipart(flags=zmq.NOBLOCK)
            log_msg = getattr(logging, topic.lower())
            log_msg(message)
        except zmq.ZMQError as zmq_error:
            if zmq_error.errno == zmq.EAGAIN:
                pass


def main():
    args = get_args()
    logger = server_logger.Logger().logger
    stop_event = Event()
    dir_tree = dirtree.DirTree()
    logger.debug("Logger initialised {0}".format(logger))
    clients_list = args.clients
    logger.info("Setting passwordless SSH connection")
    shell_utils.ShellUtils.run_shell_script("/zebra/qa/qa-util-scripts/set-ssh-python", args.cluster, False)
    logger.info("Getting cluster params...")
    active_nodes = shell_utils.FSUtils.get_active_nodes_num(args.cluster)
    logger.debug("Active Nodes: %s" % active_nodes)
    domains = shell_utils.FSUtils.get_domains_num(args.cluster)
    logger.debug("FSD domains: %s" % domains)
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, args=(logger, stop_event, dir_tree,))
    controller_process.start()
    pubsub_looger_process = Process(target=run_pubsub_logger,
                                    args=(socket.gethostbyname(socket.gethostname()), stop_event,))
    pubsub_looger_process.start()
    logger.info("Controller started")
    deploy_clients(clients_list)
    logger.info("Done deploying clients: {0}".format(clients_list))
    run_clients(args.cluster, clients_list, args.export, active_nodes, domains)
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
