#!/usr/bin/env python
"""
Directory Tree integrity test runner
2016 samuelsh (c)
"""
import argparse
import atexit
import json
import os
import socket
import sys
import traceback
from multiprocessing import Event
from multiprocessing import Process

import errno
import zmq

import config
from logger.pubsub_logger import SUBLogger
from logger.server_logger import ConsoleLogger
from server.async_controller import Controller
from tree import dirtree
from utils import shell_utils
from utils.shell_utils import ShellUtils


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='FileOps Server starter - 2016 samuels(c)')
    parser.add_argument('-c', '--cluster', type=str, required=True, help='Cluster name')
    parser.add_argument('--clients', type=str, nargs='+', required=True, help="Space separated list of clients")
    parser.add_argument('-e', '--export', type=str, default="vol0", help="Space separated list of clients")
    parser.add_argument('--tenants', action="store_true", help="Enable MultiTenancy")
    parser.add_argument('-m', '--mtype', type=str, default='nfs3', choices=['nfs3', 'nfs4', 'nfs4.1', 'smb1', 'smb2',
                                                                            'smb3'], help='Mount type')
    args = parser.parse_args()
    return args


def load_config():
    with open(os.path.join("server", "config.json")) as f:
        test_config = json.load(f)
    return test_config


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


def run_clients(cluster, clients, export, active_nodes, domains, mtype):
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
                                                       ' --nodes {4} --domains {5} --mtype {6} &'.format(
                                                           config.DYNAMO_BIN_PATH, controller, cluster, export,
                                                           active_nodes, domains, mtype))


def run_controller(event, dir_tree, test_config):
    Controller(event, dir_tree, test_config).run()


def run_sub_logger(ip, event):
    sub_logger = SUBLogger(ip)
    while not event.is_set():
        try:
            topic, message = sub_logger.sub.recv_multipart(flags=zmq.NOBLOCK)
            log_msg = getattr(sub_logger.logger, topic.lower())
            log_msg(message)
        except zmq.ZMQError as zmq_error:
            if zmq_error.errno == zmq.EAGAIN:
                pass
        except KeyboardInterrupt:
            event.set()


def cleanup(logger, clients=None):
    logger.info("Cleaning up on exit....")
    if clients:
        for client in clients:
            try:
                logger.info("{0}: Killing workers".format(client))
                ShellUtils.run_shell_remote_command(client, 'pkill -f python')
                logger.info("{0}: Unmounting".format(client))
                ShellUtils.run_shell_remote_command(client, 'umount -fl /mnt/{0}'.format('DIRSPLIT*'))
                logger.info("{0}: Removing mountpoint folder/s".format(client))
                ShellUtils.run_shell_remote_command(client, 'rm -fr /mnt/{0}'.format('DIRSPLIT*'))
            except RuntimeError:
                pass


def main():
    file_names = None
    active_nodes = 0
    domains = 0
    args = get_args()
    stop_event = Event()
    try:
        with open(config.FILE_NAMES_PATH, 'r') as f:  # If file with names isn't exists, we'll just create random files
            file_names = f.readlines()
    except IOError as io_error:
        if io_error.errno == errno.ENOENT:
            pass
    dir_tree = dirtree.DirTree(file_names)
    logger = ConsoleLogger(__name__).logger
    logger.debug("{0} Logger initialised {1}".format(__name__, logger))
    atexit.register(cleanup, logger, clients=args.clients)
    clients_list = args.clients
    logger.info("Loading Test Configuration")
    test_config = load_config()
    logger.info("Setting passwordless SSH connection")
    shell_utils.ShellUtils.run_shell_script("python utils/ssh_utils.py", "{0} -U {1} -P {2}".format(args.cluster,
                                                                                             test_config['access'][
                                                                                                 'user'],
                                                                                             test_config['access'][
                                                                                                 'password']), False)
    if not args.tenants:
        logger.info("Getting cluster params...")
        active_nodes = shell_utils.FSUtils.get_active_nodes_num(args.cluster)
        logger.debug("Active Nodes: %s" % active_nodes)
        domains = shell_utils.FSUtils.get_domains_num(args.cluster)
        logger.debug("FSD domains: %s" % domains)
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, args=(stop_event, dir_tree, test_config))
    controller_process.start()
    sub_logger_process = Process(target=run_sub_logger,
                                 args=(socket.gethostbyname(socket.gethostname()), stop_event,))
    sub_logger_process.start()
    logger.info("Controller started")
    deploy_clients(clients_list)
    logger.info("Done deploying clients: {0}".format(clients_list))
    run_clients(args.cluster, clients_list, args.export, active_nodes, domains, args.mtype)
    logger.info("Dynamo started on all clients ....")
    controller_process.join()
    print('All done')


# Start program
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('CTRL + C was pressed. Waiting for Controller to stop...')
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
