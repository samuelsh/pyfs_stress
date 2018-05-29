#!/usr/bin/env python3.6
"""
Directory Tree integrity test runner
2016 samuel (c)
"""
import argparse
import atexit
import json
import os
import socket
import sys
import errno

import redis
import zmq
import config
from multiprocessing import Event
from multiprocessing import Process

from config.redis_config import redis_config
from logger.pubsub_logger import SUBLogger
from logger.server_logger import ConsoleLogger
from server.async_controller import Controller
from tree import dirtree
from utils import ssh_utils
from utils.shell_utils import ShellUtils

stop_event = Event()
logger = ConsoleLogger(__name__).logger


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='pyFstress Server runner')
    parser.add_argument('cluster', type=str, help='File server name or IP')
    parser.add_argument('-c', '--clients', type=str, nargs='+', help="Space separated list of clients")
    parser.add_argument('-e', '--export', type=str, default="/", help="NFS export name")
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    parser.add_argument('--tenants', action="store_true", help="Enable MultiTenancy")
    parser.add_argument('-m', '--mtype', type=str, default='nfs3', choices=['nfs3', 'nfs4', 'nfs4.1', 'smb1', 'smb2',
                                                                            'smb3'], help='Mount type')
    args = parser.parse_args()
    return args


def load_config():
    with open(os.path.join("server", "config.json")) as f:
        test_config = json.load(f)
    return test_config


def deploy_clients(clients, access):
    """
    Args:
        access: dict
        clients: list

    Returns: None

    """
    with open(os.path.expanduser(os.path.join('~', '.ssh', 'id_rsa.pub')), 'r') as f:
        rsa_pub_key = f.read()
    for client in clients:
        logger.info("Setting SSH connection to {0}".format(client))
        ssh_utils.set_key_policy(rsa_pub_key, client, logger, access['user'],
                                 access['password'])
        logger.info("Deploying to {0}".format(client))
        ShellUtils.run_shell_remote_command_no_exception(client, 'mkdir -p {}'.format(config.DYNAMO_PATH))
        ShellUtils.run_shell_command('rsync',
                                     '-avz {} {}:{}'.format('client', client, config.DYNAMO_PATH))
        ShellUtils.run_shell_command('rsync',
                                     '-avz {} {}:{}'.format('config', client, config.DYNAMO_PATH))
        ShellUtils.run_shell_command('rsync',
                                     '-avz {} {}:{}'.format('logger', client, config.DYNAMO_PATH))
        ShellUtils.run_shell_command('rsync',
                                     '-avz {} {}:{}'.format('utils', client, config.DYNAMO_PATH))
        ShellUtils.run_shell_remote_command_no_exception(client, 'chmod +x {}'.format(config.DYNAMO_BIN_PATH))


def run_clients(cluster, clients, export, mtype, start_vip, end_vip):
    """

    Args:
        mtype: str
        domains: int
        active_nodes: int
        export: str
        cluster: str
        clients: list
        start_vip: str
        end_vip: str

    Returns:

    """
    #  Will explicitly pass public IP of the controller to clients since we won't rely on DNS existence
    controller = socket.gethostbyname(socket.gethostname())
    dynamo_cmd_line = "{} --controller {} --server {} --export {} --mtype {} --start_vip {} --end_vip {}". \
        format(config.DYNAMO_BIN_PATH, controller, cluster, export, mtype, start_vip, end_vip)
    for client in clients:
        ShellUtils.run_shell_remote_command_background(client, dynamo_cmd_line)


def run_controller(event, dir_tree, test_config):
    Controller(event, dir_tree, test_config).run()


def run_sub_logger(ip):
    sub_logger = SUBLogger(ip)
    while not stop_event.is_set():
        try:
            topic, message = sub_logger.sub.recv_multipart(flags=zmq.NOBLOCK)
            log_msg = getattr(sub_logger.logger, topic.lower().decode())
            log_msg(message)
        except zmq.ZMQError as zmq_error:
            if zmq_error.errno == zmq.EAGAIN:
                pass
        except KeyboardInterrupt:
            pass


def cleanup(clients=None):
    logger.info("Cleaning up on exit....")
    if clients:
        for client in clients:
            logger.info("{}: Killing workers".format(client))
            ShellUtils.run_shell_remote_command_no_exception(client, 'pkill -9 -f dynamo')
            logger.info("{}: Unmounting".format(client))
            ShellUtils.run_shell_remote_command_no_exception(client, 'sudo umount -fl /mnt/{}'.format('FSTRESS*'))
            logger.info("{}: Removing mountpoint folder/s".format(client))
            ShellUtils.run_shell_remote_command_no_exception(client, 'sudo rm -fr /mnt/{}'.format('FSTRESS*'))


def main():
    file_names = None
    args = get_args()
    try:
        with open(config.FILE_NAMES_PATH, 'r') as f:  # If file with names isn't exists, we'll just create random files
            file_names = f.readlines()
    except IOError as io_error:
        if io_error.errno == errno.ENOENT:
            pass
    dir_tree = dirtree.DirTree(file_names)
    logger.debug("{0} Logger initialised {1}".format(__name__, logger))
    atexit.register(cleanup, clients=args.clients)
    clients_list = args.clients
    logger.info("Loading Test Configuration")
    test_config = load_config()
    logger.info("Setting passwordless SSH connection")
    with open(os.path.expanduser(os.path.join('~', '.ssh', 'id_rsa.pub')), 'r') as f:
        rsa_pub_key = f.read()
    ssh_utils.set_key_policy(rsa_pub_key, args.cluster, test_config['access']['server']['user'],
                             test_config['access']['server']['password'])

    logger.info("Flushing locking DB")
    locking_db = redis.StrictRedis(**redis_config)
    locking_db.flushdb()
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, args=(stop_event, dir_tree, test_config))
    controller_process.start()
    sub_logger_process = Process(target=run_sub_logger,
                                 args=(socket.gethostbyname(socket.gethostname()),))
    sub_logger_process.start()
    logger.info("Controller started")
    deploy_clients(clients_list, test_config['access']['client'])
    logger.info("Done deploying clients: {0}".format(clients_list))
    run_clients(args.cluster, clients_list, args.export, args.mtype, args.start_vip, args.end_vip)
    logger.info("Dynamo started on all clients ....")
    controller_process.join()
    print('All done')


# Start program
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('CTRL + C was pressed. Waiting for Controller to stop...')
        stop_event.set()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
