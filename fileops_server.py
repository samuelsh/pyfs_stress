#!/usr/bin/env python3
"""
Directory Tree integrity test runner
2016 samuel (c)
"""
import multiprocessing
import random
import subprocess
import time
import argparse
import atexit
import json
import os
import socket
import sys
import errno
import redis
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

SSH_PUB_KEY_PATH = os.environ.get(
    "SSH_PUB_KEY_PATH",
    os.path.expanduser(os.path.join('~', '.ssh', 'id_rsa.pub'))
)


def ensure_ssh_key(pub_key_path):
    """Return the contents of the SSH public key, generating a keypair if needed.

    Also ensures the key is present in the local authorized_keys so that
    localhost connections work without password authentication.
    """
    priv_key_path = pub_key_path.rsplit('.pub', 1)[0]
    ssh_dir = os.path.dirname(pub_key_path)
    if not os.path.isfile(pub_key_path):
        os.makedirs(ssh_dir, mode=0o700, exist_ok=True)
        logger.info(f"SSH key not found at {pub_key_path}, generating a new keypair")
        subprocess.check_call(
            ['ssh-keygen', '-t', 'rsa', '-b', '4096', '-N', '', '-f', priv_key_path],
            stdout=subprocess.DEVNULL,
        )
    with open(pub_key_path, 'r') as f:
        pub_key = f.read().strip()

    auth_keys_path = os.path.join(ssh_dir, 'authorized_keys')
    already_authorized = False
    if os.path.isfile(auth_keys_path):
        with open(auth_keys_path, 'r') as f:
            already_authorized = pub_key in f.read()
    if not already_authorized:
        logger.info("Adding public key to local authorized_keys")
        with open(auth_keys_path, 'a') as f:
            f.write(pub_key + '\n')
        os.chmod(auth_keys_path, 0o644)

    return pub_key


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='vfs_stress Server runner')
    parser.add_argument('cluster', type=str, help='File server name or IP')
    parser.add_argument('-c', '--clients', type=str, nargs='+', help="Space separated list of clients")
    parser.add_argument('-e', '--export', type=str, default="/", help="NFS export name")
    parser.add_argument('--start_vip', type=str, help="Start VIP address range")
    parser.add_argument('--end_vip', type=str, help="End VIP address range")
    parser.add_argument('--tenants', action="store_true", help="Enable MultiTenancy")
    parser.add_argument('-m', '--mtype', type=str, default='nfs3', choices=['nfs3', 'nfs4', 'nfs4.1', 'smb1', 'smb2',
                                                                            'smb3'], help='Mount type')
    parser.add_argument('-l', '--locking', type=str, help='Locking Type', choices=['native', 'application', 'off'],
                        default="native")
    parser.add_argument('--seed', type=int, default=None,
                        help="Random seed for reproducibility. Logged at startup.")
    parser.add_argument('--strict', action='store_true',
                        help="Fail fast on first unexpected filesystem error")
    args = parser.parse_args()
    return args


def load_config():
    with open(os.path.join("server", "config.json")) as f:
        test_config = json.load(f)
    return test_config


def wait_clients_to_start(clients, timeout=120):
    cmd_line = "ps aux | grep dynamo | grep -v grep | wc -l"
    deadline = time.time() + timeout
    while True:
        total_processes = 0
        for client in clients:
            outp = ShellUtils.run_shell_remote_command(client, cmd_line)
            num_processes_per_client = int(outp)
            logger.info(f"SSH command response with {num_processes_per_client} processes on client {client}")
            total_processes += num_processes_per_client
        if total_processes >= config.MAX_WORKERS_PER_CLIENT * len(clients):
            break
        if time.time() > deadline:
            raise RuntimeError(
                f"Timed out after {timeout}s waiting for client workers to start. "
                f"Expected {config.MAX_WORKERS_PER_CLIENT * len(clients)} processes, "
                f"got {total_processes}. Check client logs for errors."
            )
        time.sleep(1)
    logger.info(f"All {len(clients)} clients started. {total_processes // len(clients)} processes per client")


def deploy_clients(clients, access):
    """
    Args:
        access: dict
        clients: list

    Returns: None

    """
    rsa_pub_key = ensure_ssh_key(SSH_PUB_KEY_PATH)
    priv_key_path = SSH_PUB_KEY_PATH.rsplit('.pub', 1)[0]
    for client in clients:
        logger.info(f"Setting SSH connection to {client}")
        ssh_utils.set_key_policy(rsa_pub_key, client, access['user'],
                                 access['password'], key_filename=priv_key_path)
        logger.info(f"Deploying to {client}")
        ShellUtils.run_shell_remote_command_no_exception(client, 'mkdir -p {}'.format(config.DYNAMO_PATH))
        for subdir in ('client', 'config', 'logger', 'utils'):
            ShellUtils.run_shell_command('rsync',
                                         '-avz {} {}:{}'.format(subdir, client, config.DYNAMO_PATH))
        ShellUtils.run_shell_command('rsync',
                                     '-avz requirements.txt {}:{}'.format(client, config.DYNAMO_PATH))
        ShellUtils.run_shell_remote_command_no_exception(client, 'chmod +x {}'.format(config.DYNAMO_BIN_PATH))
        logger.info(f"Setting up venv on {client}")
        venv_path = config.DYNAMO_PATH + '/.venv'
        ShellUtils.run_shell_remote_command_no_exception(
            client,
            f'test -d {venv_path} || python3 -m venv {venv_path}'
        )
        ShellUtils.run_shell_remote_command_no_exception(
            client,
            f'{venv_path}/bin/pip install -q -r {config.DYNAMO_PATH}/requirements.txt'
        )


def run_clients(cluster, clients, export, mtype, start_vip, end_vip, locking_type):
    controller = socket.gethostbyname(socket.gethostname())
    venv_python = config.DYNAMO_PATH + '/.venv/bin/python3'
    dynamo_cmd_line = "{} {} --controller {} --server {} --export {} --mtype {} --start_vip {} --end_vip {} " \
                      "--locking {}".format(venv_python, config.DYNAMO_BIN_PATH, controller, cluster, export, mtype,
                                            start_vip, end_vip, locking_type)
    for client in clients:
        ShellUtils.run_shell_remote_command_background(client, dynamo_cmd_line)
    wait_clients_to_start(clients)


def run_controller(event, dir_tree, test_config, clients_ready_event):
    Controller(event, dir_tree, test_config, clients_ready_event).run()


def run_sub_logger(ip):
    import zmq
    try:
        sub_logger = SUBLogger(ip)
    except zmq.error.ZMQError as e:
        print(f"[sub_logger] Failed to bind: {e}. "
              f"A previous process may still hold the port. "
              f"Try: kill $(lsof -ti :{config.PUBSUB_LOGGER_PORT})")
        return
    poller = zmq.Poller()
    poller.register(sub_logger.sub, zmq.POLLIN)
    while not stop_event.is_set():
        try:
            if poller.poll(timeout=1000):
                topic, message = sub_logger.sub.recv_multipart()
                log_msg = getattr(sub_logger.logger, topic.lower().decode())
                log_msg(message)
        except KeyboardInterrupt:
            break


_child_processes = []


def cleanup(clients=None):
    logger.info("Cleaning up on exit....")
    stop_event.set()
    for proc in _child_processes:
        if proc.is_alive():
            logger.info(f"Terminating child process {proc.name} (pid {proc.pid})")
            proc.terminate()
            proc.join(timeout=5)
            if proc.is_alive():
                proc.kill()
    if clients:
        for client in clients:
            logger.info(f"{client}: Killing workers")
            ShellUtils.run_shell_remote_command_no_exception(client, 'pkill -9 -f dynamo')
            logger.info(f"{client}: Unmounting")
            ShellUtils.run_shell_remote_command_no_exception(client, 'sudo umount -fl /mnt/{}'.format('VFS*'))
            logger.info(f"{client}: Removing mountpoint folder/s")
            ShellUtils.run_shell_remote_command_no_exception(client, 'sudo rm -fr /mnt/{}'.format('VFS*'))


def main():
    from server.journal import OperationJournal

    file_names = None
    clients_ready_event = multiprocessing.Manager().Event()
    args = get_args()

    seed = args.seed if args.seed is not None else int(time.time() * 1000) % (2**31)
    random.seed(seed)
    logger.info(f"Random seed: {seed} (use --seed {seed} to reproduce)")

    try:
        with open(config.FILE_NAMES_PATH, 'r') as f:
            file_names = f.readlines()
    except IOError as io_error:
        if io_error.errno == errno.ENOENT:
            pass
    dir_tree = dirtree.DirTree(file_names)
    logger.debug(f"{__name__} Logger initialised {logger}")
    atexit.register(cleanup, clients=args.clients)
    clients_list = args.clients
    logger.info("Loading Test Configuration")
    test_config = load_config()
    test_config['_journal'] = OperationJournal()
    test_config['_strict'] = args.strict
    logger.info(f"Operation journal: {test_config['_journal'].path}")
    logger.info("Setting passwordless SSH connection")
    rsa_pub_key = ensure_ssh_key(SSH_PUB_KEY_PATH)
    priv_key_path = SSH_PUB_KEY_PATH.rsplit('.pub', 1)[0]
    ssh_utils.set_key_policy(rsa_pub_key, args.cluster, test_config['access']['server']['user'],
                             test_config['access']['server']['password'],
                             key_filename=priv_key_path)

    if args.locking == 'application':
        logger.info("Flushing locking DB")
        locking_db = redis.StrictRedis(**redis_config)
        locking_db.flushdb()
    else:
        logger.info(f"Locking mode is '{args.locking}', skipping Redis")
    logger.info("Starting SUB Logger process")
    sub_logger_process = Process(target=run_sub_logger, name="sub_logger",
                                 args=(socket.gethostbyname(socket.gethostname()),))
    sub_logger_process.daemon = True
    sub_logger_process.start()
    _child_processes.append(sub_logger_process)
    logger.info("Controller started")
    time.sleep(10)
    deploy_clients(clients_list, test_config['access']['client'])
    logger.info(f"Done deploying clients: {clients_list}")
    run_clients(args.cluster, clients_list, args.export, args.mtype, args.start_vip, args.end_vip, args.locking)
    clients_ready_event.set()
    logger.info("Dynamo started on all clients ....")
    logger.info("Starting controller")
    controller_process = Process(target=run_controller, name="controller",
                                 args=(stop_event, dir_tree, test_config, clients_ready_event))
    controller_process.start()
    _child_processes.append(controller_process)
    controller_process.join()
    logger.info('All done')


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
