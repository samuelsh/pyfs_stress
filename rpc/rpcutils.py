"""
RPC helper functions
"""
from shell_utils import ShellUtils


def deploy_rpyc_server(cluster_node):
    ShellUtils.run_shell_remote_command(cluster_node, "yes | easy_install rpyc")


def start_rpyc_server(cluster_node):
    ShellUtils.run_shell_remote_command_background(cluster_node, "rpyc_classic.py &")


def stop_rpyc_server(cluster_node):
    ShellUtils.run_shell_remote_command(cluster_node, "pkill -f rpyc_classic.py")
