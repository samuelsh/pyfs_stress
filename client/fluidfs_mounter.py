import os

import errno
import socket

from logger import server_logger
from utils import shell_utils

__author__ = "samuels"

MOUNT_BASE = "/mnt"


def mount(server, export, mount_type, prefix, logger=None, nodes=None, domains=None):
    if not logger:
        logger = server_logger.ConsoleLogger(socket.gethostname()).logger
    mount_point = prefix + '_' + export
    if nodes and domains:
        if mount_type == 3:
            shell_utils.FSUtils.mount_fsd(server, '/' + export, nodes, domains, 'nfs3', prefix, '6')
    else:
        try:
            os.makedirs('{0}/{1}'.format(MOUNT_BASE, mount_point))
        except OSError as os_error:
            if os_error.errno == errno.EEXIST:
                pass
            else:
                logger.error(os_error)
                raise os_error
        try:
            shell_utils.ShellUtils.run_shell_command('umount', '-fl {0}/{1}'.format(MOUNT_BASE, mount_point))
        except Exception as e:
            logger.warn(e)
        shell_utils.ShellUtils.run_shell_command('mount',
                                                 '-o nfsvers={0} {1}/{2}:/{3}'.format(mount_type, server, export,
                                                                                      mount_point))
