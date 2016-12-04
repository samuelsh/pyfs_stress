import os

import errno
import random
import socket

from logger import server_logger
from utils import shell_utils

__author__ = "samuels"

MOUNT_BASE = "/mnt"


class Mounter:
    def __init__(self, server, export, mount_type, prefix, logger=None):
        self.logger = logger
        self.prefix = prefix
        self.mount_type = mount_type
        self.server = server
        self.export = export
        self.mount_points = []

    def mount(self):
        if not self.logger:
            logger = server_logger.ConsoleLogger(socket.gethostname()).logger
        else:
            logger = self.logger
        mount_point = MOUNT_BASE + '/' + self.prefix + '_' + self.export + '_' + self.server
        try:
            os.makedirs('{0}/{1}'.format(MOUNT_BASE, mount_point))
        except OSError as os_error:
            if os_error.errno == errno.EEXIST:
                pass
            else:
                logger.error(os_error)
                raise os_error
        try:
            shell_utils.ShellUtils.run_shell_command('umount', '-fl {0}'.format(mount_point))
        except RuntimeError as e:
            logger.warn(e)
        shell_utils.ShellUtils.run_shell_command('mount',
                                                 '-o nfsvers={0} {1}:/{2} {3}'.format(self.mount_type, self.server,
                                                                                      self.export, mount_point))
        self.mount_points.append(mount_point)
        if not os.path.ismount(mount_point):
            logger.error('mount failed! type: {0} server: {1} export: {2} mount point: {3}'.format(self.mount_type,
                                                                                                   self.server,
                                                                                                   self.export,
                                                                                                   mount_point))
            raise RuntimeError

    def get_random_mountpoint(self):
        return random.choice(self.mount_points)
