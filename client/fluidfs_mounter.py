import os

import errno
import random
import socket

import sys

sys.path.append('/qa/dynamo')
from logger import server_logger
from utils import shell_utils

__author__ = "samuels"

MOUNT_BASE = "/mnt"


class Mounter:
    def __init__(self, server, export, mount_type, prefix, logger=None, nodes=None, domains=None):
        self.domains = domains
        self.nodes = nodes
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
        if self.mount_type == 'nfs3':
            shell_utils.FSUtils.mount_fsd(self.server, '/' + self.export, self.nodes, self.domains, self.mount_type,
                                          self.prefix, '6')
            for i in range(self.nodes):
                for j in range(self.domains):
                    mount_point = '/mnt/%s-node%d.%s-%d' % ('DIRSPLIT', i, self.server, j)
                    self.mount_points.append(mount_point)
                    if not os.path.ismount(mount_point):
                        logger.error('mount_fsd failed! Mount point {0} not mounted'.format(mount_point))
                        raise RuntimeError
        else:
            try:
                os.makedirs('{0}'.format(mount_point))
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
