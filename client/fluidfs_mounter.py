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
    def __init__(self, server, export, mount_type, prefix, **kwargs):
        self.domains = 0
        self.nodes = 0
        self.logger = None
        self.prefix = prefix
        self.mount_type = mount_type
        self.server = server
        self.export = export
        self.mount_points = []

        if 'logger' in kwargs:
            self.logger = kwargs['logger']
        else:
            self.logger = server_logger.ConsoleLogger(socket.gethostname()).logger

        if 'nodes' in kwargs:
            self.nodes = kwargs['nodes']
        if 'domains' in kwargs:
            self.domains = kwargs['domains']

    def mount(self):
        mount_point = MOUNT_BASE + '/' + self.prefix + '_' + self.export + '_' + self.server
        # if mount type is nfs3 and number of nodes and domain makes sense, we'll try to mount multi-domain
        if self.mount_type == 'nfs3' and self.nodes and self.domains:
            shell_utils.FSUtils.mount_fsd(self.server, '/' + self.export, self.nodes, self.domains, self.mount_type,
                                          self.prefix, '6')
            for i in range(self.nodes):
                for j in range(self.domains):
                    mount_point = '/mnt/%s-node%d.%s-%d' % ('DIRSPLIT', i, self.server, j)
                    self.mount_points.append(mount_point)
                    if not os.path.ismount(mount_point):
                        self.logger.error('mount_fsd failed! Mount point {0} not mounted'.format(mount_point))
                        raise RuntimeError
        else:
            try:
                os.makedirs('{0}'.format(mount_point))
            except OSError as os_error:
                if os_error.errno == errno.EEXIST:
                    pass
                else:
                    self.logger.error(os_error)
                    raise os_error
            try:
                shell_utils.ShellUtils.run_shell_command('umount', '-fl {0}'.format(mount_point))
            except RuntimeError as e:
                self.logger.warn(e)
            mtype = self.mount_type.strip('nfs')
            shell_utils.ShellUtils.run_shell_command('mount',
                                                     '-o nfsvers={0} {1}:/{2} {3}'.format(mtype, self.server,
                                                                                          self.export, mount_point))
            self.mount_points.append(mount_point)
            if not os.path.ismount(mount_point):
                self.logger.error('mount failed! type: {0} server: {1} export: {2} mount point: {3}'.format(self.mount_type,
                                                                                                       self.server,
                                                                                                       self.export,
                                                                                                       mount_point))
                raise RuntimeError

    def get_random_mountpoint(self):
        return random.choice(self.mount_points)
