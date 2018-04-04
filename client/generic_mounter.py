import json
import os

import errno
import random
import socket

import sys

import time

sys.path.append('/qa/dynamo')
from logger import server_logger
from utils import shell_utils
from utils import ip_utils
from config import DYNAMO_PATH

__author__ = "samuel (c)"

MOUNT_BASE = "/home"
NUMBER_OF_RETRIES = 3


class Mounter:
    def __init__(self, server, export, mount_type, prefix, **kwargs):
        self.prefix = prefix
        self.mount_type = mount_type
        self.server = server
        self.export = export
        self.mount_points = []
        self.logger = None
        self.num_of_retries = NUMBER_OF_RETRIES

        if 'logger' in kwargs:
            self.logger = kwargs['logger']
        else:
            self.logger = server_logger.ConsoleLogger(socket.gethostname()).logger

        try:
            self.vip_range = [ip for ip in ip_utils.range_ipv4(kwargs['start_vip'], kwargs['end_vip'])]
        except (KeyError, AttributeError):
            self.vip_range = None

    def mount(self):
        mount_point = MOUNT_BASE + '/' + self.prefix + '_' + self.server
        try:
            os.makedirs('{}'.format(mount_point))
        except OSError as os_error:
            if os_error.errno == errno.EEXIST:
                pass
            else:
                self.logger.error(os_error)
                raise os_error
        try:
            shell_utils.ShellUtils.run_shell_command('umount', '-fl {}'.format(mount_point))
        except RuntimeError as e:
            self.logger.warn(e)
        if 'nfs' in self.mount_type:
            mtype = self.mount_type.strip('nfs')
            shell_utils.ShellUtils.run_shell_command('mount',
                                                     '{}:/{} {}'.format(self.server, self.export, mount_point))
        elif 'smb' in self.mount_type:
            with open(DYNAMO_PATH + "/client/smb_params.json") as f:
                smb_params = json.load(f)
            mtype = self.mount_type.strip('smb')
            shell_utils.ShellUtils.run_shell_command('mount', '-t cifs //{0}/{1} {2} -o vers={3},user={4}/{5}%{6}'.
                                                     format(self.server, self.export, mount_point, mtype,
                                                            smb_params['domain'], smb_params['user'],
                                                            smb_params['password']))
        self.mount_points.append(mount_point)
        if not os.path.ismount(mount_point):
            self.logger.error('mount failed! type: {0} server: {1} export: {2} mount point: {3}'.
                              format(self.mount_type, self.server, self.export, mount_point))
            raise RuntimeError

    def get_random_mountpoint(self):
        return random.choice(self.mount_points)

    def mount_all_vips(self):
        for vip in self.vip_range:
            mount_point = '/'.join([MOUNT_BASE, self.prefix + '_' + vip])
            try:
                os.makedirs('{}'.format(mount_point))
            except OSError as os_error:
                if os_error.errno == errno.EEXIST:
                    pass
                else:
                    self.logger.error(os_error)
                    raise os_error
            try:
                shell_utils.ShellUtils.run_shell_command('umount', '-fl {}'.format(mount_point))
            except RuntimeError as e:
                self.logger.warn(e)
            if 'nfs' in self.mount_type:
                mtype = self.mount_type.strip('nfs')
                export = "" if self.export == '/' else self.export
                try:
                    # shell_utils.ShellUtils.run_shell_command('mount',
                    #                                          '{}:/{} {}'.format(vip, export, mount_point))
                    self.retry_method(shell_utils.ShellUtils.run_shell_command, 'mount',
                                      '{}:/{} {}'.format(vip, export, mount_point))
                except RuntimeError as e:
                    self.logger.error("Mount error {} {}".format(socket.gethostname(), e))

            elif 'smb' in self.mount_type:
                with open(DYNAMO_PATH + "/client/smb_params.json") as f:
                    smb_params = json.load(f)
                mtype = self.mount_type.strip('smb')
                shell_utils.ShellUtils.run_shell_command('mount', '-t cifs //{0}/{1} {2} -o vers={3},user={4}/{5}%{6}'.
                                                         format(vip, self.export, mount_point, mtype,
                                                                smb_params['domain'], smb_params['user'],
                                                                smb_params['password']))
            self.mount_points.append(mount_point)
            if not os.path.ismount(mount_point):
                self.logger.error('mount failed! type: {} server: {} export: {} mount point: {}'.
                                  format(self.mount_type, self.server, self.export, mount_point))
                raise RuntimeError

    def retry_method(self, callback, command, params):
        for retry in range(self.num_of_retries):
            try:
                callback(command, params)
            except RuntimeError as e:
                if retry < self.num_of_retries:
                    self.logger.warn("Command failed, waiting 5 sec before retry ...")
                    time.sleep(5)
                else:
                    self.logger.error("Number of retries exceeded {} due to {}. "
                                      "{} failed".format(self.num_of_retries, e, callback.__name__))
            else:
                break
