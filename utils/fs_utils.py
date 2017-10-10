"""
Generic File System Helper methods
2016 - samuels(c)
"""
import os

from utils.shell_utils import ShellUtils


def mount(server, export, mount_point, mtype):
    try:
        ShellUtils.run_shell_command("mount", "-o nfsvers={0} {1}:/{2} {3}".format(mtype, server, export, mount_point))
    except OSError:
        return False
    return True


def umount(mount_point):
    try:
        ShellUtils.run_shell_command("umount", "-fl {0}".format(mount_point))
    except OSError:
        return False
    return True


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)
