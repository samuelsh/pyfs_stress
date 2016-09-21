import os
import random
import subprocess
from string import printable, digits, letters

__author__ = 'samuels'

QA_BASHLIB_PATH = "/zebra/qa/qa-bashlib/qa-bashlib.sh"
PARAMS_FUNCTIONS = "/zebra/qa/qa-functions/6.0-params-functions.sh"
GLOBAL_SSH_PATH = "/zebra/qa/qa-util-scripts/global_ssh"
SSH_PATH = "/zebra/qa/tools/gssh"


class StringUtils:
    def __init__(self):
        pass

    @staticmethod
    def get_random_string(length):
        return ''.join(random.choice(printable) for _ in range(length))

    @staticmethod
    def get_random_string_nospec(length):
        return ''.join(random.choice(digits + letters) for _ in range(length))


class ShellUtils:
    def __init__(self):
        pass

    @staticmethod
    def pipe_grep(source_process, grep_pattern):
        p2 = subprocess.Popen(["grep", grep_pattern], stdin=source_process.stdout, stdout=subprocess.PIPE)
        source_process.stdout.close()  # Allow source_process to receive a SIGPIPE if p2 exits.
        output = p2.communicate()[0]
        return output.strip()

    @staticmethod
    def run_bash_function(library_path, function_name, params):
        cmdline = ['bash', '-c', '. %s; %s %s' % (library_path, function_name, params)]
        p = subprocess.Popen(cmdline,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                function_name, p.returncode, stdout, stderr))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def run_remote_bash_function(remote_host, library_path, function_name, params):
        cmdline = ['ssh', '-nx', remote_host, 'bash', '-c', '. %s; %s %s' % (library_path, function_name, params)]
        p = subprocess.Popen(cmdline,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                function_name, p.returncode, stdout, stderr))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def run_shell_script(script, params, stdout=True):
        FNULL = open(os.devnull, 'w')
        if not stdout:
            p = subprocess.call([script, params], stdout=FNULL)
        else:
            p = subprocess.call([script, params])
        return p

    @staticmethod
    def run_shell_script_remote(remote_host, script, params, stdout=True):
        FNULL = open(os.devnull, 'w')
        if not stdout:
            p = subprocess.call(['ssh', '-nx', remote_host, script, params], stdout=FNULL)
        else:
            p = subprocess.call(['ssh', '-nx', remote_host, script, params])
        return p

    @staticmethod
    def run_shell_command(cmd, params, sep=' ', stdout=subprocess.PIPE):
        cmdline = [cmd]
        cmdline = cmdline + params.split(sep)
        p = subprocess.Popen(cmdline, stdout=stdout, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                cmd, p.returncode, stdout, stderr))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def get_shell_remote_command(remote_host, remote_cmd):
        """
        :rtype: subprocess.Popen
        """
        p = subprocess.Popen([SSH_PATH, '-nx', remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        return p

    @staticmethod
    def run_shell_remote_command(remote_host, remote_cmd):
        p = subprocess.Popen([SSH_PATH, '-nx', remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                remote_cmd, p.returncode, stdout, stderr))
        return stdout.strip()  # This is the stdout from the shell command

    @staticmethod
    def run_shell_remote_command_multiline(remote_host, remote_cmd):
        p = subprocess.Popen(['ssh', '-nx', remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError("%r failed, status code %s stdout %r stderr %r" % (
                remote_cmd, p.returncode, stdout, stderr))
        return stdout.splitlines()  # This is the stdout from the shell command

    @staticmethod
    def run_shell_remote_command_background(remote_host, remote_cmd):
        subprocess.Popen(['ssh', '-nx', remote_host, remote_cmd])

    @staticmethod
    def run_shell_remote_command_no_exception(remote_host, remote_cmd):
        p = subprocess.Popen(['ssh', '-nx', remote_host, remote_cmd], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            return False  # This is the stdout from the shell command
        return True


class FSUtils:
    def __init__(self):
        pass

    @staticmethod
    def get_disd(cluster, filename):
        return ShellUtils.run_shell_remote_command(cluster, "0cat -z %s" % filename)

    @staticmethod
    def get_fsid(cluster, vol_name):
        return ShellUtils.run_shell_remote_command(cluster, "exavol get-id %s" % vol_name)

    @staticmethod
    def get_disd(cluster, filename):
        return ShellUtils.run_shell_remote_command(cluster, "0cat -z %s" % filename)

    @staticmethod
    def get_file_path_on_cluster(cluster, dsid):
        return ShellUtils.run_shell_remote_command(cluster, "dsid2name %s" % dsid).split(' ')[2]

    @staticmethod
    def get_data_pattern():
        pass

    @staticmethod
    def is_deduped(cluster, fullpath):
        """
        :param fullpath:string
        :param cluster: string
        :return: boolean
        """
        outp = ShellUtils.run_bash_function(QA_BASHLIB_PATH, "is_deduped", "%s %s" % (cluster, fullpath))
        if outp == "fully deduped":
            return True
        else:
            return False

    @staticmethod
    def is_rehydrated(cluster, fullpath):
        """
        :param fullpath:string
        :param cluster: string
        :return: boolean
        """
        outp = ShellUtils.run_bash_function(QA_BASHLIB_PATH, "is_rehydrated", "%s %s" % (cluster, fullpath))
        if outp == "rehydrated":
            return True
        else:
            return False

    @staticmethod
    def mount_fsd(cluster, export_dir, nof_nodes, domains, mtype, prefix, exaver=6):
        """
        :type cluster: str
        :type export_dir: str
        :type nof_nodes: int
        :type domains: int
        :type mtype: str
        :type prefix: str
        :type exaver: str
        :rtype: bool

        """
        outp = ShellUtils.run_bash_function(QA_BASHLIB_PATH, "mount_fsd", "%s %s %s %s %s %s %s %s" % (
            export_dir, cluster, nof_nodes, domains, mtype, prefix, 0, exaver))
        if "Exception" in outp:
            return True
        else:
            return False

    @staticmethod
    def get_active_nodes_num(cluster):
        """
        :param cluster: string
        :return: int
        """
        return ShellUtils.run_bash_function(PARAMS_FUNCTIONS, "get_active_nodes", cluster).count('node')

    @staticmethod
    def get_domains_num(cluster):
        """
        :rtype: int
        :type cluster: str
        :return: int
        """
        return int(ShellUtils.run_shell_remote_command(cluster, 'getparam params.fs.config.instances'))

    @staticmethod
    def run_fsadmin_clusterwide_command(cluster, nodes, fsds, cmd):
        """
        :type cluster: str
        :type nodes: int
        :type fsds: int
        :type cmd: str
        :return:
        """
        for i in range(nodes):
            for j in range(fsds):
                ShellUtils.run_shell_remote_command("node%d.%s" % (i, cluster), "fsadmin %d %s" % (j, cmd))


def mount(server, export, mount_point, mtype):
    try:
        ShellUtils.run_shell_command("mount", "-o,nfsvers={0},{1}:/{2},{3}".format(mtype, server, export, mount_point),
                                     sep=',')
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
