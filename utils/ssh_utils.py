#!/usr/bin/env python3
"""
SSH Utils Helper module

Created on May 5, 2015
@author: samuels
"""

import os
import socket
import pexpect
import sys
import warnings
import argparse
import logging
import time
import errno
from socket import error as socket_error

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import paramiko


def initialize_logger(output_dir=""):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.WARN)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler(os.path.join(output_dir, "set_ssh.dbg"), "w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def is_ipv4(addr):
    try:
        socket.inet_aton(addr)
        return True
    except socket.error:
        return False


def is_hostname(hostname):
    try:
        if socket.gethostbyname(hostname) == hostname:
            return False
        else:
            return True
    except socket.error:
        return False


def set_key_policy(key, host, logger, username, password, port=22):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username, password=password, port=port, timeout=30)
        ssh.exec_command('mkdir -p ~/.ssh/')
        ssh.exec_command('echo "%s" >> ~/.ssh/authorized_keys' % key)
        ssh.exec_command('chmod 644 ~/.ssh/authorized_keys')
        ssh.exec_command('chmod 700 ~/.ssh/')
        ssh.close()
    except socket_error as serr:
        if serr.errno == errno.ECONNREFUSED:
            logger.exception("")
            raise serr
    except paramiko.BadAuthenticationType as authtype_err:
        logger.exception("")
        raise authtype_err
    except Exception as general_paramiko_ex:
        logger.exception("")
        return False

    return True


def set_ssh_pexpect(host, logger, username, password, timeout=30, port=22):
    ssh_newkey = 'Are you sure you want to continue connecting'
    ssh_success = 'to make sure we haven\'t added extra keys that you weren\'t expecting'
    ssh_error = 'error: [Errno 111] Connection refused'
    try:
        args = [host + " -p " + str(port)]
        logger.debug("ssh-copy-id: " + str(args))
        p = pexpect.spawn('/usr/bin/ssh-copy-id', args, timeout=timeout)
        match = p.expect(['Password:', ']#', ssh_newkey])
        logger.debug("expect output: %s" % p.before)
        logger.debug("match: %s" % match)
        if match == 0:
            logger.info("ssh-copy-id")
            time.sleep(5)
            p.sendline(password)
            # p.expect([']#','\r\n'])

        if match == 2:
            logger.info("RSA key confirmation")
            time.sleep(5)
            return_code = p.sendline('yes')
            print(return_code)
            # p.expect([ssh_newkey, '#'])
            if return_code != 4:
                raise RuntimeError("ssh-copy-id failed!")
            elif ssh_error in p.before.strip() in ssh_error:
                raise ssh_error
        logger.debug("expect output: %s" % p.before)
        # Last check if SSH is actually working at the end
        (outp, rc) = pexpect.run("ssh -nx -6 " + host + " ls", timeout=timeout, withexitstatus=True)
        if rc:
            logger.info("Failed to set SSH!")
            raise Exception
        logger.debug("SSH connection is set!")
    except pexpect.EOF:
        logger.debug("EOF raised")
        pass
    except Exception as pexpect_err:
        logger.exception("set_ssh_pexpect error!")
        raise pexpect_err


def connect(host, logger, username, password, timeout=30, port=22):
    try:
        s = pexpect.pxssh.pxssh()
        s.logfile = open('/tmp/logfile.txt', "w")
        s.login(host, username, password, port=port, login_timeout=timeout)
        s.prompt()  # match the prompt
        print(s.before)  # print everything before the prompt.
        s.logout()
    except Exception as pxsshEx:
        logger.exception("")
        raise


def connect_ipv6(host, logger, username, password, timeout=30, port=22):
    # my ssh command line
    try:
        args = ['-6', '-p ' + str(port), username + '@' + host]
        logger.debug("Setting up ssh: ssh " + str(args))
        p = pexpect.spawn('ssh', args, timeout=timeout)
        logger.debug("Process spawned...")
        match = p.expect(["(?i)are you sure you want to continue connecting", "]#"], timeout=timeout)
        if match == 0:
            logger.info("RSA key confirmation")
            p.sendline('yes')
            match = p.expect(']#', timeout=timeout)
        elif match == 1:
            logger.info("RSA keys already confirmed")
        logger.info("SSH connection is set")
        logger.info("%s" % p.before + str(p.match))  # print out the result
    except Exception as exPexpect:
        logger.exception("")
        raise exPexpect


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host", help="host name or IP (IPV4/IPV6)", type=str)
    parser.add_argument("-U", "--username", help="Login Username", type=str)
    parser.add_argument("-P", "--password", help="Login Password", type=str)
    parser.add_argument("-t", "--timeout", help="Login Timeout", type=int)
    parser.add_argument("-p", "--port", help="SSH Port", default=22, type=int)
    args = parser.parse_args()

    if not os.path.exists("Debug"):
        os.makedirs("Debug")

    logger = initialize_logger("Debug")

    logger.info("Creating key...")
    if not os.path.isfile(os.path.expanduser(
            '~/.ssh/id_rsa.pub')):  # in case of a new client without id_rsa.pub we'll try to create it
        try:
            logger.info("No id_rsa.pub was found on client, creating a new one")
            k = paramiko.RSAKey.generate(1024)
            k.write_private_key_file(os.path.expanduser('~/.ssh/id_rsa'))
            with open(os.path.expanduser('~/.ssh/id_rsa.pub'), "w") as f:
                f.write("ssh-rsa " + k.get_base64())
        except Exception as err:
            logger.exception("%s" % err)

    key = open(os.path.expanduser('~/.ssh/id_rsa.pub')).read()
    logger.info("Setting policy...")
    if not set_key_policy(key, args.host, logger, args.username, args.password, args.port):
        logger.error("Paramiko failed to set ssh connection will try other methods...")
        set_ssh_pexpect(args.host, logger, args.username, args.password, args.port)
        logger.info("Connection is set via pexpect (due to paramiko bug). Exiting...")
        sys.exit(0)
    logger.info("RSA key policy is set for %s. Testing connection..." % args.host)

    if not is_hostname(args.host):  # checking if hostname or IP is given
        if is_ipv4(args.host):  # checking if it is valid IPV4 address
            logger.info("Connecting via IPV4")
            connect(args.host, logger, args.username, args.password, args.timeout, args.port)
            (outp, rc) = pexpect.run("ssh " + args.host + " -p " + str(args.port) + " ls", timeout=args.timeout,
                                     withexitstatus=True)
            if rc:
                logger.info("Failed to set SSH for " + args.host)
                raise Exception
            else:
                logger.info("last check -> SSH is set !!!")

        else:  # We're assuming it's IPV6 then and will try to handle it via pure pexpect as pxssh not supporting IPV6
            logger.info("Connecting via IPV6")
            connect_ipv6(args.host, logger, args.username, args.password, args.timeout, args.port)
            (outp, rc) = pexpect.run("ssh -6 " + args.host + " -p " + str(args.port) + " 'ls'", timeout=args.timeout,
                                     withexitstatus=True,
                                     events={"(?i)are you sure you want to continue connecting": "yes\\n"})
            if rc:
                logger.info("Failed to set SSH for " + args.host)
                raise Exception
            else:
                logger.info("last check -> SSH is set !!!")

    else:
        logger.info("Connecting via HostName")
        connect(args.host, logger, args.username, args.password, args.timeout, args.port)
        (outp, rc) = pexpect.run("ssh " + args.host + " -p " + str(args.port) + " ls", timeout=args.timeout,
                                 withexitstatus=True)
        if rc:
            logger.info("Failed to set SSH for " + args.host)
            raise Exception
        else:
            logger.info("last check -> SSH is set !!!")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print("Exit on error {0}".format(e))
        sys.exit(1)
