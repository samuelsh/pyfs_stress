"""
Sparse file cmd util. Requires Win8 or higher
2017 - samuels(c)
"""
import atexit
import os
import subprocess
import argparse

import sys
import traceback

import itertools

from logger import server_logger
from utils import shell_utils


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", help="Cluster Name", type=str)
    parser.add_argument("drive", help="Drive Letter", type=str)
    parser.add_argument("share", help="Shared Folder name", type=str)
    parser.add_argument("-s", "--size", help="File Size (KB)", type=int, default=1024 * 1024 * 10)
    parser.add_argument("--nocreate", help="Skip File creation", action="store_true")
    parser.add_argument("--name", help="File Name", default="sparse.f", type=str)
    parser.add_argument("--start", help="Start Offset (KB)", default=0, type=int)
    parser.add_argument("--end", help="End Offset (KB)", default=4, type=int)
    parser.add_argument("--step", help="Step (KB)", default=4, type=int)
    parser.add_argument("--repeat", help="Repeats number", default=1, type=int)
    return parser.parse_args()


def clean(drive):
    subprocess.call(r'net use {0}: /del'.format(drive))


def create_new_file(logger, path, size):
    try:
        shell_utils.ShellUtils.run_shell_command('fsutil', 'file createnew {0} {1}'.format(path, size))
    except RuntimeError as err:
        if "exists" in err.message:
            logger.warn(err.message)
        else:
            raise err


def make_sparse(logger, args, fsize):
    fpath = os.path.join(args.drive + ':', os.sep, args.name)
    start_offset = args.start * 1024  # KB to Bytes
    end_offset = args.end * 1024
    step = args.step * 1024
    logger.info("Enabling sparse")
    shell_utils.ShellUtils.run_shell_command('fsutil', 'sparse setflag {0}'.format(fpath))
    logger.info("Go make some holes!!!")
    for _ in itertools.count(args.repeat):
        logger.info("Making hole from {0}KB to {1}KB".format(start_offset / 1024, end_offset / 1024))
        shell_utils.ShellUtils.run_shell_command('fsutil', 'sparse setrange {0} {1} {2}'.format(fpath, start_offset,
                                                                                                end_offset))
        start_offset += 2 * step
        end_offset += 2 * step
        logger.info("Skipping {0}KB".format(args.step))
        if start_offset >= fsize:
            break
    logger.info("Done making holes")


def main():
    logger = server_logger.ConsoleLogger(__name__).logger
    args = get_args()
    atexit.register(clean, args.drive)
    logger.info("Unmapping Network Drive")
    subprocess.call(r'net use {0}: /del'.format(args.drive))
    logger.info("Mapping Network Drive")
    shell_utils.ShellUtils.run_shell_command('net', r'use {0}: \\{1}\{2} /user:{3} {4}'.format(args.drive, args.cluster,
                                                                                               args.share, r'qa\Admin',
                                                                                               'manager11'))
    fpath = '{0}'.format(os.path.join(args.drive + ':', os.sep, args.name))
    if not args.nocreate:
        logger.info("Creating New File:")
        create_new_file(logger, fpath, args.size)
        logger.info("Done creating file - size {0}".format(os.path.getsize(fpath)))
    else:
        if not args.name:
            raise RuntimeError("Error: No File name was given!")
        if not os.path.exists(fpath):
            raise RuntimeError("Error: File {0} not exists!".format(fpath))

    fsize = os.path.getsize(fpath) * 1024
    if fsize == 0:
        raise RuntimeError("Bad file size!")

    logger.info("Done with creation part. Going to make sparse...")
    make_sparse(logger, args, fsize)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
