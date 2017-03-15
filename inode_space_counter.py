#!/usr/bin/env python
"""
Inodes space counter per each LUN
2017 - samuels (c)
"""
import argparse
import traceback

import sys

import logging

import subprocess

import re


class ConsoleLogger:
    def __init__(self, name):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        # create console handler and set level to info
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s;%(levelname)s - [%(name)s]: %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger


def execute(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd, popen.stdout)


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='Inodes space counter per each LUN - 2017 samuels(c)')
    parser.add_argument('--stdout', action="store_true", help="Enable read from pipe")
    args = parser.parse_args()
    return args


def main():
    luns = {}
    logger = ConsoleLogger(__name__).logger
    args = get_args()

    logger.info("Detecting existing LUNs...")
    luns_num, _ = subprocess.Popen(['getparam', 'params.fs.config.store.volumes'], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE).communicate()
    for i in range(int(luns_num)):
        luns['lun_{0}'.format(i)] = {'size': 0, 'type':
            subprocess.Popen(['getparam', 'params.VolumesConfiguration.VolumeInfo.{0}.volume_type'.format(i)],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE).communicate()[0].rstrip()}
    logger.info("{0} LUNs detected".format(int(luns_num)))
    logger.info("Preparing inodes list....")
    if not args.stdout:  # if piping disabled will execute size_inodes_maps.sh and read from output
        inodes_list, _ = subprocess.Popen('size_inodes_maps.sh', stdout=subprocess.PIPE, stderr=subprocess.PIPE) \
            .communicate()
        inodes_list = inodes_list.splitlines()
        inodes_list = [inode.split()[-1] for inode in inodes_list]
        logger.info("Calculating indoes size...")
        for inode in inodes_list:
            for mapping in execute(['fscat', '-M', inode]):
                if 'null' in mapping:
                    continue
                mapping = mapping.split('->')[-1].lstrip().rstrip()  # Extracting primary and secondary lun
                logger.debug("Got mapping: {0}".format(mapping))
                primary_lun = re.search(r"^vol [0-9]+ disk \[\w+-\w+\]", mapping).group(0)
                secondary_lun = re.search(r"vol-sec [0-9]+ disk-sec \[\w+-\w+\]", mapping).group(0)
                primary_lun_index = primary_lun.split()[1]
                secondary_lun_index = secondary_lun.split()[1]
                primary_lun_size = int(primary_lun.split()[-1].strip('[').strip(']').split('-')[1], 16) - int(
                    primary_lun.split()[-1].strip('[').strip(']').split('-')[0], 16)
                secondary_lun_size = int(secondary_lun.split()[-1].strip('[').strip(']').split('-')[1], 16) - int(
                    secondary_lun.split()[-1].strip('[').strip(']').split('-')[0], 16)
                luns['lun_{0}'.format(primary_lun_index)]['size'] += primary_lun_size
                luns['lun_{0}'.format(secondary_lun_index)]['size'] += secondary_lun_size
                logger.info("Primary Mapping size {0} at lun_{1}. Total: {2}".
                            format(primary_lun_size, primary_lun_index,
                                   luns['lun_{0}'.format(primary_lun_index)]['size']))
                logger.info("Secondary Mapping size {0} at lun_{1}. Total: {2}".
                            format(secondary_lun_size, secondary_lun_index, luns['lun_{0}'.
                                   format(secondary_lun_index)]['size']))
        logger.info("Done space calculation")
        for lun, data in luns.items():
            logger.info("{0} - Type: {1} - Total space allocated: {2}KB".format(lun, data['type'], data['size'] / 1024))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
