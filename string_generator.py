"""
Alphanumeric string generator
"""
import argparse
import sys

import utils.shell_utils

__author__ = 'samuels'

PATH_TO_HASH_TOOL = "/zebra/qa/samuels/misc/hash_tool"


def store_console(string):
    print string


def store_file(string):
    with open('filenames.dat', 'a+') as f:
        f.write(string + '\n')


def store_sqlite():
    pass


def store_redis():
    pass


def generate_random_string_hc(hc_value):
    while 1:
        generated_string = utils.shell_utils.StringUtils.get_random_string_nospec(64)
        generated_hash = utils.shell_utils.ShellUtils.run_shell_command("/zebra/qa/samuels/misc/hash_tool",
                                                                        '{0} 6'.format(generated_string))
        generated_hash = int(generated_hash)
        if hc_value == generated_hash:
            return generated_string


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='String generator')
    parser.add_argument('--length', type=str, default=64, help="String Length")
    parser.add_argument('--hc', action='store_true', help="Force hash collision")
    parser.add_argument('--count', type=int, default=10, help="Number of strings to generate")
    parser.add_argument('--hc_val', type=int, default=45, help="Hash collision value")
    parser.add_argument('--store', type=str, required=True, choices=['console', 'file', 'sqlite', 'redis'],
                        help="Where to store generated data")
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    store_method = {
        'console': store_console,
        'file': store_file,
        'sqlite': store_sqlite,
        'redis': store_redis
    }

    if args.hc:
        for _ in range(args.count):
            store_method[args.store](generate_random_string_hc(args.hc_val))

    else:
        for _ in range(args.count):
            store_method[args.store](utils.shell_utils.StringUtils.get_random_string_nospec(64))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
