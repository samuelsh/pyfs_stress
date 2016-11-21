"""
Alphanumeric string generator
"""
import argparse
import multiprocessing
import sys
import traceback
import uuid
from time import sleep

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


def hc_worker(stop_event, hc_value, names_queue, level):
    print("Worker {0} started...".format(uuid.uuid4()))
    while not stop_event.is_set():
        generated_string = utils.shell_utils.StringUtils.get_random_string_nospec(64)
        generated_hash = utils.shell_utils.ShellUtils.run_shell_command("/zebra/qa/samuels/misc/hash_tool",
                                                                        '{0} 6'.format(generated_string, level))
        generated_hash = int(generated_hash)
        if hc_value == generated_hash:
            names_queue.put(generated_string)


def generate_random_string_hc(stop_event, hc_value, names_queue, level=1):
    levels = {1: 6, 2: ""}
    num_cores = multiprocessing.cpu_count()
    workers_pool = multiprocessing.Pool(num_cores)
    for _ in range(num_cores):
        workers_pool.apply_async(hc_worker, args=(stop_event, hc_value, names_queue, levels[level]))
    workers_pool.close()
    workers_pool.join()


def get_args():
    """
    Supports the command-line arguments listed below.
    """

    parser = argparse.ArgumentParser(
        description='String generator')
    parser.add_argument('--length', type=str, default=64, help="String Length")
    parser.add_argument('--hc', action='store_true', help="Force hash collision")
    parser.add_argument('--level', type=int, choices=[1, 2], default=1,
                        help="Level of hash collision. Depends on --hc flag")
    parser.add_argument('--count', type=int, default=10, help="Number of strings to generate")
    parser.add_argument('--hc_val', type=int, default=45, help="Hash collision value")
    parser.add_argument('--store', type=str, required=True, choices=['console', 'file', 'sqlite', 'redis'],
                        default="console", help="Where to store generated data")
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    stop_event = multiprocessing.Event()
    names_queue = multiprocessing.Queue()
    store_method = {
        'console': store_console,
        'file': store_file,
        'sqlite': store_sqlite,
        'redis': store_redis
    }
    try:
        if args.hc:
            generate_random_string_hc(stop_event, args.hc_val, names_queue, args.level)
            for _ in range(args.count):
                store_method[args.store](names_queue.get())
            stop_event.set()

        else:
            for _ in range(args.count):
                store_method[args.store](utils.shell_utils.StringUtils.get_random_string_nospec(64))
    except KeyboardInterrupt:
        stop_event.set()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        traceback.print_exc()
        sys.exit(1)
