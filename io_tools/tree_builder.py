#!/usr/bin/env python3.6
import argparse
import hashlib
import os
import pathlib
import random
import string

MAX_FILES_PER_DIR = 1000


def get_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def build_recursive_tree(base_path, depth, width):
    dir_nodes = []
    res = []
    if depth > 0:
        depth -= 1
        for i in range(width):
            dir_nodes.append(Directory(base_path))
        for d in dir_nodes:
            new_base_path = d.full_path
            res += build_recursive_tree(new_base_path, depth, width)
        return dir_nodes + res
    else:
        return []


class Directory:
    def __init__(self, base_path):
        self._name = get_random_string(16)
        self._base_path = base_path
        self._full_path = os.path.join(self._base_path, self._name)
        pathlib.Path(self._full_path).mkdir()

        self._files = {}
        for _ in range(MAX_FILES_PER_DIR):
            file_obj = File(self._full_path)
            self._files[hashlib.md5(file_obj.name.encode()).hexdigest()] = file_obj

    @property
    def name(self):
        return self._name

    @property
    def base_path(self):
        return self._base_path

    @property
    def full_path(self):
        return self._full_path

    @property
    def files(self):
        return self._files


class File(object):
    def __init__(self, path):
        self._path = path
        self._name = get_random_string(16)
        self._full_path = os.path.join(self._path, self._name)
        pathlib.Path(self._full_path).touch()

    @property
    def name(self):
        return self._name

    @property
    def full_path(self):
        return self._full_path

    @property
    def path(self):
        return self._path


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Destination Path", type=str)
    parser.add_argument('-w', '--width', type=int, help="Directory tree width", default=3)
    parser.add_argument('-d', '--depth', type=int, help="Directory tree height", default=2)
    return parser.parse_args()


def main():
    args = get_args()
    try:
        build_recursive_tree(args.path, args.depth, args.width)
    except Exception as e:
        print(f"{e}")


if __name__ == '__main__':
    main()
