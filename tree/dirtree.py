import hashlib
import random

import treelib

import config
from shell_utils import StringUtils


class DirTree(object):
    def __init__(self):
        self._dir_tree = treelib.Tree()
        self._tree_base = self._dir_tree.create_node('Root', 'root')
        self._last_node = self._tree_base

    def append_node(self):
        directory = Directory()
        new_node = self._dir_tree.create_node("{0}".format(directory.name), "{0}".format(hashlib.md5(directory.name)),
                                              parent=self._tree_base.identifier, data=directory)
        self._last_node = new_node

    @property
    def last_node(self):
        return self._last_node

    def get_last_node_tag(self):
        return self._last_node.tag

    def get_last_node_data(self):
        """

        Returns: Directory

        """
        return self._last_node.data

    def get_random_dir_name(self):
        """

        Returns: list

        """
        return random.choice(self._dir_tree.to_dict()['Root']['children'])


def build_recursive_tree(tree, base, depth, width):
    """
    Args:
        tree: Tree
        base: Node
        depth: int
        width: int
    """
    if depth >= 0:
        depth -= 1
        for i in xrange(width):
            directory = Directory()
            tree.create_node("{0}".format(directory.name), "{0}".format(hashlib.md5(directory.name)),
                             parent=base.identifier, data=directory)
        dirs_nodes = tree.children(base.identifier)
        for dir_node in dirs_nodes:
            newbase = tree.get_node(dir_node.identifier)
            build_recursive_tree(tree, newbase, depth, width)
    else:
        return


class Directory(object):
    def __init__(self):
        self._name = StringUtils.get_random_string_nospec(64)
        self.files = []

    @property
    def name(self):
        return self._name

    def touch(self):
        """

        Returns: list

        """
        self.files.append(File())
        return self.files[-1].name

    def get_random_file(self):
        """

        Returns: list

        """
        return random.choice(self.files)

    def get_random_files(self, f_number=10):
        """

        Args:
            f_number: int

        Returns: list

        """
        return random.sample(set(self.files), f_number)

    def delete_random_file(self):
        index = self.files.index(random.choice(self.files))
        del self.files[index]

    def delete_random_files(self, f_number):
        for f in random.sample(set(self.files), f_number):
            index = self.files.index(f)
            del self.files[index]


class File(object):
    def __init__(self):
        self._name = StringUtils.get_random_string_nospec(64)

    @property
    def name(self):
        return self._name
