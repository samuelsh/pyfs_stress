import hashlib
import random

import itertools
import treelib

from utils.shell_utils import StringUtils


class DirTree(object):
    def __init__(self):
        self._dir_tree = treelib.Tree()
        self._tree_base = self._dir_tree.create_node('Root', 'root')
        self._last_node = self._tree_base
        self._nids = []  # Nodes IDs pool for easy random sampling
        self.synced_nodes = []  # Nodes IDs list which already Synced with storage

    def append_node(self):
        directory = Directory()
        name = directory.name
        nid = hashlib.md5(name).hexdigest()
        self._nids.append(nid)
        new_node = self._dir_tree.create_node(name, nid, parent=self._tree_base.identifier, data=directory)
        self._last_node = new_node

    @property
    def last_node(self):
        return self._last_node

    @property
    def nids(self):
        return self._nids

    @nids.setter
    def nids(self, value):
        self._nids = value

    def get_size(self):
        return self._dir_tree.size()

    def get_last_node_tag(self):
        return self._last_node.tag

    def get_dir_by_name(self, name):
        return self._dir_tree.get_node(hashlib.md5(name).hexdigest())

    def remove_dir_by_name(self, name):
        try:
            cnt = self._dir_tree.remove_node(hashlib.md5(name).hexdigest())
        except Exception:
            raise
        return cnt

    def get_last_node_data(self):
        """

        Returns: Directory

        """
        return self._last_node.data

    def get_random_dir(self):
        """

        Returns: Node

        """
        try:
            return self._dir_tree.get_node(random.choice(self.nids))
        except IndexError:
            return None

    def get_random_dir_synced(self):
        """

        Returns: Node

        """
        try:
            return self._dir_tree.get_node(random.choice(self.synced_nodes))
        except IndexError:
            return None

    def get_random_dir_not_synced(self):
        """

        Returns: Node

        """
        try:
            not_synced_dirs = [d for d in self.nids if d not in self.synced_nodes]
            return self._dir_tree.get_node(random.choice(not_synced_dirs))
        except IndexError:
            return None

    def get_random_dir_name(self):
        """

        Returns: str

        """
        return self._dir_tree.get_node(random.choice(self.nids)).tag

    def get_random_dir_files(self):
        """

        Returns: str

        """
        rand_dir = self.get_random_dir()
        num_files = len(rand_dir.data.files)
        if num_files == 0:
            return "/{0}/nofiles".format(rand_dir.tag)
        if num_files == 1:
            max_files = 1
        else:
            max_files = random.randint(1, num_files)
            if max_files > 10:
                max_files = 10
        files = rand_dir.data.get_random_files(max_files)
        filepaths = ""
        for f in files:
            if f.ondisk:
                filepaths += "/{0}/{1},".format(rand_dir.tag, f.name)
        if not filepaths:
            filepaths = "/{0}/nofiles".format(rand_dir.tag)
        return filepaths


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
        self.ondisk = False
        self.checksum = 0
        self.creation_time = None
        self.size = None
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

    def get_file_by_name(self, name):
        try:
            return next((thefile for thefile in self.files if thefile.name == name), None)
        except ValueError:
            return None

    def get_random_file(self):
        """

        Returns: list

        """
        try:
            return random.choice(self.files)
        except IndexError:
            return None

    def get_random_files(self, f_number=10):
        """

        Args:
            f_number: int

        Returns: list

        """
        try:
            return random.sample(set(self.files), f_number)
        except IndexError:
            return None

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
        self.cheksum = 0
        self.last_action = None
        self.creation_time = None
        self.ondisk = False

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self._name = new_name
