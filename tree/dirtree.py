import hashlib
import random

import treelib

from utils.shell_utils import StringUtils


class DirTree(object):
    def __init__(self):
        self._dir_tree = treelib.Tree()
        self._tree_base = self._dir_tree.create_node('Root', 'root')
        self._last_node = self._tree_base
        self._nids = []  # Nodes IDs pool for easy random sampling

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

    def get_last_node_tag(self):
        return self._last_node.tag

    def get_dir_by_name(self, name):
        return self._dir_tree.get_node(hashlib.md5(name).hexdigest())

    def get_last_node_data(self):
        """

        Returns: Directory

        """
        return self._last_node.data

    def get_random_dir(self):
        """

        Returns: Node

        """
        return self._dir_tree.get_node(random.choice(self.nids))

    def get_random_dir_synced(self):
        """

        Returns: Node

        """
        dir_node = self.get_random_dir()
        while not dir_node.ondisk:
            dir_node = self.get_random_dir()

        return dir_node

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
            return "nofiles"
        if num_files == 1:
            max_files = 1
        else:
            max_files = random.randint(1, num_files)
        files = rand_dir.data.get_random_files(max_files)
        filepaths = ""
        for f in files:
            if f.ondisk:
                filepaths += "/{0}/{1},".format(rand_dir.tag, f.name)
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
        self.ondisk = False

    @property
    def name(self):
        return self._name
