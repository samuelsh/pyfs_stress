import xxhash
import random

import uuid

import treelib

from utils.shell_utils import StringUtils


class DirTree(object):
    def __init__(self, file_names=None):
        self._dir_tree = treelib.Tree()
        self._tree_base = self._dir_tree.create_node('Root', 'root')
        self._last_node = self._tree_base
        if file_names:
            self.file_names = StringUtils.string_from_file_generator(file_names)  # pre-generated file_names iterator
        else:
            self.file_names = StringUtils.random_string_generator()
        self._nids = {}  # Nodes IDs pool for easy random sampling
        self.synced_nodes = {}  # Nodes IDs list which already Synced with storage

    def append_node(self):
        directory = Directory(self.file_names)
        name = directory.name
        nid = xxhash.xxh64(name).hexdigest()
        self._nids[nid] = name
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
        return self._dir_tree.get_node(xxhash.xxh64(name).hexdigest())

    def remove_dir_by_name(self, name):
        try:
            cnt = self._dir_tree.remove_node(xxhash.xxh64(name).hexdigest())
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
            return self._dir_tree.get_node(random.choice(list(self._nids.keys())))
        except IndexError:
            return None

    def get_random_dir_synced(self):
        """

        Returns: Node

        """
        try:
            return self._dir_tree.get_node(random.choice(list(self.synced_nodes.keys())))
        except IndexError:
            return None

    def get_random_dir_not_synced(self):
        """

        Returns: Node

        """
        try:
            return self._dir_tree.get_node(self._nids.popitem()[0])
        except KeyError:
            return None

    def get_random_dir_name(self):
        """

        Returns: str

        """
        return self._dir_tree.get_node(random.choice(list(self._nids.keys()))).tag

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
        for _ in range(width):
            directory = Directory(None)
            tree.create_node("{0}".format(directory.name), "{0}".format(xxhash.xxh64(directory.name)),
                             parent=base.identifier, data=directory)
        dirs_nodes = tree.children(base.identifier)
        for dir_node in dirs_nodes:
            newbase = tree.get_node(dir_node.identifier)
            build_recursive_tree(tree, newbase, depth, width)
    else:
        return


class Directory(object):
    def __init__(self, file_names_generator):
        self.file_names_generator = file_names_generator
        self._name = StringUtils.get_random_string_nospec(64)
        self.ondisk = False
        self.checksum = 0
        self.creation_time = None
        self.size = 0  # Size of dir entry
        self.files = []
        self.files_dict = {}

    @property
    def name(self):
        return self._name

    def touch(self):
        """

        Returns: list

        """
        new_file = File(self.file_names_generator)
        self.files_dict[xxhash.xxh64(new_file.name).hexdigest()] = new_file
        return new_file.name

    def get_file_by_name(self, name):
        try:
            return self.files_dict[xxhash.xxh64(name).hexdigest()]
        except KeyError:
            return None

    def get_random_file(self):
        """

        Returns: list

        """
        try:
            return random.choice(list(self.files_dict.values()))
        except IndexError:
            return None

    def get_random_files(self, f_number=10):
        """

        Args:
            f_number: int

        Returns: list

        """
        try:
            return random.sample(set(self.files_dict.values()), f_number)
        except IndexError:
            return None

    def delete_file_by_name(self, name):
        del self.files_dict[xxhash.xxh64(name).hexdigest()]

    def rename_file(self, source_name, dest_name):
        new_file = File(name=dest_name)
        self.files_dict[xxhash.xxh64(new_file.name).hexdigest()] = new_file
        self.files_dict[xxhash.xxh64(source_name).hexdigest()].ondisk = False
        return new_file

    def delete_random_file(self):
        del self.files_dict[random.choice(list(self.files_dict.keys()))]

    def delete_random_files(self, f_number):
        for f in random.sample(set(self.files_dict.keys()), f_number):
            del self.files_dict[f]


class File(object):
    def __init__(self, file_name_generator=None, name=None):
        # self._name = StringUtils.get_random_string_nospec(64)
        self._name = next(file_name_generator) if file_name_generator else name
        self.data_pattern = 0
        self.data_pattern_len = 0
        self.data_pattern_hash = 'ef46db3751d8e999'  # zero xxhash hash
        self.data_pattern_offset = 0
        self.uuid = uuid.uuid4().hex[-5:]  # Unique session ID, will be modified on each file modify action
        self.tid = 0  # incremental transaction id for each file
        self.last_actions = []
        self.creation_time = None
        self.modify_time = None
        self.ondisk = False
        self.size = 0

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self._name = new_name
