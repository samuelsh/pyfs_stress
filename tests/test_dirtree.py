import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import threading
from tree.dirtree import DirTree, Directory, File


def test_append_and_get_size():
    dt = DirTree()
    assert dt.get_size() == 1  # root node
    dt.append_node()
    assert dt.get_size() == 2
    dt.append_node()
    assert dt.get_size() == 3


def test_get_last_node_tag():
    dt = DirTree()
    assert dt.get_last_node_tag() == 'Root'
    dt.append_node()
    tag = dt.get_last_node_tag()
    assert tag != 'Root'
    assert len(tag) == 64


def test_get_dir_by_name():
    dt = DirTree()
    dt.append_node()
    name = dt.get_last_node_tag()
    node = dt.get_dir_by_name(name)
    assert node is not None
    assert node.data.name == name


def test_get_dir_by_name_missing():
    dt = DirTree()
    assert dt.get_dir_by_name('nonexistent') is None


def test_remove_dir_by_name():
    dt = DirTree()
    dt.append_node()
    name = dt.get_last_node_tag()
    result = dt.remove_dir_by_name(name)
    assert result == 1
    assert dt.get_dir_by_name(name) is None


def test_remove_dir_by_name_missing():
    dt = DirTree()
    result = dt.remove_dir_by_name('nonexistent')
    assert result == 0


def test_get_random_dir_synced_empty():
    dt = DirTree()
    assert dt.get_random_dir_synced() is None


def test_nids_returns_copy():
    dt = DirTree()
    dt.append_node()
    nids1 = dt.nids
    nids2 = dt.nids
    assert nids1 == nids2
    assert nids1 is not nids2  # must be a copy


def test_directory_touch_and_get():
    gen = iter(['file_a', 'file_b', 'file_c'])
    d = Directory(gen)
    name = d.touch()
    assert name == 'file_a'
    f = d.get_file_by_name('file_a')
    assert f is not None
    assert f.name == 'file_a'


def test_directory_get_random_file_empty():
    d = Directory(iter([]))
    assert d.get_random_file() is None


def test_directory_rename_file():
    gen = iter(['original'])
    d = Directory(gen)
    d.touch()
    f = d.get_file_by_name('original')
    f.ondisk = True
    new_f = d.rename_file('original', 'renamed')
    assert new_f.name == 'renamed'
    old_f = d.get_file_by_name('original')
    assert old_f is not None
    assert old_f.ondisk is False


def test_directory_delete_file():
    gen = iter(['to_delete'])
    d = Directory(gen)
    d.touch()
    assert d.get_file_by_name('to_delete') is not None
    d.delete_file_by_name('to_delete')
    assert d.get_file_by_name('to_delete') is None


def test_thread_safety_append():
    """Verify DirTree doesn't crash under concurrent appends."""
    dt = DirTree()
    errors = []

    def append_many():
        try:
            for _ in range(50):
                dt.append_node()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=append_many) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"Thread safety errors: {errors}"
    assert dt.get_size() > 1
