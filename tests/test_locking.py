import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from client.locking import is_overlap


def test_no_overlap_disjoint():
    assert not is_overlap(0, 10, 20, 10)


def test_no_overlap_adjacent():
    assert not is_overlap(0, 10, 10, 10)


def test_overlap_partial():
    assert is_overlap(0, 15, 10, 10)


def test_overlap_contained():
    assert is_overlap(5, 5, 0, 20)


def test_overlap_identical():
    assert is_overlap(10, 20, 10, 20)


def test_no_overlap_zero_length():
    assert not is_overlap(10, 0, 10, 5)


def test_overlap_reversed_order():
    assert is_overlap(20, 10, 15, 10)
