"""
Nested File cloning automated test
2017 - samuels(c)
"""
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", help="Cluster Name", type=str)
    parser.add_argument("share", help="Shared Folder name", type=str)
    parser.add_argument("-s", "--size", help="File Size (KB)", type=int, default=1024 * 1024 * 10)
    return parser.parse_args()
