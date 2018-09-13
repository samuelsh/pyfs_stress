import os
import pathlib

import random
from string import printable

KB1 = 1024
KB4 = KB1 * 4
MB1 = KB1 * 1024

DATA_PATH = os.path.join('../data_operations', 'data')


def read_dataset_file(file_path, mode='rt'):
    with open(file_path, mode) as f:
        buf = f.read()
    return buf


DATA_PATTERNS = [{'type': 'zeroed', 'data': b'\0' * KB4},
                 {'type': 'binary', 'data': os.urandom(KB4)},
                 {'type': 'text',
                  'data': read_dataset_file(os.path.join(DATA_PATH, 'textblock.txt'), mode='rt')},
                 {'type': 'binary', 'data': read_dataset_file(os.path.join(DATA_PATH, 'proteincorpus.tar'),
                                                              mode='rb')},
                 {'type': 'binary', 'data': read_dataset_file(os.path.join(DATA_PATH, 'lz_1_to_4.dat'),
                                                              mode='rb')},
                 {'type': 'binary', 'data': read_dataset_file(os.path.join(DATA_PATH, 'lz_1_to_9.dat'),
                                                              mode='rb')},
                 {'type': 'binary', 'data': read_dataset_file(os.path.join(DATA_PATH, 'lz_cmprs_by_entropy.dat'),
                                                              mode='rb')},
                 {'type': 'binary', 'data': read_dataset_file(os.path.join(DATA_PATH, 'lz_to_zstd.zstd'),
                                                              mode='rb')},
                 {'type': 'binary', 'data': read_dataset_file(os.path.join(DATA_PATH, 'ooffice_dll.bin'),
                                                              mode='rb')},
                 {'type': 'text', 'data': read_dataset_file(os.path.join(DATA_PATH, 'chromo3.fa'), mode='rt')}
                 ]


def handle_data_type(data_type, data):
    return {'binary': generate_binary_data,
            'zeroed': generate_zeroed_data,
            'text': generate_text_data,
            }[data_type](data)


def generate_binary_data(data):
    key = b"".join(random.choice(printable).encode() for _ in range(4 * KB1))
    return "".join([chr(a ^ b) for (a, b) in zip(data, key)]).encode()


def generate_zeroed_data(data):
    return data


def generate_text_data(data):
    start = random.randint(0, len(data) - 1)
    if start + KB4 >= len(data):
        start = len(data) - KB4
    return data[start:start + KB4]
