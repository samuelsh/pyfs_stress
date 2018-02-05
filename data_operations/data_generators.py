"""
This module provides few classes that generates a data.
It could be a same data, random data or data that follows some logic
"""

import os
import abc
import zlib
import time
import types
import random
import hashlib

__author__ = "Roman Yakovenko (c)"


class IDataGenerator:
    """base class for all data generators"""

    def __init__(self):
        pass

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, size):
        """generates the requested amount of data. the generated data may be too big
                   to be returned as a single buffer, so the correct way to use this function, is to call it in
                   a loop.
                """
        pass

    def get_all_data(self, size):
        """tries to return all data in a single string - use carefully"""
        data = []
        for chunk in self(size):
            data.append(chunk)
        return ''.join(data)


class Random(IDataGenerator):
    """slow, generates the random data each time"""

    def __call__(self, size):
        while 0 < size:
            chunk_size = min(size, 1024 * 1024)
            size -= chunk_size
            yield os.urandom(chunk_size)


class SemiRandom(IDataGenerator, object):
    """slow start, fast generation
        pregenerates block of data and then randomly selects between them"""

    def __init__(self, nblocks=1024, block_size=1024):
        """
                :param nblocks: number of blocks to be pre-generated
                :param block_size: size of each block
                """
        super(SemiRandom, self).__init__()
        self.blocks = []
        self.block_size = block_size
        assert block_size <= 1024 * 1024

        for _ in range(nblocks):
            self.blocks.append(os.urandom(self.block_size))

    def __xor_str_and_int(self, block, value):
        result = []
        for x in block:
            new_x = (value ^ ord(x)) % 256
            result.append(chr(new_x))
        return ''.join(result)

    def __call__(self, size):
        while 0 < size:
            block = random.choice(self.blocks)
            if size == self.block_size:
                yield self.__xor_str_and_int(block, int(time.time() * 1000000))
            else:
                yield self.__xor_str_and_int(block[:size], int(time.time() * 1000000))
            size -= self.block_size


class Compressed(IDataGenerator, object):
    """generates infinite stream of compressed data;
        This generator is slow and has high memory and CPU usage"""

    def __init__(self, data_generator):
        super(Compressed, self).__init__()
        assert (isinstance(data_generator, (Random, SemiRandom)))
        self.data_generator = data_generator
        self.compressor = zlib.compressobj(9)

    def __call__(self, size):
        while 0 < size:
            for chunk in self.data_generator(8192):
                compressed = self.compressor.compress(chunk)
                if len(compressed) <= size:
                    yield compressed
                else:
                    yield compressed[:size]
                size -= len(compressed)


class Same(IDataGenerator, object):
    """generates same data all the time"""

    def __init__(self, block):
        super(Same, self).__init__()
        """ block: the block to be repeated"""
        assert (isinstance(block, types.StringTypes) and block)
        self.block = block
        self.block_size = len(block)

    def __call__(self, size):
        while 0 < size:
            if size < self.block_size:
                yield self.block[:size]
            else:
                yield self.block
            size -= self.block_size


class Transform(IDataGenerator, object):
    """this class was born to help developers to find corruptions in the file system
        This class generates 4KB block and passes them to a transformer. The transformer
        should return 4KB blocked. The content of the block can be different:
        * it can contain some prefix or suffix
        * it can contain md5 sum of the block
        * it can contain file dsid
        * whatever you think will help to find corruption
        Pay attention, the transformer can get block smaller then 4KB
        """

    TRANSFORMED_BLOCK_SIZE = 4096  # 4KB, a smallest unit, radix tree can handle

    def __init__(self, transform, data_generator):
        """info_provider - a callable object, which takes as input offset
                  of the generated data. It is up on the caller to refresh info_provider state

                  data_generator is an instance of the data generator
                """
        super(Transform, self).__init__()
        self._transform = transform
        self._data_generator = data_generator

    def __call__(self, size):
        while 0 < size:
            data = self._data_generator.get_all_data(min(self.TRANSFORMED_BLOCK_SIZE, size))
            size -= len(data)
            transformed = self._transform(data)
            assert len(transformed) == len(data)
            yield transformed


class Transformers(object):
    """used as namespace for few transformers"""

    def __init__(self):
        pass

    @staticmethod
    def md5(block):
        """calculates blocks md5 checksum and stores it within first 32 bytes"""
        m = hashlib.md5()
        if len(block) <= m.digest_size * 3:
            return block
        m.update(block[m.digest_size * 2:])
        signed_block = m.hexdigest() + block[m.digest_size * 2:]
        assert len(signed_block) == len(block)
        return signed_block
