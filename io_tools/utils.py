# -*- coding: utf-8 -*-
import string

"""
    Helper utils for io_tools - 2018 (c)
"""


def futures_validator(futures, logger):
    """

    :param logger:
    :param futures: list
    :return: None
    """
    for future in futures:
        result = None
        try:
            try:
                result = future.result()
            except AttributeError:
                result = future.value
        except Exception as e:
            logger.error("Future raised exception: {} due to {}".format(e, result))
            raise e


def assert_raises(exc_class, func, *args):
    try:
        func(*args)
    except exc_class as e:
        return e
    else:
        raise AssertionError("{} not raised".format(exc_class))


def build_buf(counter, buf):
    printable_bytes = bytes(string.printable, 'ascii')
    buf2 = [('%02x' % i) for i in buf]
    return '{0}: {1:<39}  {2}'.format(('%07x' % (counter * 16)),
                                      ' '.join([''.join(buf2[i:i + 2]) for i in range(0, len(buf2), 2)]),
                                      ''.join([chr(c) if c in printable_bytes[:-5] else '.' for c in buf]))


def process_xxd(src_file_path, dst_file_path):
    with open(src_file_path, 'rb') as f, open(dst_file_path, 'wt') as f2:
        counter = 0
        while True:
            buf = f.read(16)
            if not buf:
                break
            buf = build_buf(counter, buf)
            f2.write(buf + '\n')
            counter += 1
