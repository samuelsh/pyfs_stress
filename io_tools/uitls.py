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
