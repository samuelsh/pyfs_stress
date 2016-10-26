__author__ = "samuels"

"""
Controller helper methods
"""


def message_to_pretty_string(incoming_message):
    """

    Args:
        incoming_message: dict

    Returns:
        str
    """
    try:
        formatted_message = "{0} | {1} | {2} | [errno:{3}] | {4} | {5} | data: {6} | {7}".format(
            incoming_message['result'],
            incoming_message['action'],
            incoming_message['target'],
            incoming_message['error_code'],
            incoming_message['error_message'],
            incoming_message['linenum'],
            incoming_message['data'],
            incoming_message['timestamp'])
    except KeyError:
        formatted_message = "{0} | {1} | {2} | data: {3} | {4}".format(incoming_message['result'],
                                                                       incoming_message['action'],
                                                                       incoming_message['target'],
                                                                       incoming_message['data'],
                                                                       incoming_message['timestamp'])
    return formatted_message
