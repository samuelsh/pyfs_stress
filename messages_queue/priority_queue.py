"""
Thread safe priority queue
samuels(c) 2016
"""
import Queue

import datetime


class PriorityQueue(object):
    def __init__(self):
        self.__rcv_messages_buffer = Queue.PriorityQueue()

    def put(self, message):
        """
        Result message format:
        Success message format: {'result', 'action', 'target', 'data:{}', 'timestamp'}
        Failure message format: {'result', 'action', 'error_message', 'path', 'linenumber', 'timestamp', 'data:{}'}
        """
        if message[0] == 'success':
            message = {'result': message[0], 'action': message[1], 'target': message[2].strip('\''),
                       'timestamp': datetime.datetime.strptime(message[4], '%Y/%m/%d %H-%M-%S.%f'), 'data': message[3]}
        else:
            message = {'result': message[0], 'action': message[1], 'error_message': message[2],
                       'target': message[3].strip('\''), 'linenum': message[4],
                       'timestamp': datetime.datetime.strptime(message[5], '%Y/%m/%d %H-%M-%S.%f'), 'data': message[6]}

        self.__rcv_messages_buffer.put((1, message['timestamp'], message))

    def get(self):
        return self.__rcv_messages_buffer.get()
