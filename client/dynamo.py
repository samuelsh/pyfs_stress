"""
Client load generator
2016 samules (c)
"""

import os
import random
import zmq
import sys
import socket
import redis

from datetime import datetime
from config.redis_config import redis_config
from locking import FLock

sys.path.append(os.path.join(os.path.expanduser('~'), 'qa', 'dynamo'))
from logger import pubsub_logger
from config import CTRL_MSG_PORT
from client_actions import response_action, DynamoException
from config import error_codes


def timestamp():
    return datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S.%f')


def build_message(result, action, data, time_stamp, error_code=None, error_message=None, path=None, line=None):
    """
    Result message format: Success message format: {'result', 'action', 'target', 'data:{'dirsize, }', 'timestamp'}
    Failure message format: {'result', 'action', 'error_code', 'error_message', 'path', 'linenumber', 'timestamp',
    'data:{}'}
    """
    if result == 'success':
        message = {'result': result, 'action': action, 'target': path,
                   'timestamp': time_stamp, 'data': data}
    else:
        message = {'result': result, 'action': action, 'error_code': error_code, 'error_message': error_message,
                   'target': path, 'linenum': line,
                   'timestamp': time_stamp, 'data': data}
    return message


class Dynamo(object):
    def __init__(self, mount_points, controller, server, **kwargs):
        try:
            self.logger = pubsub_logger.PUBLogger(controller).logger
            self.logger.info(f"PUB Logger {self.logger} is started")
            self.mount_points = mount_points
            self._server = server  # Server Cluster hostname
            self._context = zmq.Context()
            self._controller_ip = socket.gethostbyname(controller)
            # Socket to send messages on by client
            self._socket = self._context.socket(zmq.DEALER)
            # We don't need to store the id anymore, the socket will handle it
            # all for us.
            # We'll use client host name + process ID to identify the socket
            self._socket.identity = "{0}:0x{1:x}".format(socket.gethostname(), os.getpid()).encode()
            self.logger.info("Setting up connection to Controller Server...")
            self._socket.connect("tcp://{0}:{1}".format(self._controller_ip, CTRL_MSG_PORT))
            # Initialising connection to Redis (our byte-range locking DB)
            self.logger.info("Setting up Redis connection...")
            self.locking_db = redis.StrictRedis(**redis_config)
            self.flock = FLock(self.locking_db, kwargs.get('locking_type'))
            self.logger.info(f"Dynamo {self._socket.identity} init done")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")

    def run(self):
        self.logger.info(f"Dynamo {self._socket.identity} started")
        try:
            msg = None
            job_id = None
            # Send a connect message
            self._socket.send_json({'message': 'connect'})
            self.logger.debug(f"Client {self._socket.identity} sent back 'connect' message.")
            # Poll the socket for incoming messages. This will wait up to
            # 0.1 seconds before returning False. The other way to do this
            # is is to use zmq.NOBLOCK when reading from the socket,
            # catching zmq.AGAIN and sleeping for 0.1.
            while True:
                try:
                    # Note that we can still use send_json()/recv_json() here,
                    # the DEALER socket ensures we don't have to deal with
                    # client ids at all.
                    # self.logger.debug(f"Blocking waiting for response form socket {self._socket.identity}")
                    job_id, work = self._socket.recv_json()
                    # self.logger.debug(f"Job: {job_id} received from socket {self._socket.identity}")
                    msg = self._do_work(work)
                    self.logger.debug(f"Going to send {job_id}: {msg}")
                    self._socket.send_json(
                        {'message': 'job_done',
                         'result': msg,
                         'job_id': job_id})
                    self.logger.debug(f"{job_id} sent")
                except zmq.ZMQError as zmq_error:
                    self.logger.warn(f"Failed to send message due to: {zmq_error}. Message {job_id} lost!")
                except TypeError:
                    self.logger.error(f"JSON Serialisation error: msg: {msg}")
        except KeyboardInterrupt:
            pass
        except Exception as e:
            self.logger.exception(e)
        finally:
            self._disconnect()

    def _disconnect(self):
        """
        Send the Controller a disconnect message and end the run loop
        """
        self._socket.send_json({'message': 'disconnect'})

    def _do_work(self, work):
        """
        Args:
            work: dict

        Returns: str

        """
        action = work['action']
        data = {}
        mount_point = random.choice(self.mount_points)
        self.logger.debug(f"Incoming job: '{work['action']}' on '{work['data']['target']}' data: {work['data']}")
        try:
            if 'None' in work['data']['target']:
                raise DynamoException(error_codes.NO_TARGET,
                                      "{0}".format("Target not specified", work['data']['target']))
            response = response_action(action, mount_point, work['data'],
                                       dst_mount_point=mount_point, flock=self.flock)
            if response:
                data = response
        except OSError as os_error:
            return build_message('failed', action, data, timestamp(), error_code=os_error.errno,
                                 error_message=os_error.strerror,
                                 path='/'.join([mount_point, work['data']['target']]),
                                 line=sys.exc_info()[-1].tb_lineno)
        except Exception as unhandled_error:
            self.logger.exception(unhandled_error)
            return build_message('failed', action, data, timestamp(), error_message=unhandled_error.args[0],
                                 path=''.join([mount_point, work['data']['target']]),
                                 line=sys.exc_info()[-1].tb_lineno)
        return build_message('success', action, data, timestamp(), path=work['data']['target'])
