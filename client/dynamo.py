"""
Client load generator
2016 samules (c)
"""
import json
import random
import os

import datetime
import zmq
import sys
import socket
import time
import timeit

timer = timeit.default_timer

sys.path.append('/qa/dynamo')
from logger import pubsub_logger
from config import CTRL_MSG_PORT, CLIENT_MOUNT_POINT
from utils import shell_utils

MAX_DIR_SIZE = 128 * 1024


class DynamoException(Exception):
    pass


def timestamp(now=None):
    if now is None:
        now = timer()
    time_stamp = time.strftime("%Y/%m/%d %H-%M-%S", time.localtime(now))
    millisecs = "%.3f" % (now % 1.0,)
    return time_stamp + millisecs[1:]


def build_message(result, action, data, time_stamp, error_message=None, path=None, line=None):
    """
    Result message format:
    Success message format: {'result', 'action', 'target', 'data:{'dirsize, }', 'timestamp'}
    Failure message format: {'result', 'action', 'error_message', 'path', 'linenumber', 'timestamp', 'data:{}'}
    """
    if result == 'success':
        message = {'result': result, 'action': action, 'target': path,
                   'timestamp': str(time_stamp), 'data': data}
    else:
        message = {'result': result, 'action': action, 'error_message': error_message,
                   'target': path, 'linenum': line,
                   'timestamp': str(time_stamp), 'data': data}
    return message


class Dynamo(object):
    def __init__(self, stop_event, controller, server, nodes, domains, proc_id=None):
        self.stop_event = stop_event
        self.logger = pubsub_logger.PUBLogger(controller).logger
        self._server = server  # Server Cluster hostname
        self.nodes = nodes
        self.domains = domains
        self._context = zmq.Context()
        self._controller_ip = socket.gethostbyname(controller)
        # Socket to send messages on by client
        self._socket = self._context.socket(zmq.DEALER)
        # We don't need to store the id anymore, the socket will handle it
        # all for us.
        # We'll use client host name + process ID to identify the socket
        self._socket.identity = "{0}:0x{1:x}".format(socket.gethostname(), proc_id)
        self._socket.connect("tcp://{0}:{1}".format(self._controller_ip, CTRL_MSG_PORT))
        self.logger.info("Dynamo {0} init done".format(self._socket.identity))

    def run(self):
        self.logger.info("Dynamo {0} started".format(self._socket.identity))
        try:
            # Send a connect message
            self._socket.send_json({'message': 'connect'})
            # Poll the socket for incoming messages. This will wait up to
            # 0.1 seconds before returning False. The other way to do this
            # is is to use zmq.NOBLOCK when reading from the socket,
            # catching zmq.AGAIN and sleeping for 0.1.
            while not self.stop_event.is_set():
                try:
                    if self._socket.poll(100):
                        # Note that we can still use send_json()/recv_json() here,
                        # the DEALER socket ensures we don't have to deal with
                        # client ids at all.
                        job_id, work = self._socket.recv_json()
                        msg = self._do_work(work)
                        self._socket.send_json(
                            {'message': 'job_done',
                             'result': msg,
                             'job_id': job_id})
                except zmq.ZMQError:
                    self.logger.exception("ZMQ Error. Messsage {0} lost!".format(msg))
        except KeyboardInterrupt:
            pass
        finally:
            self._disconnect()

    def _disconnect(self):
        """
        Send the Controller a disconnect message and end the run loop
        """
        self.stop_event.set()
        self._socket.send_json({'message': 'disconnect'})

    def _do_work(self, work):
        """
        Args:
            work: dict

        Returns: str

        """
        action = work['action']
        data = {}
        mount_point = "".join(
            "/mnt/DIRSPLIT-node{0}.{1}-{2}".format(random.randint(0, self.nodes - 1), self._server,
                                                   random.randint(0, self.domains - 1)))
        self.logger.debug('Incoming target: {0}'.format(work['target']))
        try:
            if 'None' in work['target']:
                raise DynamoException("{0}".format("Target not specified"))
            if action == 'mkdir':
                os.mkdir("{0}/{1}".format(mount_point, work['target']))
                data['dirsize'] = os.stat("{0}/{1}".format(mount_point, work['target'])).st_size
            elif action == 'touch':
                dirsize = os.stat("{0}/{1}".format(mount_point, work['target'].split('/')[1])).st_size
                if dirsize > MAX_DIR_SIZE:  # if Directory entry size > 128K, we'll stop writing new files
                    data['target_path'] = work['target']
                    raise DynamoException("Directory Entry reached {0} size limit".format(MAX_DIR_SIZE))
                shell_utils.touch('{0}{1}'.format(mount_point, work['target']))
                data['dirsize'] = os.stat("{0}/{1}".format(mount_point, work['target'].split('/')[1])).st_size
            elif action == 'stat':
                os.stat("{0}{1}".format(mount_point, work['target']))
            elif action == 'read':
                with open("{0}/{1}".format(mount_point, work['target']), 'r') as f:
                    f.read()
            elif action == 'list':
                os.listdir('{0}/{1}'.format(mount_point, work['target']))
            elif action == 'delete':
                dirpath = work['target'].split('/')[1]
                fname = work['target'].split('/')[2]
                os.remove('{0}/{1}/{2}'.format(mount_point, dirpath, fname))
        except (IOError, OSError) as work_error:
            return build_message('failed', action, data, timestamp(), error_message=work_error.strerror,
                                 path=work_error.filename, line=sys.exc_info()[-1].tb_lineno)
        except DynamoException as dynamo_error:
            return build_message('failed', action, data, timestamp(), error_message=dynamo_error.args[0],
                                 path=work['target'], line=sys.exc_info()[-1].tb_lineno)
        except Exception as unhandled_error:
            return build_message('failed', action, data, timestamp(), error_message=unhandled_error,
                                 path=work['target'], line=sys.exc_info()[-1].tb_lineno)
        return build_message('success', action, data, timestamp(), path=work['target'])
