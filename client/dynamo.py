"""
Client load generator
2016 samules (c)
"""
import random
import time
import os
import zmq
import sys
import socket
import time
import timeit

timer = timeit.default_timer

sys.path.append('/qa/dynamo')
from config import CTRL_MSG_PORT, CLIENT_MOUNT_POINT
from utils import shell_utils

MAX_DIR_SIZE = 128 * 1024


class DynamoIOException(Exception):
    pass


def timestamp(now=None):
    if now is None:
        now = timer()
    timestamp = time.strftime("%Y/%m/%d %H-%M-%S", time.localtime(now))
    millisecs = "%.3f" % (now % 1.0,)
    return timestamp + millisecs[1:]


class Dynamo(object):
    def __init__(self, logger, stop_event, controller, server, nodes, domains, proc_id=None):
        self.stop_event = stop_event
        self.logger = logger
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
        logger.info("Dynamo {0} init done".format(self._socket.identity))

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
                if self._socket.poll(100):
                    # Note that we can still use send_json()/recv_json() here,
                    # the DEALER socket ensures we don't have to deal with
                    # client ids at all.
                    job_id, work = self._socket.recv_json()
                    self._socket.send_json(
                        {'message': 'job_done',
                         'result': self._do_work(work),
                         'job_id': job_id})
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
        Success message format: {'result', 'action', 'target', 'data'}
        Failure message format: {'result', 'action', 'error message: target', 'linenumber', 'timestamp', 'data'}
        Args:
            work: dict

        Returns: str

        """
        action = work['action']
        data = None
        # /mnt/DIRSPLIT-node0.g8-5
        mount_point = "".join(
            "/mnt/DIRSPLIT-node{0}.{1}-{2}".format(random.randint(0, self.nodes), self._server,
                                                   random.randint(0, self.domains)))
        self.logger.debug('Incoming target: {0}'.format(work['target']))
        try:
            if work['target'] == 'None':
                raise DynamoIOException("{0}".format("Target not specified"))
            if action == 'mkdir':
                os.mkdir("{0}/{1}".format(mount_point, work['target']))
                data = os.stat("{0}/{1}".format(mount_point, work['target'])).st_size
            elif action == 'touch':
                dirsize = os.stat("{0}/{1}".format(mount_point, work['target'].split('/')[1])).st_size
                if dirsize >= MAX_DIR_SIZE:  # if Directory entry size > 64K, we'll stop writing new files
                    data = work['target']
                    raise DynamoIOException("Directory Entry reached {0} size limit".format(MAX_DIR_SIZE))
                shell_utils.touch('{0}{1}'.format(mount_point, work['target']))
                data = os.stat("{0}/{1}".format(mount_point, work['target'].split('/')[1])).st_size
            elif action == 'stat':
                os.stat("{0}{1}".format(mount_point, work['target']))
            elif action == 'list':
                os.listdir('{0}/{1}'.format(mount_point, work['target']))
            elif action == 'delete':
                dirpath = work['target'].split('/')[1]
                fname = work['target'].split('/')[2]
                os.remove('{0}/{1}/{2}'.format(mount_point, dirpath, fname))
        except Exception as work_error:
            result = "failed:{0}:{1}:{2}:{3}:{4}".format(action, work_error, sys.exc_info()[-1].tb_lineno, timestamp(),
                                                         data)
            self.logger.info("Sending back result {0}".format(result))
            return result
        result = "success:{0}:{1}:{2}:{3}".format(action, work['target'], data, timestamp())
        self.logger.info("Sending back result {0}".format(result))
        return result
