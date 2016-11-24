"""
Client load generator
2016 samules (c)
"""
import random
import traceback

import zmq
import sys
import socket
import time
import timeit

timer = timeit.default_timer

sys.path.append('/qa/dynamo')
from logger import pubsub_logger
from config import CTRL_MSG_PORT
from response_actions import response_action, DynamoException
from config import error_codes


def timestamp(now=None):
    if now is None:
        now = timer()
    time_stamp = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(now))
    millisecs = "%.6f" % (now % 1.0,)
    return time_stamp + millisecs[1:]


def build_message(result, action, data, time_stamp, error_code=None, error_message=None, path=None, line=None):
    """
    Result message format:
    Success message format: {'result', 'action', 'target', 'data:{'dirsize, }', 'timestamp'}
    Failure message format: {'result', 'action', 'error_code', 'error_message', 'path', 'linenumber', 'timestamp', 'data:{}'}
    """
    if result == 'success':
        message = {'result': result, 'action': action, 'target': path,
                   'timestamp': str(time_stamp), 'data': data}
    else:
        message = {'result': result, 'action': action, 'error_code': error_code, 'error_message': error_message,
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
            msg = None
            # Send a connect message
            self._socket.send_json({'message': 'connect'})
            # Poll the socket for incoming messages. This will wait up to
            # 0.1 seconds before returning False. The other way to do this
            # is is to use zmq.NOBLOCK when reading from the socket,
            # catching zmq.AGAIN and sleeping for 0.1.
            while not self.stop_event.is_set():
                try:
                    # Note that we can still use send_json()/recv_json() here,
                    # the DEALER socket ensures we don't have to deal with
                    # client ids at all.
                    job_id, work = self._socket.recv_json(zmq.NOBLOCK)
                    msg = self._do_work(work)
                    self.logger.debug("Going to send {0}".format(msg))
                    self._socket.send_json(
                        {'message': 'job_done',
                         'result': msg,
                         'job_id': job_id})
                except zmq.ZMQError as zmq_error:
                    if zmq_error.errno == zmq.EAGAIN:
                        # if No message received, we signalling that we ready to receive a new one
                        self._socket.send_json(
                            {'message': 'job_done',
                             'result': {'result': 'ready', 'timestamp': str(timestamp())}
                             })
                    else:
                        self.logger.error("ZMQ Error. Message {0} lost!".format(msg))
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
        self.logger.debug('Incoming job: \'{0}\' on \'{1}\''.format(work['action'], work['data']['target']))
        try:
            if 'None' in work['data']['target']:
                raise DynamoException(error_codes.NO_TARGET,
                                      "{0}".format("Target not specified", work['data']['target']))
            response = response_action(action, mount_point, work['data'], nodes=self.nodes, server=self._server,
                                       domains=self.domains)
            if response:
                data = response
        except OSError as os_error:
            return build_message('failed', action, data, timestamp(), error_code=os_error.errno,
                                 error_message=os_error.strerror,
                                 path='{0}{1}'.format(mount_point, work['data']['target']),
                                 line=sys.exc_info()[-1].tb_lineno)
        except IOError as io_error:
            return build_message('failed', action, data, timestamp(), error_code=io_error.errno,
                                 error_message=io_error.strerror,
                                 path='{0}{1}'.format(mount_point, work['data']['target']),
                                 line=sys.exc_info()[-1].tb_lineno)
        except DynamoException as dynamo_error:
            return build_message('failed', action, data, timestamp(), error_code=dynamo_error.errno,
                                 error_message=dynamo_error.strerror, path=work['data']['target'],
                                 line=sys.exc_info()[-1].tb_lineno)
        except Exception as unhandled_error:
            return build_message('failed', action, data, timestamp(), error_message=unhandled_error.args[0],
                                 path='{0}{1}'.format(mount_point, work['data']['target']),
                                 line=sys.exc_info()[-1].tb_lineno)
        return build_message('success', action, data, timestamp(), path=work['data']['target'])
