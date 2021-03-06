"""
Server logic is here
2016 samuels (c)

2017 - async_controller module is now a replacement for controller module which is now deprecated
"""
import json
import random
import time
import uuid
import warnings

from threading import Thread

import zmq

from config import CTRL_MSG_PORT
from logger import server_logger
from messages_queue import priority_queue
from server import helpers
from server.request_actions import request_action
from server.response_actions import response_action

MAX_DIR_SIZE = 128 * 1024


class Job(object):
    def __init__(self, work):
        self.id = uuid.uuid4().hex
        self.work = work


class Controller(object):
    def __init__(self, stop_event, dir_tree, port=CTRL_MSG_PORT):
        """
        Args:
            stop_event: Event
            dir_tree: DirTree
            port: int
        """
        super(Controller, self).__init__()
        warnings.warn("Controller class is deprecated and replaced with AsyncController", DeprecationWarning)
        self.stop_event = stop_event
        self.logger = server_logger.Logger().logger
        self._dir_tree = dir_tree  # Controlled going to manage directory tree structure
        self._context = zmq.Context()
        self.workers = {}
        # We won't assign more than 50 jobs to a worker at a time; this ensures
        # reasonable memory usage, and less shuffling when a worker dies.
        self.max_jobs_per_worker = 1000
        # When/if a client disconnects we'll put any unfinished work in here,
        # work_iterator() will return work from here as well.
        self._work_to_requeue = []
        self.__max_rcv_queue_size = 50
        self.__rcv_message_queue = priority_queue.PriorityQueue()
        self.__rcv_message_worker_thread = Thread(target=self.rcv_messages_worker)
        # Socket to send messages on from Manager
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.bind("tcp://*:{0}".format(port))

    @property
    def dir_tree(self):
        return self._dir_tree

    def rcv_messages_worker(self):
        while not self.stop_event.is_set:
            pass

    @property
    def get_next_job(self):
        actions = ['mkdir', 'list', 'list', 'list', 'list', 'delete', 'touch', 'touch', 'touch', 'touch', 'touch',
                   'touch', 'stat', 'stat', 'stat', 'stat', 'stat', 'read', 'read', 'read', 'read', 'rename',
                   'rename_exist']

        while True:
            action = random.choice(actions)
            # if some client disconnected, messages assigned to him won't be lost
            if self._work_to_requeue:
                yield self._work_to_requeue.pop()
            # The very first event must be mkdir
            if self._dir_tree.get_last_node_tag() == 'Root':
                action = "mkdir"
                self._dir_tree.append_node()
                target = self._dir_tree.get_last_node_tag()
                yield Job({'action': action, 'target': target})
            yield Job({'action': action, 'target': request_action(action, self.logger, self._dir_tree)})

    def _get_next_worker_id(self):
        """Return the id of the next worker available to process work. Note
        that this will return None if no clients are available.
        """
        # It isn't strictly necessary since we're limiting the amount of work
        # we assign, but just to demonstrate that we're doing our own load
        # balancing we'll find the worker with the least work
        if self.workers:
            worker_id, work = sorted(self.workers.items(),
                                     key=lambda x: len(x[1]))[0]
            if len(work) < self.max_jobs_per_worker:
                return worker_id
        # No worker is available. Our caller will have to handle this.
        return None

    def _handle_worker_message(self, worker_id, message):
        """Handle a message from the worker identified by worker_id.

        {'message': 'connect'}
        {'message': 'disconnect'}
        {'message': 'job_done', 'job_id': 'xxx', 'result': 'yyy'}
        """
        if message['message'] == 'connect':
            assert worker_id not in self.workers
            self.workers[worker_id] = {}  # Adding new worker ot workers dict on connect event
            self.logger.info('[%s]: connect', worker_id)
        elif message['message'] == 'disconnect':
            # Remove the worker so no more work gets added, and put any
            # remaining work into _work_to_requeue
            remaining_work = self.workers.pop(worker_id)
            self._work_to_requeue.extend(remaining_work.values())
            self.logger.info('[%s]: disconnect, %s jobs requeued', worker_id,
                             len(remaining_work))
        elif message['message'] == 'job_done':
            result = message['result']
            job = self.workers[worker_id].pop(message['job_id'])
            #  Under work - Priority Queue
            # self.__rcv_message_queue.put(result)
            #
            self._process_results(worker_id, job, result)
        else:
            raise Exception('unknown message: %s' % message['message'])

    def _process_results(self, worker_id, job, incoming_message):
        """
        Result message format:
        Success message format: {'result', 'action', 'target', 'data{}', 'timestamp'}
        Failure message format: {'result', 'action', 'error_code', 'error_message', 'target', 'linenumber', 'timestamp',
         'data{}'}
        """
        formatted_message = helpers.message_to_pretty_string(incoming_message)
        self.logger.info('[{0}]: finished {1}, result: {2}'.format(worker_id, job.id, formatted_message))
        response_action(self.logger, incoming_message, self.dir_tree)

    def run(self):
        try:
            # for job in self.work_iterator():
            for job in self.get_next_job:
                next_worker_id = None

                while next_worker_id is None:
                    # First check if there are any worker messages to process. We
                    # do this while checking for the next available worker so that
                    # if it takes a while to find one we're still processing
                    # incoming messages.
                    while self._socket.poll(0):
                        # Note that we're using recv_multipart() here, this is a
                        # special method on the ROUTER socket that includes the
                        # id of the sender. It doesn't handle the json decoding
                        # automatically though so we have to do that ourselves.
                        worker_id, message = self._socket.recv_multipart()
                        message = json.loads(message.decode('utf8'))
                        self._handle_worker_message(worker_id, message)
                    # If there are no available workers (they all have 50 or
                    # more jobs already) sleep for half a second.
                    next_worker_id = self._get_next_worker_id()
                    if next_worker_id is None:
                        time.sleep(0.5)
                # We've got a Job and an available worker_id, all we need to do
                # is send it. Note that we're now using send_multipart(), the
                # counterpart to recv_multipart(), to tell the ROUTER where our
                # message goes.
                self.logger.info('sending job %s to worker %s', job.id,
                                 next_worker_id)
                self.workers[next_worker_id][job.id] = job
                self._socket.send_multipart(
                    [next_worker_id, json.dumps((job.id, job.work)).encode('utf8')])
                if self.stop_event.is_set():
                    break
        except Exception as generic_error:
            self.logger.exception(generic_error)
            raise
        self.stop_event.set()
