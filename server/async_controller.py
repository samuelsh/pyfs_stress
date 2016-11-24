"""
Asynchronous Server logic is here
2016 samuels (c)
"""
import Queue
import timeit
from bisect import bisect
from random import randint, random
import json
import random
import time
import uuid

from threading import Thread

import zmq

from config import CTRL_MSG_PORT
from logger import server_logger
from server import helpers
from server.request_actions import request_action
from server.response_actions import response_action

timer = timeit.default_timer

MAX_DIR_SIZE = 128 * 1024
MAX_CONTROLLER_WORKERS = 2

__author__ = 'samuels'


def timestamp(now=None):
    if now is None:
        now = timer()
    time_stamp = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(now))
    millisecs = "%.6f" % (now % 1.0,)
    return time_stamp + millisecs[1:]


def weighted_choice(choices):
    values, weights = zip(*choices)
    total = 0
    cum_weights = []
    for w in weights:
        total += w
        cum_weights.append(total)
    x = random.random() * total
    i = bisect(cum_weights, x)
    return values[i]


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
        try:
            self.stop_event = stop_event
            self.logger = server_logger.Logger().logger
            self._dir_tree = dir_tree  # Controlled going to manage directory tree structure
            self.client_workers = {}
            self.incoming_message_workers = []
            # We won't assign more than 50 jobs to a worker at a time; this ensures
            # reasonable memory usage, and less shuffling when a worker dies.
            self.max_jobs_per_worker = 100
            # When/if a client disconnects we'll put any unfinished work in here,
            # get_next_job() will return work from here as well.
            self._work_to_requeue = []
            self._incoming_message_queue = Queue.PriorityQueue()
            self._outgoing_message_queue = Queue.Queue()
            self.logger.info("Starting Async Server....")
            proxy_device_thread = AsyncControllerServer(self.logger, self.stop_event, self._incoming_message_queue,
                                                        self._outgoing_message_queue)
            proxy_device_thread.start()
        except Exception as e:
            self.logger.exception(e)

    @property
    def dir_tree(self):
        return self._dir_tree

    @property
    def get_next_job(self):
        actions = [('mkdir', 5), ('list', 5), ('delete', 5), ('touch', 55), ('stat', 5), ('read', 5), ('rename', 5),
                   ('rename_exist', 5), ('write', 5), ('truncate', 5)]
        while True:
            action = weighted_choice(actions)
            # if some client disconnected, messages assigned to him won't be lost
            if self._work_to_requeue:
                yield self._work_to_requeue.pop()
            # The very first event must be mkdir
            if self._dir_tree.get_last_node_tag() == 'Root':
                action = "mkdir"
                self._dir_tree.append_node()
                target = self._dir_tree.get_last_node_tag()
                yield Job({'action': action, 'data': {'target': target}})
            yield Job({'action': action, 'data': request_action(action, self.logger, self._dir_tree)})

    def _get_next_worker_id(self):
        """Return the id of the next worker available to process work. Note
        that this will return None if no clients are available.
        """
        # It isn't strictly necessary since we're limiting the amount of work
        # we assign, but just to demonstrate that we're doing our own load
        # balancing we'll find the worker with the least work
        if self.client_workers:
            worker_id, work = sorted(self.client_workers.items(),
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
            assert worker_id not in self.client_workers
            self.client_workers[worker_id] = {}
            self.logger.info('[%s]: connect', worker_id)
        elif message['message'] == 'disconnect':
            # Remove the worker so no more work gets added, and put any
            # remaining work into _work_to_requeue
            remaining_work = self.client_workers.pop(worker_id)
            self._work_to_requeue.extend(remaining_work.values())
            self.logger.info('[%s]: disconnect, %s jobs requeued', worker_id,
                             len(remaining_work))
        elif message['message'] == 'job_done':
            result = message['result']
            job = self.client_workers[worker_id].pop(message['job_id'])
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
            for job in self.get_next_job:
                next_worker_id = None

                while next_worker_id is None:
                    # First check if there are any worker messages to process. We
                    # do this while checking for the next available worker so that
                    # if it takes a while to find one we're still processing
                    # incoming messages.
                    _, (worker_id, message) = self._incoming_message_queue.get()
                    self._handle_worker_message(worker_id, message)
                    # If there are no available workers (they all have 50 or
                    # more jobs already) sleep for half a second.
                    next_worker_id = self._get_next_worker_id()
                # We've got a Job and an available worker_id, all we need to do
                # is send it. Note that we're now using send_multipart(), the
                # counterpart to recv_multipart(), to tell the ROUTER where our
                # message goes.
                # self.logger.debug('sending job %s to worker %s', job.id,
                #                   next_worker_id)
                self.client_workers[next_worker_id][job.id] = job
                self._outgoing_message_queue.put((next_worker_id, job.id, job.work))
                # self.logger.info("Incoming Queue: {0} Outgoing Queue: {1}".format(self._incoming_message_queue.qsize(),
                #                                                                   self._outgoing_message_queue.qsize()))
                if self.stop_event.is_set():
                    break
        except Exception as generic_error:
            self.logger.exception(generic_error)
            raise generic_error
        finally:
            self.stop_event.set()


class AsyncControllerServer(Thread, object):
    def __init__(self, logger, stop_event, incoming_queue, outgoing_queue):
        super(AsyncControllerServer, self).__init__()
        self._stop_event = stop_event
        self._logger = logger
        self._incoming_queue = incoming_queue
        self._outgoing_queue = outgoing_queue
        self._context = zmq.Context()
        self._frontend = self._context.socket(zmq.ROUTER)
        self._frontend.bind("tcp://*:{0}".format(CTRL_MSG_PORT))
        self._backend = self._context.socket(zmq.DEALER)
        self._backend.bind('inproc://backend')

    def run(self):
        self._logger.info(
            "Async Controller Server thread {0} started".format(self.name))
        try:
            workers = []
            for _ in range(MAX_CONTROLLER_WORKERS):
                worker = AsyncControllerWorker(self._logger, self._context, self._incoming_queue, self._outgoing_queue,
                                               self._stop_event)
                workers.append(worker)
                worker.start()
            self._logger.info("Starting Proxy Device...")
            zmq.proxy(self._frontend, self._backend)
        except zmq.ZMQError as zmq_error:
            self._logger.exception(zmq_error)
            self._stop_event.set()
            raise zmq_error
        except Exception as generic_error:
            self._logger.exception("Unhandled exception {0}".format(generic_error))
            self._stop_event.set()
            raise generic_error

    def __del__(self):
        self._logger.info("Closing sockets...")
        self._context.close()
        self._backend.close()
        self._context.term()


class AsyncControllerWorker(Thread, object):
    def __init__(self, logger, context, incoming_queue, outgoing_queue, stop_event):
        super(AsyncControllerWorker, self).__init__()
        self._logger = logger
        self._context = context
        self.stop_event = stop_event
        self.incoming_queue = incoming_queue
        self.outgoing_queue = outgoing_queue
        self.max_jobs_per_worker = 1000
        try:
            self._worker = self._context.socket(zmq.DEALER)
            self._worker.connect('inproc://backend')
        except zmq.ZMQError as zmq_error:
            self._logger.exception(zmq_error)
            self.stop_event.set()
            raise zmq_error

    def run(self):
        self._logger.info("Controller incoming/outgoing messages worker thread {0} started".format(self.name))
        while not self.stop_event.is_set():
            try:
                worker_id, message = self._worker.recv_multipart()  # flags=zmq.NOBLOCK)
                message = json.loads(message.decode('utf8'))
                if message['message'] == 'connect' or message['message'] == 'disconnect':
                    time_stamp = timestamp()
                else:
                    time_stamp = message['result']['timestamp']
                self.incoming_queue.put(
                    (time_stamp, (worker_id, message)))  # Putting messages to queue by timestamp priority
            except zmq.ZMQError as zmq_error:
                self._logger.exception("ZMQ Error {0}".format(zmq_error))
                self.stop_event.set()
                raise zmq_error
            except Exception as generic_error:
                self._logger.exception("Unhandled exception {0}".format(generic_error))
                self.stop_event.set()
                raise
            try:
                #  Sending out messages from outgoing message queue
                next_worker_id, job_id, job_work = self.outgoing_queue.get()
                self._worker.send_multipart(
                    [next_worker_id, json.dumps((job_id, job_work)).encode('utf8')])
            except Queue.Empty:
                pass
            except zmq.ZMQError as zmq_error:
                if zmq_error.errno == zmq.EAGAIN:
                    pass
                else:
                    self._logger.error("ZMQ Error: {0}".format(zmq_error))
                    self.stop_event.set()
                    raise
            except Exception as generic_error:
                self._logger.exception("Unhandled exception {0}".format(generic_error))
                self.stop_event.set()
                raise

    def __del__(self):
        self._logger.info("Closing sockets...")
        self._context.close()
        self._worker.close()
        self._context.term()
