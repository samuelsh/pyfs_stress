"""
Server logic is here
2016 samuels (c)
"""
import hashlib
import json
import random
import time
import uuid

import datetime
from threading import Thread

import zmq
from treelib.tree import NodeIDAbsentError

from config import CTRL_MSG_PORT
from messages_queue import priority_queue

MAX_DIR_SIZE = 128 * 1024


class Job(object):
    def __init__(self, work):
        self.id = uuid.uuid4().hex
        self.work = work


class Controller(object):
    def __init__(self, logger, stop_event, dir_tree, port=CTRL_MSG_PORT):
        """
        Args:
            logger: Logger
            stop_event: Event
            dir_tree: DirTree
            port: int
        """
        super(Controller, self).__init__()
        self.stop_event = stop_event
        self.logger = logger
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

    def rcv_messages_worker(self):
        while 1:
            pass

    @property
    def get_next_job(self):
        actions = ['mkdir', 'list', 'list', 'list', 'list', 'delete', 'touch', 'touch', 'touch', 'touch', 'touch',
                   'touch', 'stat', 'stat', 'stat', 'stat', 'stat', 'read', 'read', 'read', 'read']

        while True:
            action = random.choice(actions)
            target = 'None'
            # if some client disconnected, messages assigned to him won't be lost
            if self._work_to_requeue:
                yield self._work_to_requeue.pop()
            # The very first event must be mkdir
            if self._dir_tree.get_last_node_tag() == 'Root':
                action = "mkdir"
                self._dir_tree.append_node()
                target = self._dir_tree.get_last_node_tag()
                yield Job({'action': action, 'target': target})
            if action == "mkdir":
                # if self._dir_tree.get_last_node_data().size >= MAX_DIR_SIZE:
                if len(self._dir_tree.nids) < 100:
                    self._dir_tree.append_node()
                self.logger.debug(
                    "Controller: New dir appended to list {0}".format(self._dir_tree.get_last_node_tag()))
                target_dir = self._dir_tree.get_random_dir_not_synced()
                if target_dir:
                    target = target_dir.data.name
                self.logger.debug(
                    "Controller: Dir {0} current size is {1}".format(target, self._dir_tree.get_last_node_data().size))
            elif action == "touch":
                rdir = self._dir_tree.get_random_dir_synced()
                if not rdir:
                    target = 'None'
                else:
                    fname = rdir.data.touch()
                    target = "/{0}/{1}".format(rdir.tag, fname)
            elif action == 'stat' or action == 'read':
                rdir = self._dir_tree.get_random_dir_synced()
                if rdir:
                    rfile = rdir.data.get_random_file()
                    if not rfile:
                        target = 'None'
                    else:
                        fname = rfile.name
                        target = "/{0}/{1}".format(rdir.tag, fname)
                else:
                    target = 'None'
            elif action == "list":
                rdir = self._dir_tree.get_random_dir_synced()
                if not rdir:
                    target = 'None'
                else:
                    target = rdir.data.name
            elif action == 'delete':
                rdir = self._dir_tree.get_random_dir_synced()
                if not rdir:
                    target = 'None'
                else:
                    file_to_delete = rdir.data.get_random_file()
                    if not file_to_delete:
                        target = 'None'
                    else:
                        fname = file_to_delete.name
                        target = "/{0}/{1}".format(rdir.tag, fname)
                        # target = self._dir_tree.get_random_dir_files()
            yield Job({'action': action, 'target': target})

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
            self.workers[worker_id] = {}
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
        self.logger.info('[{0}]: finished {1}, result: {2}'.format(worker_id, job.id, formatted_message))
        if incoming_message['result'] == 'success':
            if incoming_message['action'] == 'mkdir':  # mkdir successful which means is synced with storage
                syncdir = self._dir_tree.get_dir_by_name(incoming_message['target'])
                syncdir.data.size = int(incoming_message['data']['dirsize'])
                syncdir.data.ondisk = True
                syncdir.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                                   '%Y/%m/%d %H-%M-%S.%f')
                self._dir_tree.synced_nodes.append(hashlib.md5(syncdir.data.name).hexdigest())
                self.logger.debug(
                    "Directory {0} was created at: {1}".format(syncdir.data.name, syncdir.creation_time))
                self.logger.info(
                    'Directory {0} is synced. Size is {1}'.format(syncdir.data.name,
                                                                  int(incoming_message['data']['dirsize'])))
            elif incoming_message['result'] == 'touch':
                path = incoming_message['target'].split('/')[1:]  # folder:file
                syncdir = self._dir_tree.get_dir_by_name(path[0])
                if not syncdir:
                    self.logger.debug(
                        "Directory {0} already removed from active dirs list, dropping touch {1}".format(path[0],
                                                                                                         path[1]))
                # There might be a raise when successful mkdir message will arrive after successful touch message
                # So we won't check here if dir is already synced
                else:
                    for f in syncdir.data.files:
                        if f.name == path[1]:  # Now, when we got reply from client that file was created,
                            #  we can mark it as synced
                            syncdir.data.size = int(incoming_message['data']['dirsize'])
                            f.ondisk = True
                            f.creation_time = datetime.datetime.strptime(incoming_message['timestamp'],
                                                                         '%Y/%m/%d %H-%M-%S.%f')
                            self.logger.debug(
                                "File {0}/{1} was created at: {2}".format(path[0], path[1], f.creation_time))
                            self.logger.info(
                                'File {0}/{1} is synced. Directory size updated to {2}'.format(path[0], path[1],
                                                                                               int(incoming_message[
                                                                                                       'data'][
                                                                                                       'dirsize'])))
                            break
            elif incoming_message['action'] == 'delete':
                path = incoming_message['target'].split('/')[1:]  # folder:file
                deldir = self._dir_tree.get_dir_by_name(path[0])
                if not deldir:
                    self.logger.debug(
                        "Directory {0} already removed from active dirs list, skipping....".format(path[0]))
                else:
                    self.logger.debug('Directory exists {0}, going to delete {1}'.format(deldir.data.name, path[1]))
                    if deldir.data.ondisk:
                        rfile = deldir.data.get_file_by_name(path[1])
                        if rfile and rfile.ondisk:
                            self.logger.debug('File {0}/{1} is found, removing'.format(path[0], path[1]))
                            rfile.ondisk = False
                            self.logger.info('File {0}/{1} is removed form disk'.format(path[0], path[1]))
                        else:
                            self.logger.warn("File {0}/{1} is not on disk, nothing to update".format(path[0], path[1]))
                    else:
                        self.logger.debug("Directory {0} is not on disk, nothing to update".format(deldir.data.name))
        else:  # failure analysis
            if incoming_message['error_message'] == "Target not specified" or "File exists" in incoming_message[
                'error_message']:
                return
            # in case that touch op failed due to size limit
            if incoming_message['action'] == "touch" and "size limit" in incoming_message['error_message']:
                rdir_name = incoming_message['target'].split('/')[1]  # get target folder name from path
                try:
                    self.logger.info("Directory {0} going to be removed from dir tree".format(rdir_name))
                    self._dir_tree.remove_dir_by_name(rdir_name)
                    node_index = self._dir_tree.synced_nodes.index(hashlib.md5(rdir_name).hexdigest())
                    del self._dir_tree.synced_nodes[node_index]
                    node_index = self._dir_tree.nids.index(hashlib.md5(rdir_name).hexdigest())
                    del self._dir_tree.nids[node_index]
                    self.logger.info(
                        "Directory {0} is reached its size limit and removed from active dirs list".format(rdir_name))
                    self._dir_tree.append_node()
                    self.logger.info(
                        "New Directory node appended to tree {0}".format(self._dir_tree.get_last_node_tag()))
                except NodeIDAbsentError:
                    self.logger.debug(
                        "Directory {0} already removed from active dirs list, skipping....".format(rdir_name))
            # in case stat, read or delete ops failed for some reason
            elif incoming_message['action'] == "stat" or incoming_message['action'] == "delete" or \
                            incoming_message['action'] == 'read':
                rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
                rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path

                rdir = self._dir_tree.get_dir_by_name(rdir_name)
                if rdir:
                    rfile = rdir.data.get_file_by_name(rfile_name)
                    if rfile and rfile.ondisk:
                        error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H-%M-%S.%f')
                        if error_time > rfile.creation_time:
                            self.logger.error(
                                "Result Verify FAILED: Operation {0} failed on file {1} which is on disk".format(
                                    incoming_message['action'], rdir_name + "/" + rfile_name))
                    else:
                        self.logger.info('Result verify OK: File {0} is not on disk'.format(rfile_name))
                else:
                    self.logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
            # in case if touch op failed and it's not dir size limit error
            elif incoming_message['action'] == "touch":
                rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
                try:  # If there's no filename, that means that stat failed
                    rfile_name = incoming_message['target'].split('/')[4]  # get target file name from path
                except IndexError:
                    rfile_name = None
                rdir = self._dir_tree.get_dir_by_name(rdir_name)
                if rdir and rdir.data.ondisk:
                    error_time = datetime.datetime.strptime(incoming_message['timestamp'], '%Y/%m/%d %H-%M-%S.%f')
                    if error_time > rdir.creation_time:
                        self.logger.error(
                            "Result Verify FAILED: Operation {0} failed on {1}/{2} which is on disk".format(
                                incoming_message['action'], rdir_name, rfile_name))
                else:
                    self.logger.info('Result verify OK: Directory {0} is not on disk'.format(rdir_name))
            else:
                rdir_name = incoming_message['target'].split('/')[3]  # get target folder name from path
                self.logger.error(
                    'Operation {0} FAILED UNEXPECTEDLY on Directory {1} due to {2}'.format(incoming_message['action'],
                                                                                           rdir_name,
                                                                                           incoming_message[
                                                                                               'error_message']))

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
