#!/usr/bin/python
import multiprocessing
import os
import subprocess
import os.path
import sys
from Queue import Empty
from multiprocessing import Process, Pool
from optparse import OptionParser
import traceback

# import hanging_threads
import time

MAX_PROCESSES = 16
unsearched = multiprocessing.Manager().Queue()
files_queue = multiprocessing.Manager().Queue()
stop_event = multiprocessing.Event()
dir_scanner_pool = None
stopped_processes_count = 0


def fscat(options, queue, results_q, name, is_multithread=True):
    problematic_ranges = []
    problem = None
    while not queue.empty():
        try:
            file = queue.get()
            print name + ": getting %s from queue" % file
            if not os.path.isfile(file):
                continue
            rangeslength = [0, 0, 0]  # Null, oca, disk
            output = "file " + file
            problem = False
            over64KBwithoutOCAorNULL = False
            command = "fscat"
            p = subprocess.Popen([command, "-M", file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            c = p.communicate()
            if p.returncode != 0:
                if options.verbose:
                    print c[1]
                    print c[0]
                print name + ": fscat returned " + str(p.returncode) + " for file " + file + ", skipping."
                continue
            if options.fscatoutput:
                print c[1]
                print c[0]
            if len(c[0]) == 0:
                output += " is inline"
            startofdiskrange = 0
            for line in c[0].splitlines():
                line = line.split(" -> ")
                line = [line[0].split("{Mapping: [")[1].split("]")[0].split("-"), line[1]]
                if not line[0][1] == "8000000000000000":  # last range points outside the file and is always Null
                    #  - just ignore it
                    line[0] = [int(x, 16) for x in line[0]]
                    rangelen = line[0][1] - line[0][0]
                    if "-sec" in line[1]:
                        print name + ": " + output + " contains redundant data!"
                        continue
                    if "oca" in line[1]:
                        rangeslength[1] = rangeslength[1] + rangelen
                        startofdiskrange = line[0][1]  # reset consecutive disk range count
                    elif "[0-0]" in line[1]:
                        rangeslength[0] = rangeslength[0] + rangelen
                        startofdiskrange = line[0][1]
                    elif "disk" in line[1]:
                        rangeslength[2] = rangeslength[2] + rangelen
                        if (line[0][
                                1] - startofdiskrange) >= 65536:  # consecutive disk range larger than 64KB - shouldn't happen
                            # after dedup for files of at least 64KB
                            problem = True
                            problematic_ranges.append("[ %x - %x ]" % (int(line[0][0]), int(line[0][1])))
                    elif "shared null" in line[1] or "owned null" in line[1]:  # Skipping "shared null" lines
                        startofdiskrange = line[0][1]
                        if options.verbose:
                            print name + ": " + output + " null mapping"
                    else:
                        print name + ": " + output + " parsing error!"
                        print "Parsing error DEBUG: %s \n" % line[1]
                        break
            output = "Null ranges: " + str(rangeslength[0]) + " \tOca ranges: " + str(
                rangeslength[1]) + " \tDisk ranges: " + str(rangeslength[2]) + " " + output
            if problem or options.filesize or (
                                options.outputnondeduped and rangeslength[0] == 0 and rangeslength[1] == 0 and
                            rangeslength[
                                2] >= 65536):
                filesize = os.path.getsize(file)
                output = output + " Size: " + str(filesize)
                if filesize < 65536:  # file too small to be deduped (e.g. a 63KB file is mapped to a 64KB disk range)
                    problem = False
                else:
                    if rangeslength[0] == 0 and rangeslength[1] == 0 and rangeslength[2] >= 65536:
                        over64KBwithoutOCAorNULL = True
                        output += " is over 64KB without OCA or NULL ranges!!!"
                if problem:
                    if options.verbose:
                        for mrange in problematic_ranges:  # Will print all ranges that are expected to be deduped
                            output += " contains a disk range %s that should have been deduped!\n" % mrange
                    else:
                        output += " contains a disk range/s that should have been deduped!"
            if options.verbose or problem or over64KBwithoutOCAorNULL:
                print name + ": " + output

            if is_multithread is False:  # in case function is called as single execution, we won't loop
                break

        except Exception as e:
            raise e

    results_q.put(problem)
    print name + ": finished"


# Driectory tree crawler functions
def explore_path(path):
    directories = []
    for filename in os.listdir(path):
        fullname = os.path.join(path, filename)
        if os.path.isdir(fullname):
            directories.append(fullname)
        else:
            print "Putting " + fullname + " to files query"
            files_queue.put(fullname)
    return directories


def dir_scan_worker(task_num):
    while not unsearched.empty():
        try:
            path = unsearched.get_nowait()
            dirs = explore_path(path)
            print "Task: " + str(task_num) + " >>> Explored path: " + path
            for newdir in dirs:
                unsearched.put(newdir)
        except Empty:
            print "Task: " + str(task_num) + " reached end of the queue"
    print "Done dir_scan_worker " + str(task_num)
    unsearched.task_done()


def run_crawler(base_path):
    global dir_scanner_pool
    dir_scanner_pool = Pool(multiprocessing.cpu_count())
    if not os.path.isdir(base_path):
        raise IOError("Base path not found: " + base_path)

    cpu_count = multiprocessing.cpu_count()

    # acquire the list of all paths inside base path
    first_level_dirs = next(os.walk(base_path))[1]
    for path in first_level_dirs:
        unsearched.put(base_path + "/" + path)
    dir_scanner_pool.map_async(dir_scan_worker, range(cpu_count))
    dir_scanner_pool.close()
    # unsearched.join()


#

def fscat_stub(options, name, is_multithread=True):
    global stopped_processes_count
    retry_count = 0
    me_stopped = False
    while not stop_event.is_set():
        try:
            print name + ": running fscat_stub on path " + files_queue.get_nowait()
        except Empty:
            print name + " reaching empty query"
            if retry_count < 3:
                print name + " retrying get file"
                time.sleep(1)
                retry_count += 1
            else:
                if stopped_processes_count < MAX_PROCESSES:
                    if not me_stopped:
                        stopped_processes_count += 1
                    print name + " I'm done, waiting others to complete"
                    me_stopped = True
                elif stopped_processes_count == MAX_PROCESSES:
                    print name + " timed out. Sending stop event"
                    stop_event.set()


def run_recursive_scan(options, results_q):
    process_pool = Pool(MAX_PROCESSES)

    run_crawler(options.path)

    # for dirpath, dirnames, filenames in os.walk(options.path):
    #     for name in filenames:
    #         print "Putting in queue: " + dirpath + "/" + name
    #         queue.put(os.path.join(dirpath, name))

    for i in range(MAX_PROCESSES):
        p = process_pool.apply_async(fscat_stub, (options, ("process-%d" % i)))

    # for p in process_pool:
    #     print "process %s started" % p.name
    #     p.start()
    #
    # for p in process_pool:
    #     p.join()
    process_pool.close()
    process_pool.join()

    while not results_q.empty():
        q = results_q.get()
        if q is True:  # if 'True', there is a problem
            return q


def run_single_folder_scan(options, queue, results_q):
    process_pool = []

    for name in os.listdir(options.path):
        queue.put(os.path.join(options.path, name))

    for i in range(0, MAX_PROCESSES):
        p = Process(target=fscat, name=("process-%d" % i), args=(options, queue, results_q, ("process-%d" % i)))
        process_pool.append(p)

    for p in process_pool:
        print "thread %s started" % p.name
        p.start()

    for p in process_pool:
        p.join()

    while not results_q.empty():
        q = results_q.get()
        if q is True:
            return q


def run_single_file_scan(options, queue, results_q):
    queue.put(options.path)

    p = Process(target=fscat, name="process-1", args=(options, queue, results_q, "process-1", False))

    print "thread %s started" % p.name
    p.start()
    p.join()

    q = results_q.get()
    if q is True:
        return q


def main():
    """ Main function
    Returns:
    0 - all relevant files are deduped
    1 - dedup error
    2 - bad argument passed
    """

    queue = multiprocessing.Manager().Queue()
    results_q = multiprocessing.Queue()

    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_option("-r", "--recursive", action="store_true", dest="recursive")
    parser.add_option("-f", "--fscatoutput", action="store_true", dest="fscatoutput", help="Print fscat -M's output")
    parser.add_option("-s", "--filesize", action="store_true", dest="filesize", help="Always print file size")
    parser.add_option("-p", "--path", help="file/dir path", action="store", dest="path", default=".")
    parser.add_option("-o", "--outputnondeduped", action="store_true", dest="outputnondeduped",
                      help="Indicates files that are over 64KB, without OCA or NULL ranges")
    (options, args) = parser.parse_args()

    if options.path[0] != "/":
        options.path = os.path.abspath(options.path)
        print "Using path: " + options.path
    # if options.path[:9] != "/mnt/mgmt":
    #     print "Path should start with /mnt/mgmt"
    #     sys.exit(2)

    if os.path.isfile(options.path):
        print "scanning single file..."
        if run_single_file_scan(options, queue, results_q) is True:
            sys.exit(1)
    elif os.path.isdir(options.path):
        if options.recursive:
            print "Scanning directory tree..."
            if run_recursive_scan(options, results_q) is True:
                sys.exit(1)
        else:
            print "Scanning folder for files..."
            if run_single_folder_scan(options, queue, results_q) is True:
                sys.exit(1)


if __name__ == '__main__':
    try:
        main()
        print "#### The End ####"
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
