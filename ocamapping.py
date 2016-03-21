#!/usr/bin/python
import os
import subprocess
import os.path
import sys
from multiprocessing import Process, JoinableQueue as Queue, Pool
from optparse import OptionParser

# import hanging_threads

MAX_PROCESSES = 16
unsearched = Queue()


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


def explore_path(pid, path):
    directories = []
    #print "process-" + pid + " -- Exploring path " + path
    for filename in os.listdir(path):
        fullname = os.path.join(path, filename)
        if os.path.isdir(fullname):
            directories.append(fullname)
    return directories


def dir_scan_worker(pid):
    while True:
        path = unsearched.get()
        dirs = explore_path(pid, path)
        print pid + " Explored: " + dirs
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()


def fscat_stub(options, queue, results_q, name, is_multithread=True):
    print name + ": running fscat_stub on path"


def run_recursive_scan(options, queue, results_q):
    process_pool = []
    first_level_dirs = next(os.walk(options.path))[1]

    folders_scan_pool = Pool(MAX_PROCESSES)
    for path in first_level_dirs:
        unsearched.put(path)

    for i in range(MAX_PROCESSES):
        folders_scan_pool.apply_async(dir_scan_worker, args=i)

    # for dirpath, dirnames, filenames in os.walk(options.path):
    #     for name in filenames:
    #         print "Putting in queue: " + dirpath + "/" + name
    #         queue.put(os.path.join(dirpath, name))

    for i in range(0, MAX_PROCESSES):
        p = Process(target=fscat_stub, name=("process-%d" % i),
                    args=(options, queue, results_q, ("process-%d" % i)))
        process_pool.append(p)

    for p in process_pool:
        print "process %s started" % p.name
        p.start()

    for p in process_pool:
        p.join()

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

    queue = Queue()
    results_q = Queue()

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
            if run_recursive_scan(options, queue, results_q) is True:
                sys.exit(1)
        else:
            print "Scanning folder for files..."
            if run_single_folder_scan(options, queue, results_q) is True:
                sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print e
