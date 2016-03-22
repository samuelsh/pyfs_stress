import argparse
from multiprocessing.pool import Pool
from multiprocessing import JoinableQueue as Queue
import os
from optparse import OptionParser


def explore_path(path):
    directories = []
    nondirectories = []
    print "Explored path: " + path
    for filename in os.listdir(path):
        fullname = os.path.join(path, filename)
        if os.path.isdir(fullname):
            directories.append(fullname)
    return directories


def parallel_worker():
    while not unsearched.empty():
        path = unsearched.get()
        dirs = explore_path(path)
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="file/dir path", action="store", dest="path", default=".", required=True)
args = parser.parse_args()

# acquire the list of paths
first_level_dirs = next(os.walk(args.path))[1]

unsearched = Queue()
for path in first_level_dirs:
    unsearched.put(path)

pool = Pool(5)
for i in range(5):
    pool.apply_async(parallel_worker)

#unsearched.join()
print 'Done'
