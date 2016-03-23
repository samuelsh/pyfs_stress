import argparse
from multiprocessing.pool import Pool
from multiprocessing import Manager
import os

MAX_POOL_SIZE = 1

unsearched = Manager().Queue()


def explore_path(task_num, dirpath):
    directories = []
    nondirectories = []
    print "Task: " + str(task_num) + " >>> Explored path: " + dirpath
    for filename in os.listdir(dirpath):
        fullname = os.path.join(dirpath, filename)
        if os.path.isdir(fullname):
            directories.append(fullname)
    return directories


def parallel_worker(task_num):
    while True:
        dirpath = unsearched.get()
        dirs = explore_path(task_num, dirpath)
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="file/dir path", action="store", dest="path", default=".", required=True)
args = parser.parse_args()

# acquire the list of paths
first_level_dirs = next(os.walk(args.path))[1]

for path in first_level_dirs:
    unsearched.put(args.path + "/" + path)

pool = Pool(MAX_POOL_SIZE)
pool.map_async(parallel_worker, range(MAX_POOL_SIZE))
pool.close()
unsearched.join()

print 'Done'
