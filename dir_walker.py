import argparse
from multiprocessing.pool import Pool
from multiprocessing import Manager
import os

unsearched = Manager().Queue()


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
        dirpath = unsearched.get_nowait()
        dirs = explore_path(dirpath)
        for newdir in dirs:
            unsearched.put_nowait(newdir)
        unsearched.task_done()


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="file/dir path", action="store", dest="path", default=".", required=True)
args = parser.parse_args()

# acquire the list of paths
first_level_dirs = next(os.walk(args.path))[1]

for path in first_level_dirs:
    unsearched.put(path)

pool = Pool(5)
for i in range(5):
    result = pool.apply_async(parallel_worker)

    print result.get()


print 'Done'
