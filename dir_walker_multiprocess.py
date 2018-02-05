import argparse
import multiprocessing
from multiprocessing import Pool, Queue
from multiprocessing import Manager
import os

unsearched = Manager().Queue()
dirpath_queue = Queue()


def explore_path():
    directories = []
    dirpath = dirpath_queue.get()
    for filename in os.walk(dirpath).next()[1]:
        fullname = os.path.join(dirpath, filename)
        directories.append(fullname)
    return directories


def parallel_worker(task_num):
    while True:
        dirpath = unsearched.get()
        print("Task: " + str(task_num) + " >>> Explored path: " + dirpath)
        dirpath_queue.put(dirpath)
        dirs = explore_path()
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()


def run_crawler(base_path):
    if not os.path.isdir(base_path):
        raise IOError("Base path not found: " + base_path)

    cpu_count = multiprocessing.cpu_count()
    pool = Pool(cpu_count)

    # acquire the list of all paths inside base path
    first_level_dirs = next(os.walk(base_path))[1]
    for path in first_level_dirs:
        unsearched.put(base_path + "/" + path)
    pool.map_async(parallel_worker, range(cpu_count))
    pool.close()
    unsearched.join()


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="file/dir path", action="store", dest="path", default=".", required=True)
args = parser.parse_args()

run_crawler(args.path)

print('Done')
