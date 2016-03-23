import argparse
import multiprocessing
from multiprocessing.pool import Pool
from multiprocessing import Manager
import os


class TreeCrawler(object):
    def __init__(self, base_path, callback):
        self.base_path = base_path
        self.unsearched = Manager().Queue()
        self.cpu_count = multiprocessing.cpu_count()
        self.pool = Pool(self.cpu_count)
        self.callback = callback
        # acquire the list of all paths inside base path
        self.first_level_dirs = next(os.walk(self.base_path))[1]

        for path in self.first_level_dirs:
            self.unsearched.put(self.base_path + "/" + path)

    def run_crawler(self):
        self.pool.map_async(parallel_worker, range(self.cpu_count))
        self.pool.close()
        self.unsearched.join()

    @property
    def get_unsearched(self):
        return self.unsearched


def parallel_worker(task_num):
    while True:
        dirpath = unsearched.get()
        dirs = explore_path(task_num, dirpath)
        for newdir in dirs:
            unsearched.put(newdir)
        unsearched.task_done()


def explore_path(task_num, dirpath):
    directories = []
    print "Task: " + str(task_num) + " >>> Explored path: " + dirpath
    for filename in os.listdir(dirpath):
        fullname = os.path.join(dirpath, filename)
        if os.path.isdir(fullname):
            directories.append(fullname)
    return directories


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="file/dir path", action="store", dest="path", default=".", required=True)
args = parser.parse_args()

crawler = TreeCrawler(args.path, explore_path)
unsearched = crawler.unsearched
crawler.run_crawler()

print 'Done'
