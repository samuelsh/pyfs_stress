import rpyc
import rpyc.utils

import time


__author__ = 'samuels'

TESTING_PACKAGE_PATH = "/public/samuels/testing"
DATASET_GENERATORS_PACKAGE_PATH = '/public/samuels/testing/datasets'


class Rpc:
    def __init__(self, logger, cluster_node):
        """
        :param logger:Logger
        :param cluster_node:string
        :return:
        """
        self._cluster_node = cluster_node
        self._logger = logger
        self._logger.info("Deploying server %s" % self._cluster_node)
        RpcUtils.deploy_rpyc_server(self._cluster_node)
        self._logger.info("Starting Controller %s" % self._cluster_node)
        RpcUtils.start_rpyc_server(self._cluster_node)
        time.sleep(5)
        self._connection = rpyc.classic.connect(self._cluster_node)
        self._logger.debug("%s" % str(self._connection))

        try:
            ShellUtils.run_shell_remote_command(self._cluster_node, "stat " + TESTING_PACKAGE_PATH)
        except Exception as e:
            logger.error("Can't find %s" % TESTING_PACKAGE_PATH)
            raise e

        self._logger.debug("Loading all relevant modules to server %s" % self._cluster_node)
        self._connection.modules.sys.path.append(TESTING_PACKAGE_PATH)
        self._connection.modules.sys.path.append(DATASET_GENERATORS_PACKAGE_PATH)
        self._connection.root.getmodule('testInfra')
        self._connection.root.getmodule('pyfsutils')
        self._connection.root.getmodule('data_generators')
        self._connection.root.getmodule('file_generators')
        self._connection.root.getmodule('radix_tree_generators')
        self._logger.debug('Done loading modules')

    @property
    def connection(self):
        return self._connection

    def __del__(self):
        self._logger.debug("Closing connection %s" % str(self._connection))
        self._connection.close()
        self._logger.debug("Stopping Controller %s" % self._cluster_node)
        RpcUtils.stop_rpyc_server(self._cluster_node)
