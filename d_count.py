"""
author samuels
"""
import argparse
import traceback

import sys

from logger import Logger
from shell_utils import ShellUtils, FSUtils


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cluster", help="Cluster Name", required=True, type=str)
    parser.add_argument("-v", "--volume", help="Volume Name", required=True, type=str)
    args = parser.parse_args()

    logger = Logger().logger
    logger.debug("Logger Initialised %s" % logger)

    logger.info("Setting passwordless SSH connection")
    ShellUtils.run_shell_script("/zebra/qa/qa-util-scripts/set-ssh-python", args.cluster, False)
    logger.info("Getting cluster params...")
    active_nodes = FSUtils.get_active_nodes_num(args.cluster)
    logger.debug("Active Nodes: %s" % active_nodes)
    domains = FSUtils.get_domains_num(args.cluster)
    logger.debug("FSD domains: %s" % domains)

    for node in range(active_nodes):
        for domain in range(domains):
            logger.info("node{0} - domain {1}".format(node, domain))
            p = ShellUtils.get_shell_remote_command("node{0}.{1}".format(node, args.cluster),
                                                    'fsfind -m {0} {1}'.format(domain, '/mnt/mgmt/' + args.volume))
            outp = ShellUtils.pipe_grep(p, "found")
            print outp


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as stop_test_exception:
        print(" CTRL+C pressed. Stopping test....")
    except Exception:
        traceback.print_exc()
    sys.exit(0)
