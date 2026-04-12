import os

CTRL_MSG_PORT = int(os.environ.get("CTRL_MSG_PORT", "5557"))
CLIENT_MSG_PORT = int(os.environ.get("CLIENT_MSG_PORT", "5558"))
CLIENT_PROXY_FRONTEND = int(os.environ.get("CLIENT_PROXY_FRONTEND", "6000"))
PUBSUB_LOGGER_PORT = int(os.environ.get("PUBSUB_LOGGER_PORT", "5559"))
MAX_FILES_PER_DIR = int(os.environ.get("MAX_FILES_PER_DIR", "10000"))

SET_SSH_PATH = os.environ.get("SET_SSH_PATH", "/zebra/qa/qa-util-scripts/set-ssh-client")
DYNAMO_PATH = os.environ.get("DYNAMO_PATH", "~/qa/dynamo")
DYNAMO_BIN_PATH = os.environ.get("DYNAMO_BIN_PATH", "~/qa/dynamo/client/dynamo_starter.py")
MAX_WORKERS_PER_CLIENT = int(os.environ.get("MAX_WORKERS_PER_CLIENT", "32"))
CLIENT_MOUNT_POINT = os.environ.get("CLIENT_MOUNT_POINT", "/mnt/test_workdir")
FILE_NAMES_PATH = os.environ.get("FILE_NAMES_PATH", "filenames.dat")
