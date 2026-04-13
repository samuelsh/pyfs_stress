import os

REDIS_SERVER = os.environ.get("REDIS_SERVER", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

redis_config = {"host": REDIS_SERVER, "port": REDIS_PORT, "db": 0}
