import os

REDIS_SERVER = os.environ.get("REDIS_SERVER", "10.27.50.31")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

redis_config = {"host": REDIS_SERVER, "port": REDIS_PORT, "db": 0}
