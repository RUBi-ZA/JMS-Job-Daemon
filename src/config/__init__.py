import os

from src.config.email import *
from src.config.impersonator import *
from src.config.scheduler import *
from src.config.storage import *
from src.config.cache import *

SHARED_DIRECTORY = os.path.expanduser(SHARED_DIRECTORY)
TEMP_DIRECTORY = os.path.expanduser(TEMP_DIRECTORY)

config = {
    "storage": {
        "shared_directory": SHARED_DIRECTORY,
        "temp_dir": TEMP_DIRECTORY
    },
    "scheduler": {
        "name": "torque_2_4_x",
        "poll_interval": 30,
        "daemon_credentials": {
            "username": DAEMON_CREDENTIALS["username"],
            "password": DAEMON_CREDENTIALS["password"]
        }
    },
    "impersonator": {
        "host": IMPERSONATOR_HOST,
        "port": IMPERSONATOR_PORT
    },
    "filemanager": {
        "root_url": os.path.join(SHARED_DIRECTORY, "users/"),
        "temp_dir": TEMP_DIRECTORY
    },
    "cache": CACHE
}
