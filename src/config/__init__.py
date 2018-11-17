import os

from .email import *
from .impersonator import *
from .scheduler import *
from .storage import *

SHARED_DIRECTORY = os.path.expanduser(SHARED_DIRECTORY)
TEMP_DIRECTORY = os.path.expanduser(TEMP_DIRECTORY)

config = {
    "storage": {
        "shared_directory": SHARED_DIRECTORY,
        "temp_dir": TEMP_DIRECTORY
    },
    "scheduler": {
        "name": "torque",
        "poll_interval": 30
    },
    "impersonator": {
        "host": IMPERSONATOR_HOST,
        "port": IMPERSONATOR_PORT
    },
    "filemanager": {
        "root_url": os.path.join(SHARED_DIRECTORY, "users/"),
        "temp_dir": TEMP_DIRECTORY
    }
}
