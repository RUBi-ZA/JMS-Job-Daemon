import os

SHARED_DIRECTORY = os.environ.get("SHARED_DIRECTORY", "/tmp/JMS/shared/")
TEMP_DIRECTORY = os.path.join(SHARED_DIRECTORY, "tmp")
