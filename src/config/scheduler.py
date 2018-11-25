import os

SCHEDULER = "torque_2_4_x"
SCHEDULER_POLL_INTERVAL = 30

DAEMON_CREDENTIALS = {
    "username": os.environ.get("JMS_DAEMON_USERNAME"),
    "password": os.environ.get("JMS_DAEMON_PASSWORD")
}
