import sys
sys.path.append("..")

from impersonator.client import Impersonator

from src.config import config
from src.schedulers import schedulers
from src.helpers.daemon import Daemon
from src.helpers.data_structures import SchedulerStatus 
from src.helpers.context_managers import SchedulerTransaction

import time, json


PID_FILE = "/tmp/scheduler_daemon.pid"


class SchedulerDaemon(Daemon):
    def __init__(self, pid_file, cache, scheduler, service_token, poll_interval=30):
        super().__init__(pid_file)
        self.cache = cache
        self.scheduler = scheduler
        self.service_token = service_token
        self.poll_interval = poll_interval

    def update_cache(self):
        prev_jobs = self.cache.get("jobs")

        with SchedulerTransaction(self.scheduler, self.service_token):
            nodes = self.scheduler.get_nodes()
            disk_usage = self.scheduler.get_disk_usage()
            status = SchedulerStatus(nodes, disk_usage).to_JSON()
            jobs = self.scheduler.get_jobs().to_JSON()

        self.cache.set("status", status)
        self.cache.set("jobs", jobs)                

    def run(self):
        while True:
            self.update_cache()
            time.sleep(self.poll_interval)


def configure_daemon(config, Impersonator, SchedulerDaemon):
    scheduler_name = config["scheduler"]["name"]
    Scheduler = schedulers[scheduler_name]["scheduler"]

    username = config["scheduler"]["daemon_credentials"]["username"]
    password = config["scheduler"]["daemon_credentials"]["password"]

    impersonator = Impersonator(config["impersonator"]["host"], config["impersonator"]["port"])

    cache = config["cache"]
    scheduler = Scheduler(config, impersonator)
    service_token = impersonator.login(username, password)
    poll_interval = config["scheduler"]["poll_interval"]

    impersonator.token = None

    return SchedulerDaemon(PID_FILE, cache, scheduler, service_token, poll_interval)


if __name__ == "__main__":
    daemon = configure_daemon(config, Impersonator, SchedulerDaemon)

    if sys.argv[1] == "start":
        daemon.start()
    elif sys.argv[1] == "stop":
        daemon.stop()
    elif sys.argv[1] == "restart":
        daemon.restart()
    else:
        print("Invalid argument: %s" % sys.argv[1])
        sys.exit(1)