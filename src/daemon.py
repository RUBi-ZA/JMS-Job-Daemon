import sys
sys.path.append("..")

from impersonator.client import Impersonator

from src.config import config
from src.schedulers import schedulers
from src.helpers.daemon import Daemon 

import redis, time

Scheduler = schedulers[config["scheduler"]["name"]]["scheduler"]

cache = redis.StrictRedis(host="localhost", port=6379, db=0)


class SchedulerDaemon(Daemon):    
    def update_cache(self, scheduler):
        status = scheduler.get_status().to_JSON()
        jobs = scheduler.get_jobs().to_JSON()

        cache.set("status", status)
        cache.set("jobs", jobs)

    def run(self):        
        username = config["scheduler"]["daemon_credentials"]["username"]
        password = config["scheduler"]["daemon_credentials"]["password"]

        impersonator = Impersonator()
        impersonator.login(username, password)
        
        scheduler = Scheduler(config, impersonator.token)
        poll_interval = config["scheduler"]["poll_interval"]

        while True:
            self.update_cache(scheduler)
            time.sleep(poll_interval)


print("Starting daemon...")
daemon = SchedulerDaemon("/tmp/scheduler_daemon.pid")
daemon.start()
