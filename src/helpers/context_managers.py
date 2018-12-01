class SchedulerTransaction():

    def __init__(self, scheduler, token):
        self.scheduler = scheduler
        self.token = token

    def __enter__(self):
        self.scheduler.impersonator.token = self.token
        self.scheduler._is_transaction = True
        return self.scheduler

    def __exit__(self, *args):
        self.scheduler.impersonator.token = None
        self.scheduler._is_transaction = False