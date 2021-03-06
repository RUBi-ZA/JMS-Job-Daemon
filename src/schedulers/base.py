'''
The base class that all implemented resource managers should inherit from.

This class provides the constructor and a small number of methods that can
be used in child classes.

All unimplemented methods must be overridden in child classes. Comments
within each method provide the required return values.
'''

from src.helpers.data_structures import SchedulerStatus, DiskUsage
from src.helpers.decorators import clear_credentials, decorate_callables

from impersonator.client import Impersonator


@decorate_callables(clear_credentials)
class BaseScheduler(object):
    def __init__(self, config, impersonator):
        self.config = config
        self.impersonator = impersonator
        self._is_transaction = False
    
    def set_credentials(self, credentials):
        self.impersonator.token = credentials
    
    def clear_credentials(self):
        self.impersonator.token = None

    def run_process(self, cmd):
        return self.impersonator.execute(cmd)

    def get_disk_usage(self):
        path = self.config["storage"]["shared_directory"]
        output = self.run_process("df -h %s" % path)['out']

        lines = output.split('\n')

        index = lines[0].index("Size")
        size = lines[1][index:index+5].strip()
        used = lines[1][index+5:index+11].strip()
        available = lines[1][index+11:index+17].strip()

        return DiskUsage(size, available, used)

    def get_jobs(self):
        '''
        Returns a JobMap object representing the jobs currently queued or running on the cluster.
        '''
        raise NotImplementedError
    
    def transform_job_list_to_queue(self, jobs, page_num=1, page_size=50):
        '''
        Accepts a list of ClusterJob objects and transforms them into a paginated JobTable
        '''
        raise NotImplementedError

    def get_server_config(self):
        '''
        Returns a Data object representing the server config.
        '''
        raise NotImplementedError

    def update_server_config(self, settings):
        '''
        Updates the scheduler configuration.
        Returns a Data object representing the server config.
        '''
        raise NotImplementedError

    def get_queues(self):
        '''
        Returns a Data object representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def add_queue(self, queue_name):
        '''
        Adds a queue to the scheduler configuration.
        Returns a Data object representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def update_queue(self, queue):
        '''
        Updates a queue's configuration.
        Returns a Data object representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def delete_queue(self, queue):
        '''
        Removes a queue from the scheduler configuration.
        Returns a Data object representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def get_administrators(self):
        '''
        Return a Data object representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def add_administrator(self, Administrators):
        '''
        Adds an administrator user to the scheduler configuration.
        Return a Data object representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def update_administrator(self, Administrators):
        '''
        Update an administrator user.
        Return a Data object representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def delete_administrator(self, Administrators):
        '''
        Delete an administrator from the scheduler configuration
        Return a Data object representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def get_nodes(self):
        '''
        Return list of Node objects representing the nodes in the cluster.
        '''
        raise NotImplementedError

    def add_node(self, node):
        '''
        Add a node to the scheduler configuration
        Return list of Node objects representing the nodes in the cluster.
        '''
        raise NotImplementedError

    def update_node(self, node):
        '''
        Update a node in the scheduler configuration
        Return list of Node objects representing the nodes in the cluster.
        '''
        raise NotImplementedError

    def delete_node(self, id):
        '''
        Delete a node from the scheduler configuration
        Return list of Node objects representing the nodes in the cluster.
        '''
        raise NotImplementedError

    def stop(self):
        '''
        Stop the scheduler process
        '''
        raise NotImplementedError

    def start(self):
        '''
        Start the scheduler process
        '''
        raise NotImplementedError

    def restart(self):
        '''
        Restart the scheduler process
        '''
        raise NotImplementedError

    def get_default_resources(self):
        '''
        Return DataSection object representing the default cluster resources that will be provided to a job
        '''
        raise NotImplementedError

    def create_job_script(self, **kwargs):
        '''
        Set up a valid job script that can be executed by the given scheduler
        Return path to job script
        '''
        raise NotImplementedError

    def execute_job_script(self, path):
        '''
        Submit a job to the scheduler to be executed
        Return a ClusterJob object representing the job details 
        '''
        raise NotImplementedError

    def hold_job(self, id):
        '''
        Prevent a job that is currently in the queue from being scheduled for execution
        Return a ClusterJob object representing the job details 
        '''
        raise NotImplementedError

    def release_job(self, id):
        '''
        Release a job from being held and allow it to be scheduled
        Return a ClusterJob object representing the job details 
        '''
        raise NotImplementedError

    def kill_job(self, id):
        '''
        Kill a scheduled or running job
        Return a ClusterJob object representing the job details 
        '''
        raise NotImplementedError

    def alter_job(self, Key, Value):
        '''
        Alter the configuration of a scheduled job
        Return a ClusterJob object representing the job details 
        '''
        raise NotImplementedError
