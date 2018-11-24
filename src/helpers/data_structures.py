class ValueType:  
    TEXT = 10
    NUMBER = 20
    CHECKBOX = 30
    LABEL = 40
    OPTIONS = 50


class Status:   
    HELD = 10
    QUEUED = 20
    RUNNING = 30
    COMPLETE = 40


class ClusterJob:  
    def __init__(self, job_id, name, user, status, output, error, working_directory, exit_code=None, data_sections=[]):        
        self.job_id = job_id
        self.job_name = name
        self.user = user
        self.status = status
        self.output = output
        self.error = error
        self.working_directory = working_directory   
        self.exit_code = exit_code 
        self.data_sections = data_sections


class Data:  
    def __init__(self, data_sections, data):
        self.data_sections = data_sections
        self.data = data


class DataSection:
    def __init__(self, section_header, data_fields):
        self.section_header = section_header
        self.data_fields = data_fields


class DataField: 
    def __init__(self, key, label, value_type, default_value, disabled=False):
        self.key = key
        self.label = label
        self.value_type = value_type
        self.default_value = default_value
        self.disabled = disabled


class SchedulerStatus:
    def __init__(self, nodes, disk):
        self.nodes = nodes      
        self.disk = disk

   
class Node:  
    def __init__(self, name, state, num_cores, busy_cores, free_cores, other):
        self.name = name
        self.state = state
        self.num_cores = num_cores
        self.busy_cores = busy_cores
        self.free_cores = free_cores
        self.other = other
        self.jobs = []


class Job:   
    def __init__(self, job_id, cores):
        self.job_id = job_id
        self.cores = cores


class JobQueue:   
    def __init__(self, column_names, rows):
        self.column_names = column_names # A list of column names (strings)
        self.rows = rows # a list of QueueRows

class QueueRow:   
    def __init__(self, job_id, state, values):
        self.job_id = job_id
        self.state = state
        self.values = values


class QueueItem: 
    def __init__(self, job_id, username, job_name, nodes, cores, state, time, queue):
        self.job_id = job_id
        self.username = username
        self.job_name = job_name
        self.nodes = nodes
        self.cores = cores
        self.state = state
        self.time = time
        self.queue = queue


class DiskUsage:
    def __init__(self, disk_size, available_space, used_space):
        self.disk_size = disk_size
        self.available_space = available_space
        self.used_space = used_space


class SettingsSection: 
    def __init__(self, section_header, settings):
        self.section_header = section_header
        self.settings = settings
   

class Setting: 
    def __init__(self, name, value):
        self.name = name
        self.value = value


class Administrator:
    def __init__(self, administrator_name=None, settings_sections=[]):
        self.administrator_name = administrator_name
        self.settings_sections = settings_sections

    
class Queue:
    def __init__(self, queue_name=None, settings_sections=[]):
        self.queue_name = queue_name
        self.settings_sections = settings_sections
