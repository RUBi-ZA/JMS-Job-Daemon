'''
Scheduler plugin for version 2.4.x of Torque Resource Manager (http://www.adaptivecomputing.com/products/torque/)
'''
from .base import BaseScheduler

from ..helpers.data_structures import *
from ..helpers.utils import deepgetattr
from ..helpers.filesystem import create_directory, copy_directory

from lxml import objectify

import os, socket, sys

class Torque(BaseScheduler):
    def get_job(self, id):
        out = self.run_process("qstat -x %s" % id)
        data = objectify.fromstring(out)
        return self._parse_job(data.Job)

    def get_jobs(self):
        '''
        Returns a JobQueue object representing the jobs currently queued or running on the cluster.
        '''
        raise NotImplementedError

    def get_server_config(self):
        '''
        Returns a list of Data objects representing the server config.
        '''
        raise NotImplementedError

    def update_server_config(self, settings):
        '''
        Updates the scheduler configuration.
        Returns a list of Data objects representing the server config.
        '''
        raise NotImplementedError

    def get_queues(self, queue):
        '''
        Returns a list of Data objects representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def add_queue(self, queue):
        '''
        Adds a queue to the scheduler configuration.
        Returns a list of Data objects representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def update_queue(self, queue):
        '''
        Updates a queue's configuration.
        Returns a list of Data objects representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def delete_queue(self, queue):
        '''
        Removes a queue from the scheduler configuration.
        Returns a list of Data objects representing the configuration of the scheduler queues.
        '''
        raise NotImplementedError

    def get_administrators(self):
        '''
        Return list of Data objects representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def add_administrator(self, Administrators):
        '''
        Adds an administrator user to the scheduler configuration.
        Return list of Data objects representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def update_administrator(self, Administrators):
        '''
        Update an administrator user.
        Return list of Data objects representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def delete_administrator(self, Administrators):
        '''
        Delete an administrator from the scheduler configuration
        Return list of Data objects representing the administrator users of the scheduler.
        '''
        raise NotImplementedError

    def get_nodes(self):
        nodes = []
        
        try:
            out = self.run_process("qnodes -x")
            data = objectify.fromstring(out)
            
            for node in data.Node:
                name = str(node.name)
                state = str(node.state)
                num_cores = int(node.np)
                properties = str(deepgetattr(node, 'properties', ''))
                busy_cores = 0         
                
                job_dict = {}
                
                jobs = deepgetattr(node, 'jobs', None)
                if jobs:
                    for job in str(jobs).split(','):
                        job_cores = job.split("/")
                        busy_cores += 1
                            
                        if job_cores[1] in job_dict:
                            job_dict[job] += 1
                        else:
                            job_dict[job] = 1
                
                free_cores = num_cores - busy_cores
                
                node = Node(name, state, num_cores, busy_cores, free_cores, properties)
                
                for job_id in job_dict:
                    job = Job(job_id, job_dict[job_id])                
                    node.jobs.append(job)
                
                nodes.append(node)
        
        except Exception as ex:
            sys.stderr.write(str(ex))
        
        return nodes

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
        self.run_process('qmgr -c "delete node %s"' % id)
        return self.get_nodes()

    def stop(self):
        return self.run_process("qterm -t quick")    
    
    def start(self):
        return self.run_process("qserverd")

    def restart(self):
        output = self.stop()
        output += self.start()
        return output

    def get_default_resources(self):
        return DataSection("torque", [
            DataField(
                Key = "nodes",
                Label = "Nodes",
                ValueType = ValueType.Number,
                DefaultValue = 1,
                Disabled = False
            ), DataField(
                Key = "ppn",
                Label = "Cores",
                ValueType = ValueType.Number,
                DefaultValue = 1,
                Disabled = False
            ), DataField(
                Key = "mem",
                Label = "Memory (GB)",
                ValueType = ValueType.Number,
                DefaultValue = 1,
                Disabled = False
            ), DataField(
                Key = "walltime",
                Label = "Walltime (h:m:s)",
                ValueType = ValueType.Text,
                DefaultValue = "01:00:00",
                Disabled = False
            ), DataField(
                Key = "queue",
                Label = "Queue",
                ValueType = ValueType.Text,
                DefaultValue = "batch",
                Disabled = False
            ), DataField(
                Key = "variables",
                Label = "Environmental Variables",
                ValueType = ValueType.Text,
                DefaultValue = "",
                Disabled = False
            )
        ])

    def create_job_script(self, job_name, job_dir, script_name, output_log, error_log, settings, has_dependencies, commands):
        script = os.path.join(job_dir, script_name)
        
        with open(script, 'w') as job_script:
            print("#!/bin/sh", file=job_script)
            print("#PBS -o localhost:%s" % output_log, file=job_script)
            print("#PBS -e localhost:%s" % error_log, file=job_script)
            print("#PBS -d %s" % job_dir, file=job_script)
            print("#PBS -N %s" % job_name, file=job_script)
            
            nodes = ""
            for setting in settings:
                if setting.Name == "nodes":
                    nodes += "#PBS -l nodes=%s" % setting.Value
                elif setting.Name == "ppn":
                    nodes += ":ppn=%s" % setting.Value
                elif setting.Name == "mem":
                    print("#PBS -l mem=%sgb" % setting.Value, file=job_script)
                elif setting.Name == "walltime":
                    print("#PBS -l walltime=%s" % setting.Value, file=job_script)
                elif setting.Name == "queue":
                    print("#PBS -q %s" % setting.Value, file=job_script)
                elif setting.Name == "variables":
                    if setting.Value.strip() != "":
                        print("#PBS -v %s" % setting.Value, file=job_script)
                    else:
                        print("#PBS -V", file=job_script)
            print(nodes, file=job_script)
            
            if has_dependencies:
                print("#PBS -h", file=job_script)
            
            print("", file=job_script)
            print(commands, file=job_script)
        
        return script

    def execute_job_script(self, path):
        self.run_process("qsub %s" % path)
        return self.get_job(id)

    def hold_job(self, id):
        self.run_process("qhold %s" % id)
        return self.get_job(id)

    def release_job(self, id):
        self.run_process("qrls %s" % id)
        return self.get_job(id)

    def kill_job(self, id):
        self.run_process("qdel %s" % id)
        return self.get_job(id)

    def alter_job(self, Key, Value):
        '''
        Alter the configuration of a scheduled job
        Return a ClusterJob object representing the job details 
        '''
        raise NotImplementedError

    def _parse_job(self, job):
        exit_code = str(deepgetattr(job, 'exit_status', None))
        state = str(deepgetattr(job, 'job_state', Status.Held))
        if state == 'H':
            state = Status.Held
        elif state == 'Q':
            state = Status.Queued
        elif state == 'R':
            state = Status.Running
        elif state in ['E', 'C']:
            state = Status.Complete

        output_path = str(deepgetattr(job, 'Output_Path', 'n/a'))
        error_path = str(deepgetattr(job, 'Error_Path', 'n/a'))
        
        if len(output_path.split(":")) == 2:
            output_path = output_path.split(":")[1]
        if len(error_path.split(":")) == 2:
            error_path = error_path.split(":")[1]
        
        env = str(deepgetattr(job, 'Variable_List', 'n/a'))
        vars = env.split(',')
        
        working_dir = "~"
        for v in vars:
            kv = v.split("=")
            if kv[0] == "PBS_O_WORKDIR":
                working_dir = kv[1]
                break
        
        job_id = str(deepgetattr(job, 'Job_Id', 'Unknown'))
        name = str(deepgetattr(job, 'Job_Name', 'Unknown'))
        user = str(deepgetattr(job, 'Job_Owner', 'Unknown'))
        
        cluster_job = ClusterJob(job_id, name, user, state, output_path, error_path, working_dir, exit_code, [])
        
        resources_allocated = DataSection("Allocated Resources", [
            DataField(Key='mem', Label="Allocated Memory", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'Resource_List.mem'))
            ),
            DataField(Key='nodes', Label="Allocated Nodes", ValueType=4, 
                DefaultValue=str(deepgetattr(job, "Resource_List.nodes"))
            ),
            DataField(Key='walltime', Label="Allocated Walltime", 
                ValueType=4, DefaultValue=str(deepgetattr(job, "Resource_List.walltime"))
            ),
            DataField(Key='queue', Label="Queue", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'queue'))
            ),
        ])
        
        resources_used = DataSection("Resources Used", [
            DataField(Key='cput', Label="CPU Time", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'resources_used.cput', 'n/a'))
            ),
            DataField(Key='mem_used', Label="Memory Used", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'resources_used.mem', 'n/a'))
            ),
            DataField(Key='vmem', Label="Virtual Memory Used", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'resources_used.vmem', 'n/a'))
            ),
            DataField(Key='walltime_used', Label="Walltime Used", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'resources_used.walltime', 'n/a'))
            ),
            DataField(Key='exec_host', Label="Execution Node", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'exec_host', 'n/a')).split("/")[0]
            )
        ])
        
        other = DataSection("Other", [
            DataField(Key='server', Label="Server", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'server', 'n/a'))
            ),
            DataField(Key='submit_args', Label="Submit Args", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'submit_args', 'n/a'))
            ),
            DataField(Key='Output_Path', Label="Output Log", ValueType=4, 
                DefaultValue=output_path
            ),
            DataField(Key='Error_Path', Label="Error Log", ValueType=4, 
                DefaultValue=error_path
            ),
            DataField(Key='Priority', Label="Priority", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'Priority', 'n/a'))
            ),
            DataField(Key='Variable_List', Label="Environmental Variables", 
                ValueType=4, DefaultValue=env
            ),
            DataField(Key='comment', Label="Comment", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'comment', 'n/a'))
            )
        ])
        
        time = DataSection("Time", [
            DataField(Key='ctime', Label="Created Time", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'ctime', 'n/a'))
            ),
            DataField(Key='qtime', Label="Time Entered Queue", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'qtime', 'n/a'))
            ),
            DataField(Key='etime', Label="Time Eligible to Run", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'etime', 'n/a'))
            ),
            DataField(Key='mtime', Label="Last Modified", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'mtime', 'n/a'))
            ),
            DataField(Key='start_time', Label="Start Time", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'start_time', 'n/a'))
            ),
            DataField(Key='comp_time', Label="Completed Time", ValueType=4, 
                DefaultValue=str(deepgetattr(job, 'comp_time', 'n/a'))
            ),
        ])
        
        cluster_job.DataSections.append(resources_allocated)
        cluster_job.DataSections.append(resources_used)
        cluster_job.DataSections.append(time)
        cluster_job.DataSections.append(other)
        
        return cluster_job
    