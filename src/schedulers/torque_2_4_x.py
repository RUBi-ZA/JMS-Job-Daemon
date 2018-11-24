'''
Scheduler plugin for version 2.4.x of Torque Resource Manager (http://www.adaptivecomputing.com/products/torque/)
'''
from __future__ import print_function

from .base import BaseScheduler

from ..helpers.data_structures import *
from ..helpers.utils import deepgetattr
from ..helpers.filesystem import create_directory, copy_directory

from lxml import objectify

import os, socket, sys

class Torque(BaseScheduler):
    def get_job(self, id):
        out = self.run_process("qstat -x %s" % id)["out"]
        data = objectify.fromstring(out)
        return self._parse_job(data.Job)

    def get_jobs(self):
        column_names = ["Job ID", "Username", "Queue", "Job Name", "State", "Nodes", "Cores", "Time Requested", "Time Used"]
        rows = []
        
        queue = JobQueue(column_names, rows)
        
        try:
            out = self.run_process("qstat -x")['out']
            data = objectify.fromstring(out)
        except Exception as e:
            return queue
            
        for job in data.Job:
            cores = 1
            nodes = str(job.Resource_List.nodes).split(":")
            if len(nodes) > 1:
                cores = int(nodes[1].split("=")[1])
            nodes = nodes[0]
            
            try:
                time_used = str(job.resources_used.walltime)
            except:
                time_used = "n/a"
            
            state = str(job.job_state)
            if state == "H":
                state = Status.HELD
            elif state == "Q":
                state = Status.QUEUED
            elif state == "R" or state == "E":
                state = Status.RUNNING
            elif state == "C":
                state = Status.COMPLETE
            
            row = [
                str(job.Job_Owner).split("@")[0],
                str(job.queue),
                str(job.Job_Name),
                str(job.job_state),
                nodes,
                cores,
                str(job.Resource_List.walltime),
                time_used
            ]
            
            queue.rows.append(QueueRow(str(job.Job_Id), state, row))         
            
        return queue

    def get_server_config(self):
        output = self.run_process('qmgr -c "print server"')['out']
        
        data_sections = [
            DataSection(
                section_header = "Torque Settings",
                data_fields = [
                    DataField("acl_hosts", "Server name", ValueType.LABEL, "", True),
                    DataField("default_queue", "Default queue", ValueType.TEXT, ""),
                    DataField("scheduler_iteration", "Scheduler iteration (ms)", ValueType.NUMBER, False),
                    DataField("node_check_rate", "Node check rate (ms)", ValueType.NUMBER, False),
                    DataField("tcp_timeout", "TCP timeout (ms)", ValueType.NUMBER, False),
                    DataField("job_stat_rate", "Job stat rate (ms)", ValueType.NUMBER, False),
                    DataField("keep_completed", "Keep completed time (s)", ValueType.NUMBER, False),
                    DataField("scheduling", "Scheduling?", ValueType.CHECKBOX, False),
                    DataField("mom_job_sync", "Sync server with MOM jobs?", ValueType.CHECKBOX, False),
                    DataField("query_other_jobs", "View other users' jobs in queue?", ValueType.CHECKBOX, False),
                    DataField("moab_array_compatible", "Moab array compatible?", ValueType.CHECKBOX, False),
                ]
            )
        ]
        
        settings_section = SettingsSection("Torque Settings", [])
        
        for line in output.split('\n'):
            if line.startswith("set server"):
                setting_line = line[11:].split("=")
                
                setting_name = setting_line[0].strip()
                value = setting_line[1].strip()
                
                if setting_name in ["scheduling", "query_other_jobs", "mom_job_sync", "moab_array_compatible"]:
                    value = value.lower() == "true"
                
                if setting_name in ["scheduling", "query_other_jobs", "mom_job_sync", "moab_array_compatible", 
                    "acl_hosts", "default_queue", "scheduler_iteration", "node_check_rate", "tcp_timeout",
                    "job_stat_rate", "keep_completed"]:
                    setting = Setting(name=setting_name, value=value)
                    settings_section.settings.append(setting)
        
        return Data(data_sections, [settings_section])

    def update_server_config(self, settings_sections):
        for section in settings_sections:
            for setting in section.settings:
                self.run_process('qmgr -c "set server %s = %s"' % (setting.name, str(setting.value)))
        return self.get_server_config()

    def get_queues(self):
        output = self.run_process('qmgr -c "print server"')['out']
        
        queue_dict = {}
        acl = {}
        
        data_sections = [
            DataSection(
                section_header = "General",
                data_fields = [
                    DataField("queue_type", "Queue type", ValueType.TEXT, "Execution"),
                    DataField("max_queuable", "Max jobs queuable at a time", ValueType.NUMBER, 10),
                    DataField("max_running", "Max jobs running at a time", ValueType.NUMBER, 5),
                    DataField("enabled", "Enabled?", ValueType.CHECKBOX, False),
                    DataField("started", "Started?", ValueType.CHECKBOX, False),
                ]
            ), DataSection(
                section_header = "User Settings",
                data_fields = [
                    DataField("max_user_queuable", "Max jobs queuable per user", ValueType.NUMBER, 5),
                    DataField("max_user_run", "Max jobs running per user", ValueType.NUMBER, 1),
                ]
            ), DataSection(
                section_header = "Resources",
                data_fields = [
                    DataField("resources_max.mem", "Max memory", ValueType.TEXT, "1gb"),
                    DataField("resources_max.ncpus", "Max cores", ValueType.NUMBER, 1),
                    DataField("resources_max.nodes", "Max nodes", ValueType.NUMBER, 1),
                    DataField("resources_max.walltime", "Max walltime", ValueType.TEXT, "01:00:00"),
                    DataField("resources_default.mem", "Default memory", ValueType.TEXT, "1gb"),
                    DataField("resources_default.ncpus", "Default cores", ValueType.NUMBER, 1),
                    DataField("resources_default.nodes", "Default nodes", ValueType.NUMBER, 1),
                    DataField("resources_default.walltime", "Default walltime", ValueType.TEXT, "01:00:00"),
                ]
            ), DataSection(
                section_header = "Access Control",
                data_fields = [
                    DataField("acl_group_enable", "Enable group-based access control?", ValueType.CHECKBOX, False),
                    DataField("acl_user_enable", "Enable user-based access control?", ValueType.CHECKBOX, False),
                    DataField("acl_groups", "Groups with access", ValueType.TEXT, ""),
                    DataField("acl_users", "Users with access", ValueType.TEXT, "")
                ]
            )
        ]
        
        for line in output.split('\n'):   
            if line.startswith("create queue"):
                queue_name = line[13:].strip()
                queue = Queue(queue_name=queue_name, settings_sections=[
                    SettingsSection("General", []), 
                    SettingsSection("User Settings", []), 
                    SettingsSection("Resources", []), 
                    SettingsSection("Access Control", [])
                ])
                
                queue_dict[queue_name] = queue
                acl[queue_name] = {
                    "groups": "",
                    "users": ""
                }                
            elif line.startswith("set queue"):
                setting_line = line[10:].split("=")
                
                queue_name = setting_line[0].split(" ")[0]
                queue = queue_dict[queue_name]
                
                setting = setting_line[0].split(" ")[1].strip()
                value = "=".join(setting_line[1:]).strip()
                
                if setting in ["enabled", "started", "acl_group_enable", "acl_user_enable"]:
                    value = value.lower() == "true"
                
                if setting in ["acl_groups", "acl_groups +"]:
                    acl[queue_name]["groups"] += value + ","                
                elif setting in ["acl_users", "acl_users +"]:
                    acl[queue_name]["users"] += value + ","                
                else:
                    setting = Setting(name=setting, value=value)
                
                    if setting in ["queue_type", "max_queuable","max_running", "enabled", "started"]:
                        queue.settings_sections[0].settings.append(setting)                    
                    elif setting in ["max_user_queuable", "max_user_run"]:
                        queue.settings_sections[1].settings.append(setting)                        
                    elif setting in ["resources_max.mem", "resources_max.walltime", 
                        "resources_default.mem", "resources_default.walltime"]:
                        queue.settings_sections[2].settings.append(setting)                        
                    elif setting == "resources_max.nodes":
                        new_values = value.split(":")                        
                        if len(new_values) > 1:
                            setting.value = new_values[0]
                            queue.settings_sections[2].settings.append(setting)
                            queue.settings_sections[2].settings.append(Setting(
                                name="resources_max.ncpus", 
                                value=new_values[1].split("=")[1]
                            ))
                        else:
                            queue.settings_sections[2].settings.append(setting)
                            queue.settings_sections[2].settings.append(Setting(
                                name="resources_max.ncpus", 
                                value=1
                            ))                        
                    elif setting == "resources_default.nodes":
                        new_values = value.split(":ppn=")
                        if len(new_values) > 1:
                            setting.value = new_values[0]
                            queue.settings_sections[2].settings.append(setting)
                            queue.settings_sections[2].settings.append(Setting(
                                name="resources_default.ncpus", 
                                value=new_values[1]
                            ))
                        else:
                            queue.settings_sections[2].settings.append(setting)
                            queue.settings_sections[2].settings.append(Setting(
                                name="resources_default.ncpus", 
                                value=1
                            ))                    
                    elif setting in ["acl_group_enable", "acl_user_enable"]:
                        queue.settings_sections[3].settings.append(setting)
        
        queues = Data(data_sections, [])
        for queue_name in queue_dict:
            queue = queue_dict[queue_name]
            
            group_access = Setting(name="acl_groups", value=acl[queue_name]["groups"].strip(","))
            queue.SettingsSections[3].Settings.append(group_access)
            
            user_access = Setting(name="acl_users", value=acl[queue_name]["users"].strip(","))
            queue.SettingsSections[3].Settings.append(user_access)
            
            queues.data.append(queue)
            
        return queues

    def add_queue(self, queue_name):
        self.run_process('qmgr -c "create queue %s"' % queue_name)
        return self.get_queues()

    def update_queue(self, queue):
        max_nodes = 1
        max_procs = 1
        def_nodes = 1
        def_procs = 1
        
        for section in queue.settings_sections:
            for setting in section.settings:
                #set access controls - values come as csv
                if setting.name in ["acl_groups", "acl_users"]:
                    values = setting.value.split(",")
                    if len(values) == 1 and values[0] == "":
                        self.run_process('qmgr -c "set queue %s %s = temp"' % (queue.queue_name, setting.name))
                        self.run_process('qmgr -c "set queue %s %s -= temp"' % (queue.queue_name, setting.name))
                    else:
                        for index, value in enumerate(values):
                            value = value.strip()
                            if index == 0:
                                self.run_process('qmgr -c "set queue %s %s = %s"' % (queue.queue_name, setting.name, value))
                            else:
                                self.run_process('qmgr -c "set queue %s %s += %s"' % (queue.queue_name, setting.name, value))
                elif setting.name == "resources_max.nodes":
                    max_nodes = setting.value
                elif setting.name == "resources_max.ncpus":
                    max_procs = setting.value
                elif setting.name == "resources_default.nodes":
                    def_nodes = setting.value
                elif setting.name == "resources_default.ncpus":
                    def_procs = setting.value
                
                self.run_process('qmgr -c "set queue %s %s = %s"' % (queue.queue_name, setting.name, str(setting.value)))
        
        self.run_process('qmgr -c "set queue %s resources_max.nodes = %s:ppn=%s"' % (queue.queue_name, str(max_nodes), str(max_procs)))
        self.run_process('qmgr -c "set queue %s resources_default.nodes = %s:ppn=%s"' % (queue.queue_name, def_nodes, def_procs))
        
        return self.get_queues()

    def delete_queue(self, queue_name):
        self.run_process('qmgr -c "delete queue %s"' % queue_name)
        return self.get_queues()

    def get_administrators(self):
        output = self.run_process('qmgr -c "print server"')['out']
        
        data_sections = [
            DataSection(
                section_header = "Privileges",
                data_fields = [
                    DataField("managers", "Manager?", ValueType.CHECKBOX, False),
                    DataField("operators", "Operator?", ValueType.CHECKBOX, False)
                ]
            )
        ]
        
        admins = {}
        
        for line in output.split('\n'):
            if line.startswith("set server"):
                setting_line = line[11:].split("=")
                
                setting = setting_line[0].strip().strip(" +")
                value = setting_line[1].strip()                
                
                if setting == "managers" or setting == "managers +":
                    setting = Setting(name=setting, value=True)
                    if value in admins:
                        admins[value].settings_sections[0].settings.append(setting)
                    else:
                        section = SettingsSection(section_header="Privileges", settings=[setting])
                        admins[value] = Administrator(administrator_name=value, settings_sections=[section])

                elif setting == "operators" or setting == "operators +":
                    setting = Setting(name=setting, value=True)
                    if value in admins:
                        admins[value].settings_sections[0].settings.append(setting)
                    else:
                        section = SettingsSection(section_header="Privileges", settings=[setting])
                        admins[value] = Administrator(administrator_name=value, settings_sections=[section])
        
        administrators = Data(data_sections, [])
        for k in admins:
            administrators.data.append(admins[k])
        
        return administrators

    def add_administrator(self, administrator_name):
        self.run_process('qmgr -c "set server managers += %s"' % (administrator_name))
        self.run_process('qmgr -c "set server operators += %s"' % (administrator_name))
        return self.get_administrators()

    def update_administrator(self, administrator):
        for section in administrator.settings_sections:
            for setting in section.settings:
                if setting.value:
                    self.run_process('qmgr -c "set server %s += %s"' % (setting.name, administrator.administrator_name))
                else:
                    self.run_process('qmgr -c "set server %s -= %s"' % (setting.name, administrator.administrator_name))
        return self.get_administrators()

    def delete_administrator(self, administrator_name):
        self.run_process('qmgr -c "set server managers -= %s"' % administrator_name)
        self.run_process('qmgr -c "set server operators -= %s"' % administrator_name)
        return self.get_administrators()

    def get_nodes(self):
        nodes = []
        
        try:
            out = self.run_process("qnodes -x")["out"]
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
        self.run_process('qmgr -c "create node %s"' % node.name)   
        return self.update_node(node)

    def update_node(self, node):
        self.run_process('qmgr -c "set node %s np = %s"' % (node.name, str(node.num_cores)))
        self.run_process('qmgr -c "set node %s properties = %s"' % (node.name, node.other))
        return self.get_nodes()

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
                key = "nodes",
                label = "Nodes",
                value_type = ValueType.NUMBER,
                default_value = 1,
                disabled = False
            ), DataField(
                key = "ppn",
                label = "Cores",
                value_type = ValueType.NUMBER,
                default_value = 1,
                disabled = False
            ), DataField(
                key = "mem",
                label = "Memory (GB)",
                value_type = ValueType.NUMBER,
                default_value = 1,
                disabled = False
            ), DataField(
                key = "walltime",
                label = "Walltime (h:m:s)",
                value_type = ValueType.TEXT,
                default_value = "01:00:00",
                disabled = False
            ), DataField(
                key = "queue",
                label = "Queue",
                value_type = ValueType.TEXT,
                default_value = "batch",
                disabled = False
            ), DataField(
                key = "variables",
                label = "Environmental Variables",
                value_type = ValueType.TEXT,
                default_value = "",
                disabled = False
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
                if setting.name == "nodes":
                    nodes += "#PBS -l nodes=%s" % setting.value
                elif setting.name == "ppn":
                    nodes += ":ppn=%s" % setting.value
                elif setting.name == "mem":
                    print("#PBS -l mem=%sgb" % setting.value, file=job_script)
                elif setting.name == "walltime":
                    print("#PBS -l walltime=%s" % setting.value, file=job_script)
                elif setting.name == "queue":
                    print("#PBS -q %s" % setting.value, file=job_script)
                elif setting.name == "variables":
                    if setting.value.strip() != "":
                        print("#PBS -v %s" % setting.value, file=job_script)
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
        state = str(deepgetattr(job, 'job_state', Status.HELD))
        if state == 'H':
            state = Status.HELD
        elif state == 'Q':
            state = Status.QUEUED
        elif state == 'R':
            state = Status.RUNNING
        elif state in ['E', 'C']:
            state = Status.COMPLETE

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
            DataField(
                key='mem', 
                label="Allocated Memory", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'Resource_List.mem'))
            ), DataField(
                key='nodes', 
                label="Allocated Nodes", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, "Resource_List.nodes"))
            ), DataField(
                key='walltime', 
                label="Allocated Walltime", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, "Resource_List.walltime"))
            ), DataField(
                key='queue', 
                label="Queue", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'queue'))
            ),
        ])
        
        resources_used = DataSection("Resources Used", [
            DataField(
                key='cput', 
                label="CPU Time", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'resources_used.cput', 'n/a'))
            ), DataField(
                key='mem_used', 
                label="Memory Used", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'resources_used.mem', 'n/a'))
            ), DataField(
                key='vmem', 
                label="Virtual Memory Used", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'resources_used.vmem', 'n/a'))
            ), DataField(
                key='walltime_used', 
                label="Walltime Used", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'resources_used.walltime', 'n/a'))
            ), DataField(
                key='exec_host', 
                label="Execution Node", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'exec_host', 'n/a')).split("/")[0]
            )
        ])
        
        other = DataSection("Other", [
            DataField(
                key='server', 
                label="Server", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'server', 'n/a'))
            ), DataField(
                key='submit_args', 
                label="Submit Args", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'submit_args', 'n/a'))
            ), DataField(
                key='Output_Path', 
                label="Output Log", 
                value_type=ValueType.LABEL, 
                default_value=output_path
            ), DataField(
                key='Error_Path', 
                label="Error Log", 
                value_type=ValueType.LABEL, 
                default_value=error_path
            ), DataField(
                key='Priority', 
                label="Priority", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'Priority', 'n/a'))
            ), DataField(
                key='Variable_List', 
                label="Environmental Variables", 
                value_type=ValueType.LABEL, 
                default_value=env
            ), DataField(
                key='comment', 
                label="Comment", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'comment', 'n/a'))
            )
        ])
        
        time = DataSection("Time", [
            DataField(
                key='ctime', 
                label="Created Time", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'ctime', 'n/a'))
            ), DataField(
                key='qtime', 
                label="Time Entered Queue", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'qtime', 'n/a'))
            ), DataField(
                key='etime', 
                label="Time Eligible to Run", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'etime', 'n/a'))
            ), DataField(
                key='mtime', 
                label="Last Modified", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'mtime', 'n/a'))
            ), DataField(
                key='start_time', 
                label="Start Time", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'start_time', 'n/a'))
            ), DataField(
                key='comp_time', 
                label="Completed Time", 
                value_type=ValueType.LABEL, 
                default_value=str(deepgetattr(job, 'comp_time', 'n/a'))
            ),
        ])
        
        cluster_job.data_sections.append(resources_allocated)
        cluster_job.data_sections.append(resources_used)
        cluster_job.data_sections.append(time)
        cluster_job.data_sections.append(other)
        
        return cluster_job
    