#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

#    DBCrunch: controller.py
#    Copyright (C) 2017 Ross Altman
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful, 
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys, os, json, yaml, pprint
from errno import ENOENT
from math import ceil
from glob import iglob
from datetime import datetime
from pytz import utc
from time import time, sleep
from threading import Thread, active_count
from argparse import ArgumentParser, REMAINDER
from crunch_config import *

class AsyncTrackLocks(Config, Thread):
    def __init__(self, **kwargs):

        # Initiate config

        Config.__init__(self, **kwargs)

        # Initiate thread

        Thread.__init__(self)

        # Initiate locks directory

        locks_path = self.controller.path + "/locks"
        if not os.path.isdir(locks_path):
                os.mkdir(locks_path)

        # Initiate private variables

        self.__signal = False
        self.daemon = True

    def run(self):
        while not self.__signal:
            lockslist = os.listdir(self.controller.path + "/locks")
            nlocks = 0
            for lockfile in lockslist:
                if lockfile.endswith(".lock"):
                    nlocks += 1
            if nlocks < self.options.nworkers:
                nready = 0
                for readyfile in lockslist:
                    if readyfile.endswith(".ready"):
                        os.rename(self.controller.path + "/locks/" + readyfile, self.controller.path + "/locks/" + readyfile.replace(".ready", ".lock"))
                        nready += 1
                        if nlocks + nready >= self.options.nworkers:
                            break
            sleep(0.1)
            self.reload()
            #print("NThreads: " + str(active_count()))
            #sys.stdout.flush()

    def signal(self):
        self.__signal = True

class BatchCounter(object):
    def __init__(self, counter_path):
        self.__counter_path = counter_path
        self.done = False
        self.load()

    def load(self):
        try:
            with open(self.__counter_path, "r") as counter_stream:
                self.batch, self.step, self.doc = counter_stream.readline().rstrip("\n").split()
        except IOError as e:
            if e.errno != ENOENT:
                raise
            else:
                self.batch = 0
                self.step = 0
                self.doc = 0
                self.dump()

    def dump(self):
        with open(self.__counter_path, "w") as counter_stream:
            counter_stream.write(" ".join([str(x) for x in [self.batch, self.step, self.doc]]))
            counter_stream.flush()

    def incr_batch(self, n):
        assert(isinstance(n, int))
        self.batch += n

    def incr_step(self, n):
        assert(isinstance(n, int))
        self.step += n

    def incr_doc(self, n):
        assert(isinstance(n, int))
        self.doc += n

def time_left(config):
    if config.controller.maxtime:
        return config.starttime + config.controller.maxtime - config.controller.buffertime - time()
    else:
        return 1

def storage_left(config):
    if config.controller.storagelimit:
        return unformat_mem(config.controller.storagelimit) - dir_size(config.controller.path)
    else:
        st = os.statvfs(config.controller.path)
        return st.f_frsize * st.f_bavail

def job_slots_left(wm_api, config):
    nuserjobs = wm_api.n_user_jobs(config.cluster.user)
    if config.cluster.job.jobs.max and nuserjobs >= config.cluster.job.jobs.max:
        return False
    ncontrollerjobs = wm_api.n_controller_jobs(config.cluster.user, config.module.name, config.controller.name)
    if config.job.jobs.max and ncontrollerjobs >= config.job.jobs.max:
        return False
    return True

def write_job_file(wm_api, config, steps):
    job_max_time_limit = 0
    job_n_cpus = 0
    max_cpus = 0
    job_host_names = []
    job_partitions = []
    for step in steps:
        if step["timelimit"] != None and step["timelimit"] > job_max_time_limit:
            job_max_time_limit = step["timelimit"]
        for host_name, host in step["hostlist"].items():
            job_n_cpus += host["ncpus"]
            if host["ncpus"] > max_cpus:
                max_cpus = host["ncpus"]
            if host_name not in job_host_names:
                job_host_names += [host_name]
            if host["partition"] not in job_partitions:
                job_partitions += [host["partition"]]
    
    job_string = "#!/bin/bash\n"
    job_string += "\n"
    job_string += "# Created " + wm_api.get_timestamp() + "\n"
    job_string += "\n"
    job_string += "# Job name\n"
    job_string += "#SBATCH -J \"" + steps[0]["jobname"] + "\"\n"
    job_string += "#################\n"
    job_string += "# Working directory\n"
    job_string += "#SBATCH -D \"" + config.controller.path + "/logs\"\n"
    job_string += "#################\n"
    job_string += "# Job output file\n"
    job_string += "#SBATCH -o \"" + steps[0]["jobname"] + ".info\"\n"
    job_string += "#################\n"
    job_string += "# Job error file\n"
    job_string += "#SBATCH -e \"" + steps[0]["jobname"] + ".err\"\n"
    job_string += "#################\n"
    job_string += "# Job file write mode\n"
    job_string += "#SBATCH --open-mode=\"" + config.job.writemode + "\"\n"
    job_string += "#################\n"
    job_string += "# Job max time\n"
    job_string += "#SBATCH --time=\"" + str(job_max_time_limit) + "\"\n"
    job_string += "#################\n"
    job_string += "# Partition(s) to use for job\n"
    job_string += "#SBATCH --partition=\"" + ",".join(job_partitions) + "\"\n"
    job_string += "#################\n"
    #job_string += "# Number of tasks allocated for job\n"
    #job_string += "#SBATCH -n " + str(len(steps)) + "\n"
    #job_string += "#################\n"
    #job_string += "# Number of CPUs allocated for each task\n"
    #job_string += "#SBATCH -n " + str(job_n_cpus) + "\n"
    #job_string += "#################\n"
    job_string += "# Number of CPUs allocated for each task\n"
    job_string += "#SBATCH -c " + str(max_cpus) + "\n"
    job_string += "#################\n"
    job_string += "# List of nodes to distribute n tasks across\n"
    job_string += "#SBATCH -w \"" + ",".join(job_host_names) + "\"\n"
    job_string += "#################\n"
    job_string += "# Requeue job on node failure\n"
    job_string += "#SBATCH --requeue\n"
    job_string += "#################\n"
    job_string += "\n"

    job_string += "# Initialize job steps\n"
    #jobinfo = []
    i = 0
    for step in steps:
        host_names = []
        partitions = []
        n_cpus = []
        n_procs = []
        for host_name, host in step["hostlist"].items():
            host_names += [host_name]
            partitions += [host["partition"]]
            n_cpus += [host["ncpus"]]
            n_procs += [host["nprocs"]]

        job_string += "# " + str(i + 1) + "\n"
        job_string += "srun -w \"" + ",".join(host_names) + "\" -n \"1\" -c \"" + str(sum(n_cpus)) + "\" -J \"" + step["name"] + "\" --mem-per-cpu=\"" + step["cpumemorylimit"] + "\" "
        #job_string += "srun -w \"" + ",".join(host_names) + "\" -n \"" + str(sum(n_cpus)) + "\" -J \"" + step["name"] + "\" --mem-per-cpu=\"" + step["cpumemorylimit"] + "\" "
        if step["timelimit"]:
            job_string += "--time=\"" + step["timelimit"] + "\" "
        #wrapstep = {}
        #for key in step.keys():
        #    if key != "docs":
        #        wrapstep[key] = step[key]
        job_string += "python ${CRUNCH_ROOT}/bin/wrapper.py --controller-path \"" + config.controller.path + "\" --step-name \"" + step["name"] + "\" --step-id \"${SLURM_JOBID}." + str(i) + "\" --stats \"TotalCPUTime\" \"Rss\" \"Size\" &"#"\" --stepdoc " + json.dumps(wrapstep, separators = (',',':'))
        job_string += "\n\n"
        #jobinfo += [{"host_names": host_names, "jobstepname": step["name"], "partitions": partitions, "ncpus": sum(n_cpus), "mem": step["cpumemorylimit"], "n_docs": len(step["docs"])}]
        i += 1

    job_string += "wait"

    with open(config.controller.path + "/jobs/" + steps[0]["jobname"] + ".job", "w") as job_stream:
        job_stream.write(job_string)
        job_stream.flush()

    #return jobinfo

def write_job_submit_details(wm_api, config, steps, refill = False):
    steps = sorted(steps, key = lambda step: tuple(int(x) for x in step["name"].split("_") if x.isdigit()))

    all_partitions = []
    step_partitions = []
    all_host_names = []
    all_n_cpus = 0
    step_n_cpus = []
    step_memorylimits = []
    all_mem = 0
    for step in steps:
        partitions = []
        n_cpus = 0
        for host_name, host in step["hostlist"].items():
            if host["partition"] not in all_partitions:
                all_partitions += [host["partition"]]
            if host["partition"] not in partitions:
                partitions += [host["partition"]]
            if host_name not in all_host_names:
                all_host_names += [host_name]
            all_n_cpus += host["ncpus"]
            n_cpus += host["ncpus"]
        step_memorylimit = n_cpus * unformat_mem(step["cpumemorylimit"])
        all_mem += step_memorylimit
        step_memorylimits += [format_mem(step_memorylimit, unit = "MB")]
        step_partitions += [partitions]
        step_n_cpus += [n_cpus]
    all_partitions = sorted(all_partitions)
    all_mem = format_mem(all_mem, unit = "MB")
    print(wm_api.get_timestamp())
    if refill:
        submitstring = "Added documents to "
    else:
        submitstring = "Submitted "
    #try:
    job_id = steps[0]["id"].split(".")[0]
    #except IndexError:
    #    print(pprint.pformat(steps))
    #    sys.stdout.flush()
    #    raise
    print(submitstring + "batch job " + job_id + " as " + steps[0]["jobname"] + " on partition(s) [" + ", ".join(all_partitions) + "] with " + str(len(all_host_names)) + " nodes, " + str(all_n_cpus) + " CPU(s), and " + str(all_mem) + " RAM allocated.")
    for i in range(len(steps)):
        n_iters = min(config.options.nbatch, len(steps[i]["docs"]))
        print("...(" + str(i + 1) + ")...with job step " + steps[i]["id"] + " as " + steps[i]["name"] + " in batches of " + str(n_iters) + "/" + str(len(steps[i]["docs"])) + " iteration(s) on partition [" + ", ".join(step_partitions[i]) + "] with " + str(len(steps[i]["hostlist"].keys())) + " nodes, " + str(step_n_cpus[i]) + " CPU(s), and " + str(step_memorylimits[i]) + " RAM allocated.")
    print("")
    sys.stdout.flush()

def wait_for_slots(wm_api, config):
    with open(config.controller.path + "/status", "w") as status_stream:
        status_stream.truncate(0)
        status_stream.write("Waiting for slots.")
        status_stream.flush()

    while storage_left(config) <= 0:
        sleep(0.1)

    if config.options.nrefill and wm_api.n_controller_steps(config.cluster.user, config.module.name, config.controller.name) >= config.options.nworkers:
        steps = []
        for refill_file in iglob(config.controller.path + "/docs/*.refill"):
            with open(refill_file, "r") as refill_stream:
                refill_line = refill_stream.readline().rstrip("\n")
                steps.append(json.loads(refill_line))
        if len(steps) > config.options.nrefill:
            return True, steps

    nodes = wm_api.get_avail_nodes(config.cluster.resources.keys())
    if config.job.threads.min:
        has_wrapper_node = False
        for node in nodes:
            min_step_cpus = int(ceil(float(config.job.threads.min) / node["threadspercpu"]))
            if node["ncpus"] >= min_step_cpus:
                has_wrapper_node = True
                break
    else:
        has_wrapper_node = True
    if not has_wrapper_node:
        nodes = []
    wm_api.release_held_jobs(config.cluster.user, config.module.name, config.controller.name)
    #print((time_left(config) > 0, job_slots_left(wm_api, config), len(nodes) > 0, storage_left(config) > 0))
    #sys.stdout.flush()
    while time_left(config) > 0 and not (job_slots_left(wm_api, config) and len(nodes) > 0 and storage_left(config) > 0):
        sleep(0.1)
        #print((time_left(config) > 0, job_slots_left(wm_api, config), len(nodes) > 0, storage_left(config) > 0))
        #sys.stdout.flush()

        if config.options.nrefill and wm_api.n_controller_steps(config.cluster.user, config.module.name, config.controller.name) >= config.options.nworkers:
            steps = []
            for refill_file in iglob(config.controller.path + "/docs/*.refill"):
                with open(refill_file, "r") as refill_stream:
                    refill_line = refill_stream.readline().rstrip("\n")
                    steps.append(json.loads(refill_line))
            if len(steps) > config.options.nrefill:
                return True, steps

        nodes = wm_api.get_avail_nodes(config.cluster.resources.keys())
        if config.job.threads.min:
            has_wrapper_node = False
            for node in nodes:
                min_step_cpus = int(ceil(float(config.job.threads.min) / node["threadspercpu"]))
                if node["ncpus"] >= min_step_cpus:
                    has_wrapper_node = True
                    break
        else:
            has_wrapper_node = True
        if not has_wrapper_node:
            nodes = []
        wm_api.release_held_jobs(config.cluster.user, config.module.name, config.controller.name)

    #print((time_left(config) > 0, job_slots_left(wm_api, config), len(nodes) > 0, storage_left(config) > 0))
    #sys.stdout.flush()
    return False, nodes

def prep_nodes(wm_api, config, refill, slots, doc_batch, start_slot = 0):
    n_iters = config.options.niters

    with open(config.controller.path + "/status", "w") as status_stream:
        status_stream.write("Populating steps.")
        status_stream.flush()

    if refill:
        return slots

    nodes = slots[start_slot:]
    n_docs = len(doc_batch)
    wrapper_procs = []
    other_procs = []
    for node in nodes:
        n_wrapper_cpus = int(ceil(float(config.job.threads.min) / node["threadspercpu"]))
        n_wrappers = node["ncpus"] / n_wrapper_cpus
        i = 0
        if len(wrapper_procs) == 0:
            while i < n_wrappers and (n_docs == 0 or len(wrapper_procs) * n_iters < n_docs):
                wrapper_procs += [{"partition": node["partition"], "hostname": node["hostname"], "ncpus": n_wrapper_cpus, "cpumem": config.cluster.resources[node["partition"]]["memorylimit"] / node["ntotcpus"]}]
                i += 1
        if node["ncpus"] - (i * n_wrapper_cpus) > 0:
            other_procs += [{"partition": node["partition"], "hostname": node["hostname"], "ncpus": node["ncpus"] - (i * n_wrapper_cpus), "cpumem": config.cluster.resources[node["partition"]]["memorylimit"] / node["ntotcpus"]}]
    #print("wrapper_procs: " + str(wrapper_procs))
    #print("other_procs: " + str(other_procs))
    #sys.stdout.flush()
    steps = []
    host_names = []
    break_flag = False
    next_doc_ind = 0
    while len(wrapper_procs) > 0 and len(steps) < config.job.steps.max and not break_flag:
        i = 0
        host_name = wrapper_procs[0]["hostname"]
        if host_name not in host_names:
            if len(steps) > 0:
                break_flag = True
                break
            host_names += [host_name]
        partition = wrapper_procs[0]["partition"]
        min_time_limit = unformat_duration(config.job.timelimit, unit = "seconds")
        part_time_limit = unformat_duration(wm_api.get_partition_time_limit(partition), unit = "seconds")
        if not min_time_limit or part_time_limit < min_time_limit:
            min_time_limit = part_time_limit
        min_cpu_mem = config.job.memorylimit
        if not min_cpu_mem or wrapper_procs[0]["cpumem"] < min_cpu_mem:
            min_cpu_mem = wrapper_procs[0]["cpumem"]
        n_cpus = wrapper_procs[0]["ncpus"]
        step = {
                 "hostlist":
                   {
                     host_name:
                       {
                         "partition": partition,
                         "nprocs": 1,
                         "ncpus": n_cpus
                       }
                    }
               }
        if len(doc_batch) > 0 and config.db.nprocsfield:
            max_step_procs = max([x[config.db.nprocsfield] for x in doc_batch[next_doc_ind:next_doc_ind + n_iters]])
        else:
            max_step_procs = 1
        del wrapper_procs[0]
        i += 1
        while i < max_step_procs and (len(wrapper_procs) > 0 or len(other_procs) > 0) and not break_flag:
            while i < max_step_procs and len(wrapper_procs) > 0:
                host_name = wrapper_procs[0]["hostname"]
                if host_name not in host_names:
                    if len(steps) > 0:
                        break_flag = True
                        break
                    host_names += [host_name]
                partition = wrapper_procs[0]["partition"]
                part_time_limit = unformat_duration(wm_api.get_partition_time_limit(partition), unit = "seconds")
                if part_time_limit < min_time_limit:
                    min_time_limit = part_time_limit
                if  wrapper_procs[0]["cpumem"] < min_cpu_mem:
                    min_cpu_mem = wrapper_procs[0]["cpumem"]
                n_cpus = 1
                if host_name not in step:
                    step["hostlist"][host_name] = {
                                                   "partition": partition,
                                                   "nprocs": 1,
                                                   "ncpus": n_cpus
                                                 }
                else:
                    step["hostlist"][host_name]["nprocs"] += 1
                    step["hostlist"][host_name]["ncpus"] += n_cpus
                wrapper_procs[0]["ncpus"] -= 1
                other_procs += [wrapper_procs[0]]
                del wrapper_procs[0]
                i += 1
            while i < max_step_procs and len(other_procs) > 0:
                host_name = other_procs[0]["hostname"]
                if host_name not in host_names:
                    if len(steps) > 0:
                        break_flag = True
                        break
                    host_names += [host_name]
                partition = other_procs[0]["partition"]
                part_time_limit = unformat_duration(wm_api.get_partition_time_limit(partition), unit = "seconds")
                if part_time_limit < min_time_limit:
                    min_time_limit = part_time_limit
                if  other_procs[0]["cpumem"] < min_cpu_mem:
                    min_cpu_mem = other_procs[0]["cpumem"]
                n_cpus = 1
                if host_name not in step:
                    step["hostlist"][host_name] = {
                                                   "partition": partition,
                                                   "nprocs": 1,
                                                   "ncpus": n_cpus
                                                 }
                else:
                    step["hostlist"][host_name]["nprocs"] += 1
                    step["hostlist"][host_name]["ncpus"] += n_cpus
                other_procs[0]["ncpus"] -= 1
                if other_procs[0]["ncpus"] == 0:
                    del other_procs[0]
                i += 1
        if i == max_step_procs:
            step["cpumemorylimit"] = format_mem(min_cpu_mem, unit = "MB")
            step["maxtime"] = min_time_limit
            step["timelimit"] = format_duration(min_time_limit)
            #step["buffertime"] = format_duration(config.job.buffertime)
            steps += [step]
            next_doc_ind += n_iters

    return steps

def do_input(wm_api, config, doc_batch):
    config.reload()
    #print("NThreads: " + str(active_count()))
    #sys.stdout.flush()
    
    refill, slots = wait_for_slots(wm_api, config)
    steps = prep_nodes(wm_api, config, refill, slots, doc_batch, start_slot = 0)
    
    return steps

def do_verify(wm_api, config, counter, doc_batch):
    n_iters = config.options.niters
    start_slot = 0
    while time_left(config) > 0:
        refill, slots = wait_for_slots(wm_api, config)
        steps = prep_nodes(wm_api, config, refill, slots, doc_batch, start_slot = start_slot)

        job_name = "crunch_" + config.module.name + "_" + config.controller.name + "_job_" + str(counter.batch + 1) + "_steps_" + str(counter.step + 1) + "-" + str(counter.step + len(steps))
        
        i = 0
        next_doc_ind = 0
        filled_steps = []
        while i < len(steps) and next_doc_ind + n_iters <= len(doc_batch):
            step = steps[i]
            step["docs"] = doc_batch[next_doc_ind:next_doc_ind + n_iters]
            if "jobname" not in step:
                step["jobname"] = job_name
            if "name" not in step:
                step["name"] = "crunch_" + config.module.name + "_" + config.controller.name + "_job_" + str(counter.batch + 1) + "_step_" + str(i + 1)
            #print("Step name: " + steps[i]["name"])
            #print("Length steps_docs: " + str(len(steps[i]["docs"])) + "\n")
            #sys.stdout.flush()
            filled_steps.append(step)
            i += 1
            next_doc_ind += n_iters
        
        steps = filled_steps

        if refill:
            break
        else:
            with open(config.controller.path + "/status", "w") as status_stream:
                status_stream.write("Writing job file.")
                status_stream.flush()

            write_job_file(wm_api, config, steps)

            with open(config.controller.path + "/status", "w") as status_stream:
                status_stream.write("Submitting job.")
                status_stream.flush()

            #job_id = "test"

            job_id = wm_api.submit_job(config.controller.path + "/jobs", job_name)
            wm_api.release_held_jobs(config.cluster.user, config.module.name, config.controller.name)
            job_state = wm_api.get_job_state(job_id)
            start_time = time()
            while time() - start_time < 30 and job_state[0] == "PENDING" and job_state[1] == "None":
                sleep(0.1)
                job_state = wm_api.get_job_state(job_id)
            if job_state[0] in ["RUNNING", "COMPLETING", "COMPLETED"] or (job_state[0] == "PENDING" and job_state[1] == "None"):
                for i in range(len(steps)):
                    steps[i]["id"] = job_id + "." + str(i)
                break
            else:
                wm_api.cancel_job(job_id)
                os.remove(config.controller.path + "/jobs/" + job_name + ".job")
                start_slot += 1
                if start_slot == len(slots):
                    start_slot = 0
                    sleep(10)
            
            #for i in range(len(steps)):
            #    if "id" not in steps[i]:
            #        steps[i]["id"] = str(job_id) + "." + str(i)

        #break

    doc_batch = doc_batch[next_doc_ind:]

    write_job_submit_details(wm_api, config, steps, refill = refill)

    wm_api.release_held_jobs(config.cluster.user, config.module.name, config.controller.name)

    return refill, steps, doc_batch

def do_initialize(config, steps, refill):
    with open(config.controller.path + "/status", "w") as status_stream:
        status_stream.write("Initializing job.")
        status_stream.flush()

    if refill:
        for step in steps:
            with open(config.controller.path + "/docs/" + step["name"] + ".refill", "w") as doc_stream:
                for doc in step["docs"]:
                    doc_stream.write(json.dumps(doc, separators = (',', ':')) + "\n")
                    doc_stream.flush()
                os.rename(doc_stream.name, doc_stream.name.replace(".refill", ".docs"))
    else:
        for step in steps:
            with open(config.controller.path + "/docs/" + step["name"] + ".docs", "w") as doc_stream:
                for doc in step["docs"]:
                    doc_stream.write(json.dumps(doc, separators = (',', ':')) + "\n")
                    doc_stream.flush()
            wrap_step = {}
            for key in step.keys():
                if key != "docs":
                    wrap_step[key] = step[key]
            with open(config.controller.path + "/jobs/" + step["name"] + ".step", "w") as step_stream:
                #print(step_stream.name)
                #sys.stdout.flush()
                yaml.dump(wrap_step, step_stream)

def next_batch(wm_api, db_cursor, config, counter, doc_batch):
    #doc_batch = []
    config.reload()
    #print("NThreads: " + str(active_count()))
    #sys.stdout.flush()
    
    steps = do_input(wm_api, config, doc_batch)

    n_docs = len(steps) * config.options.niters

    with open(config.controller.path + "/status", "w") as status_stream:
        status_stream.write("Loading input from database.")
        status_stream.flush()

    if len(doc_batch) < n_docs:
        counter.done = True
        for doc in db_cursor:
            doc_batch.append(doc)
            #print(len(doc_batch))
            #sys.stdout.flush()
            counter.incr_doc(1)
            if len(doc_batch) == n_docs:
                counter.done = False
                break

    refill, steps, doc_batch = do_verify(wm_api, config, counter, doc_batch)

    do_initialize(config, steps, refill)

    if not refill:
        counter.incr_batch(1)
        counter.incr_step(len(steps))
    counter.dump()

    return doc_batch

def iterate_batches(wm_api, db_api, db_collections, config, counter, doc_batch):
    if config.db.api == "db_mongodb":
        kwargs = {
                     "no_cursor_timeout": True,
                     "allow_partial_results": True,
                 }
    db_cursor = db_collections.find(config.db.query, config.db.projection, **kwargs)
    db_cursor = db_cursor.hint(config.db.hint)
    db_cursor = db_cursor.skip(config.db.skip)
    db_cursor = db_cursor.limit(config.db.limit)
    if config.db.sort:
        db_cursor = db_cursor.sort(config.db.sort)
    while time_left(config) > 0 and not counter.done:
        try:
            doc_batch = next_batch(wm_api, db_cursor, config, counter, doc_batch)
        except StopIteration:
            break

    return doc_batch

# Load arguments

parser = ArgumentParser()

parser.add_argument('--controller-path', dest = 'controller_path', action = 'store', required = True, help = '')
parser.add_argument('--controller-id', dest = 'controller_id', action = 'store', required = True, help = '')

kwargs = vars(parser.parse_known_args()[0])

# Configure controller

config = Config(**kwargs)

# Import workload manager API

wm_api = __import__(config.cluster.wm.api)

# Import database module API

db_api = __import__(config.db.api)

db_host = str(config.db.host) + ":" + str(config.db.port) + "/" + config.db.name
if config.db.api == "db_mongodb":
    if config.db.username and config.db.password:
        db_host = config.db.username + ":" + config.db.password + "@" + db_host
    db_uri = "mongodb://" + db_host
db_client = db_api.dbClient(db_uri)
db_database = db_client[config.db.name]
db_collections = db_api.dbCollections()
for collection_name in config.db.collections:
    db_collections.join(db_database[collection_name])

# Create controller subdirectories

for directory in ["jobs", "docs", "logs", "bkps"]:
    path_to_dir = config.controller.path + "/" + directory
    if not os.path.isdir(path_to_dir):
        if directory != "bkps" or config.options.intermedlocal or config.options.outlocal or config.options.statslocal:
            os.mkdir(path_to_dir)

# Begin thread for tracking job step locks

locker = AsyncTrackLocks(**kwargs)
locker.start()

# Block jobs until dependencies complete (if set)

if config.options.blocking:
    while time_left(config) > 0 and wm_api.is_dependency_running(config.cluster.user, config.controller.dependencies):
        sleep(0.1)

# Start controller

with open(config.controller.path + "/status", "w") as status_stream:
    status_stream.write("Starting controller.")
    status_stream.flush()

print(wm_api.get_timestamp())
print("Starting job crunch_" + config.module.name + "_" + config.controller.name + "_controller")
print("")
sys.stdout.flush()

# Initialize counter and batch list

counter = BatchCounter(config.controller.path + "/counter")
doc_batch = []

# Begin controller body

doc_batch = iterate_batches(wm_api, db_api, db_collections, config, counter, doc_batch)

while time_left(config) > 0 and (wm_api.is_dependency_running(config.cluster.user, config.controller.dependencies) or wm_api.n_controller_jobs(config.cluster.user, config.module.name, config.controller.name) > 0):
    doc_batch = iterate_batches(wm_api, db_api, db_collections, config, counter, doc_batch)

doc_batch = iterate_batches(wm_api, db_api, db_collections, config, counter, doc_batch)

# Tie up loose ends and restart controller if necessary

if config.options.nrefill:
    for refill_file in os.listdir(config.controller.path + "/docs"):
        if refill_file.endswith(".refill"):
            os.remove(config.controller.path + "/docs/" + refill_file)
            open(config.controller.path + "/docs/" + refill_file.replace(".refill", ".done"), "w").close()

job_name = "crunch_" + config.module.name + "_" + config.controller.name + "_controller"

if (not time_left(config) > 0) and (firstlastrun or wm_api.is_dependency_running(config.cluster.user, config.controller.dependencies) or wm_api.n_controller_jobs(config.cluster.user, config.module.name, config.controller.name) > 0):
    slots = wm_api.get_avail_nodes(config.cluster.resources.keys())
    if config.controller.threads.min:
        has_wrapper_node = False
        for node in slots:
            min_step_cpus = int(ceil(float(config.controller.threads.min) / node["threadspercpu"]))
            if node["ncpus"] >= min_step_cpus:
                has_wrapper_node = True
                break
    else:
        has_wrapper_node = True
    if not has_wrapper_node:
        slots = []
    while time_left(config) > 0 and not (job_slots_left(wm_api, config) and len(slots) > 0 and storage_left(config) > 0):
        sleep(0.1)

        slots = wm_api.get_avail_nodes(config.cluster.resources.keys())
        if config.controller.threads.min:
            has_wrapper_node = False
            for node in slots:
                min_step_cpus = int(ceil(float(config.controller.threads.min) / node["threadspercpu"]))
                if node["ncpus"] >= min_step_cpus:
                    has_wrapper_node = True
                    break
        else:
            has_wrapper_node = True
        if not has_wrapper_node:
            slots = []

    node = slots[0]
    max_time_limit = wm_api.get_partition_time_limit(node["partition"])
    if unformat_duration(max_time_limit, unit = "seconds") < config.controller.timelimit:
        config.controller.wmtimelimit = max_time_limit
    wm_api.write_controller_job_file(config, job_name, node)
    job_id = wm_api.submit_job(config.controller.path, job_name)

    with open(config.controller.path + "/status", "w") as status_stream:
        status_stream.write("Resubmitting controller.")
        status_stream.flush()

    mem = format_mem((config.cluster.resources[node["partition"]]["memorylimit"] / node["ntotcpus"]) * node["ncpus"], unit = "MB")

    print("")
    print(wm_api.get_timestamp())
    print("Resubmitted batch job " + job_id + " as " + job_name + " on partition " + node["partition"] + " with 1 node, " + str(node["ncpus"]) + " CPU(s), and " + mem + " RAM allocated.")
    print("")
else:
    with open(config.controller.path + "/status", "w") as status_stream:
        status_stream.write("Completing controller.")
        status_stream.flush()

    print("")
    print(wm_api.get_timestamp())
    print("Completing job " + job_name + "\n")
    sys.stdout.flush()

#inputdoc = do_input(config, [])
#doc_batch = [{"n": i, "NNVERTS": 1} for i in range(1605)]
#batchincr, next_doc_ind, stepbatchincr, stepnext_doc_ind = do_action(config, [1, 1, 1, 1], inputdoc, doc_batch)

#print((batchincr, next_doc_ind, stepbatchincr, stepnext_doc_ind))
#sys.stdout.flush()

locker.signal()
locker.join()

db_client.close()