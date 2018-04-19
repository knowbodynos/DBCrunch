import sys, os
from signal import signal, SIGPIPE, SIG_DFL
from subprocess import Popen, PIPE
from pytz import utc
from datetime import datetime
from math import ceil
from time import sleep

# Subprocess methods

def default_sigpipe():
    signal(SIGPIPE, SIG_DFL)

def retry(script, warn_tries = 10, max_tries = None):
    #MAX_TRIES = 10
    #for i in range(MAX_TRIES):
    if max_tries and warn_tries > max_tries:
        max_tries = warn_tries
    n_tries = 0
    while True:
        proc = Popen(script, shell = True, stdout = PIPE, stderr = PIPE, preexec_fn = default_sigpipe)
        stdout, stderr = proc.communicate()
        if stderr:
            n_tries += 1
            if n_tries > 0 and n_tries % warn_tries == 0:
                sys.stderr.write(stderr + "\n")
                sys.stderr.write(str(n_tries) + " attempts made.")
                sys.stderr.flush()
            if (not max_tries) or n_tries < max_tries:
                sleep(0.1)
            else:
                raise Exception(stderr)
        else:
            return stdout, stderr

def get_timestamp():
    return datetime.utcnow().replace(tzinfo = utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"

# Miscellaneous workload manager methods

def get_partition(job_id):
    script = "sacct -n -j " + job_id + " -o 'Partition%30' | head -n 1 | sed 's/\s//g' | head -c -1"
    stdout, stderr = retry(script)
    return stdout

def get_partition_time_limit(partition):
    script = "sinfo -h -o '%l %P' | grep -E '" + partition + "\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1"
    stdout, stderr = retry(script)
    if stdout == "infinite":
        return None
    return stdout

def n_user_jobs(user_name):
    script = "squeue -h -r -u " + user_name + " -o '%.130j' | grep 'crunch_' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout)

def n_controller_jobs(user_name, module_name, controller_name):
    script = "squeue -h -r -u " + user_name + " -o '%.130j' | grep 'crunch_" + module_name + "_" + controller_name + "_job_' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout)

def n_controller_steps(user_name, module_name, controller_name):
    script = "sacct -n -u " + user_name + " -s 'R' -o 'JobName%50' | grep -E 'crunch_" + module_name + "_" + controller_name + "_job_.*_step_' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout)

def is_controller_running(user_name, module_name, controller_name):
    script = "squeue -h -u " + user_name + " -o '%.130j' | grep 'crunch_" + module_name + "_" + controller_name + "_controller' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout) > 0

def is_dependency_running(user_name, dependencies):
    if len(dependencies) == 0:
        n_jobs_running = 0
    else:
        grep_str = "\|".join(dependencies)
        script = "squeue -h -u " + user_name + " -o '%.130j' | grep 'crunch_\(" + grep_str + "\)_controller' | wc -l | head -c -1"
        stdout, stderr = retry(script)
        n_jobs_running = int(stdout)
    return n_jobs_running > 0

def submit_job(job_path, job_name):
    #print("Line: sbatch " + job_path + "/" + job_name + ".job")
    script = "sbatch " + job_path + "/" + job_name + ".job"
    stdout, stderr = retry(script)
    return stdout.rstrip("\n").split()[-1]

def release_held_jobs(user_name, module_name, controller_name):
    script = "for job in $(squeue -h -u " + user_name + " -o '%j %A %r' | grep 'crunch_" + module_name + "_" + controller_name + "_' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release ${job}; done"
    retry(script)

def get_avail_nodes(partitions):#, minthreads = 1):
    node_info = []
    for partition in partitions:
        #script = "sinfo -h -r -p " + partition + " -o '%N' | xargs scontrol show node | grep -B4 -E 'State=(IDLE|MIXED) ' | tr '\n' ' ' | sed 's/--/\\n/g' | perl -pe 's|.*CPUAlloc=(.*?) .*CPUTot=(.*?) .*NodeHostName=(.*?) .*ThreadsPerCore=(.*?) .*|\\1 \\2 \\3 \\4|g'"
        #temp_nodeinfo = stdout.rstrip("\n")
        script = "sinfo -h -r -p " + partition + " -o '%N' | xargs scontrol show node | grep -B4 -E 'State=(IDLE|MIXED|COMPLETING) ' | tr '\n' ' ' | sed 's/--/\\n/g' | perl -pe 's|.*CPUAlloc=(.*?) .*CPUTot=(.*?) .*NodeHostName=(.*?) .*ThreadsPerCore=(.*?) .*|\\1 \\2 \\3 \\4|g'"
        stdout, stderr = retry(script)
        stdout_split = stdout.rstrip("\n").split("\n")
        temp_node_info = []
        for line in stdout_split:
            fields = line.split()
            if len(fields) == 4:
                n_tot_cpus = int(fields[1])
                n_cpus = int(fields[1]) - int(fields[0])
                host_name = fields[2]
                threads_per_cpu = int(fields[3])
                #min_step_cpus = int(ceil(float(min_threads) / threads_per_cpu))
                #max_steps = n_cpus / min_step_cpus
                #if n_cpus >= min_step_cpus:
                temp_node_info += [{"partition": partition, "ntotcpus": n_tot_cpus, "ncpus": n_cpus, "hostname": host_name, "threadspercpu": threads_per_cpu}]#, "minstepcpus": min_step_cpus, "maxsteps": max_steps}]
        node_info += temp_node_info
    return sorted(node_info, key = lambda x: x["ncpus"], reverse = True)

def get_max_jobs():
    script = "scontrol show config | grep 'MaxJobCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout)

def get_max_steps():
    script = "scontrol show config | grep -E 'MaxJobCount|MaxTasksPerNode' | sed 's/\s//g' | cut -d'=' -f2 | sort -n | head -n1 | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout)

def get_exit_code(job_id):
    script = "sacct -n -j " + job_id + " -o 'ExitCode' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2"
    stdout, stderr = retry(script)
    return stdout

def get_job_state(job_id):
    script = "scontrol show job " + job_id + " | grep 'Reason=' | sed 's/^.*=\(.*\) .*=\(.*\) .*/\\1 \\2/g' | head -c -1"
    stdout, stderr = retry(script)
    return stdout.split()

def cancel_job(job_id):
    script = "scancel " + job_id
    retry(script)

def write_controller_job_file(config, job_name, node):
    #freenodes = get_avail_nodes(config.glob.resources.keys())
    #maxind = len(freenodes) - 1
    #maxshift = min(maxind, nodeshift)
    #node = freenodes[maxind - maxshift]
    n_cpus = int(ceil(float(config.controller.threads.min) / node["threadspercpu"]))

    job_string = "#!/bin/bash\n"
    job_string += "\n"
    job_string += "# Created " + get_timestamp() + "\n"
    job_string += "\n"
    job_string += "# Job name\n"
    job_string += "#SBATCH -J \"" + job_name + "\"\n"
    job_string += "#################\n"
    job_string += "# Working directory\n"
    job_string += "#SBATCH -D \"" + config.controller.path + "\"\n"
    job_string += "#################\n"
    job_string += "# Job output file\n"
    job_string += "#SBATCH -o \"" + job_name + ".info\"\n"
    job_string += "#################\n"
    job_string += "# Job error file\n"
    job_string += "#SBATCH -e \"" + job_name + ".err\"\n"
    job_string += "#################\n"
    job_string += "# Job file write mode\n"
    job_string += "#SBATCH --open-mode=\"" + config.controller.writemode + "\"\n"
    job_string += "#################\n"
    if config.controller.timelimit:
        job_string += "# Job max time\n"
        job_string += "#SBATCH --time=\"" + config.controller.timelimit + "\"\n"
        job_string += "#################\n"
    job_string += "# Partition(s) to use for job\n"
    job_string += "#SBATCH --partition=\"" + node["partition"] + "\"\n"
    job_string += "#################\n"
    #job_string += "# Number of nodes to distribute n tasks across\n"
    #job_string += "#SBATCH -N 1\n"
    #job_string += "#################\n"
    job_string += "# Number of tasks allocated for job\n"
    job_string += "#SBATCH -n 1\n"
    job_string += "#################\n"
    job_string += "# Number of CPUs allocated for each task\n"
    job_string += "#SBATCH -c " + str(n_cpus) + "\n"
    job_string += "#################\n"
    job_string += "# List of nodes to distribute n tasks across\n"
    job_string += "#SBATCH -w \"" + str(node["hostname"]) + "\"\n"
    job_string += "#################\n"
    job_string += "# Requeue job on node failure\n"
    job_string += "#SBATCH --requeue\n"
    job_string += "#################\n"
    job_string += "\n"
    job_string += "python ${CRUNCH_ROOT}/bin/controller.py --controller-path " + config.controller.path + " --controller-id ${SLURM_JOBID}"

    with open(config.controller.path + "/" + job_name + ".job", "w") as job_stream:
        job_stream.write(job_string)
        job_stream.flush()

def write_tool_job_file(config, job_name, node, kwargs):
    #freenodes = get_avail_nodes(config.glob.resources.keys())
    #maxind = len(freenodes) - 1
    #maxshift = min(maxind, kwargs['nodeshift'])
    #node = freenodes[maxind - maxshift]

    tool_name = kwargs["tool"].split(".")[0]

    job_string = "#!/bin/bash\n"
    job_string += "\n"
    job_string += "# Created " + get_timestamp() + "\n"
    job_string += "\n"
    job_string += "# Job name\n"
    job_string += "#SBATCH -J \"" + job_name + "\"\n"
    job_string += "#################\n"
    job_string += "# Working directory\n"
    job_string += "#SBATCH -D \"" + config.controller.path + "/" + tool_name + "\"\n"
    job_string += "#################\n"
    job_string += "# Job output file\n"
    job_string += "#SBATCH -o \"" + job_name + ".info\"\n"
    job_string += "#################\n"
    job_string += "# Job error file\n"
    job_string += "#SBATCH -e \"" + job_name + ".err\"\n"
    job_string += "#################\n"
    job_string += "# Job file write mode\n"
    job_string += "#SBATCH --open-mode=\"truncate\"\n"
    job_string += "#################\n"
    job_string += "# Job max time\n"
    job_string += "#SBATCH --time=\"1-00:00:00\"\n"
    job_string += "#################\n"
    job_string += "# Partition(s) to use for job\n"
    job_string += "#SBATCH --partition=\"" + node["partition"] + "\"\n"
    job_string += "#################\n"
    #job_string += "# Number of nodes to distribute n tasks across\n"
    #job_string += "#SBATCH -N 1\n"
    #job_string += "#################\n"
    job_string += "# Number of tasks allocated for job\n"
    job_string += "#SBATCH -n 1\n"
    job_string += "#################\n"
    job_string += "# Number of CPUs allocated for each task\n"
    job_string += "#SBATCH -c 1\n"
    job_string += "#################\n"
    job_string += "# List of nodes to distribute n tasks across\n"
    job_string += "#SBATCH -w \"" + str(node["hostname"]) + "\"\n"
    job_string += "#################\n"
    job_string += "# Requeue job on node failure\n"
    job_string += "#SBATCH --requeue\n"
    job_string += "#################\n"
    job_string += "\n"
    job_string += "python ${CRUNCH_ROOT}/bin/tools/" + kwargs['tool'] + " "
    if kwargs['job_limit'] != "":
        job_string += "--job-limit " + kwargs['job_limit'] + " "
    if kwargs['time_limit'] != "":
        job_string += "--time-limit " + kwargs['time_limit'] + " "
    job_string += kwargs['in_path'] + " " + kwargs['out_path'] + " " + kwargs['controller_path'] + " " + " ".join(kwargs['out_file_names'])

    if not os.path.isdir(config.controller.path + "/" + tool_name):
        os.mkdir(config.controller.path + "/" + tool_name)

    with open(config.controller.path + "/" + tool_name + "/" + job_name + ".job", "w") as job_stream:
        job_stream.write(job_string)
        job_stream.flush()

'''
def get_pendjob_namespaths(user_name):
    script = "squeue -h -u " + user_name + " -o '%T %j %.130Z' | grep 'crunch_' | grep 'PENDING' | cut -d' ' -f2,3 | grep -v '_controller' | tr '\n' ',' | head -c -1"
    stdout, stderr = retry(script)
    return [x.split() for x in stdout.split(",")]

def get_allocatejob(allocstring):
    #print("Line: sbatch " + job_path + "/" + job_name + ".job")
    script = allocstring
    stdout, stderr = retry(script)
    return stdout.rstrip("\n").split()[-1]

def get_partitionsidle(partitions):
    greppartitions = "|".join(partitions)
    script = "sinfo -h -o '%t %c %D %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'idle' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1"
    stdout, stderr = retry(script)
    return stdout.split(',')

def get_partitionscomp(partitions):
    greppartitions = "|".join(partitions)
    script = "sinfo -h -o '%t %c %D %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'comp' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1"
    stdout, stderr = retry(script)
    return stdout.split(',')

def get_partitionsrun(partitions):
    greppartitions = "|".join(partitions)
    script = "squeue -h -o '%L %T %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'RUNNING' | sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ',' | head -c -1"
    stdout, stderr = retry(script)
    return stdout.split(',')

def get_partitionspend(partitions):
    greppartitions = "|".join(partitions)
    script = "sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | grep 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1"
    stdout, stderr = retry(script)
    return stdout.split(',')

def get_partitionncorespernode(partition):
    #script = "sinfo -h -p '" + partition + "' -o '%c' | head -n1 | head -c -1"
    #return int(stdout)
    script = "echo \"$(scontrol show partition='" + partition + "' | grep 'TotalCPUs=' | perl -pe 's|.*TotalCPUs=([0-9]+).*TotalNodes=([0-9]+).*|\\1/\\2|g')\" | bc | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout)

def get_idlenodeCPUs(partitions):
    strpartitions = ",".join(partitions)
    script = "sinfo -h -r -e -p " + strpartitions + " -t 'IDLE' -o '%R %N %c' | sort -k3,3nr | while read -r partition nodelist ncores; do [[ ${ncores} != \"\" ]] && scontrol show hostname ${nodelist} | while read -r node; do echo \"${partition} ${node} ${ncores} ${ncores}\"; done; done"
    stdout, stderr = retry(script)
    nodeinfo = [x.split() for x in stdout.rstrip("\n").split("\n")]
    return [[x[0], x[1], int(x[2]), int(x[3])] for x in nodeinfo if len(x) == 4]

def get_mixnodeCPUs(partitions):
    strpartitions = ",".join(partitions)
    script = "sinfo -h -r -e -p " + strpartitions + " -t 'MIX' -o '%R %N %c' | while read -r partition nodelist ncores; do squeue -h -w ${nodelist} -o '%N %C' | while read -r node ncoresused; do echo \"${partition} ${node} ${ncores} $((${ncores}-${ncoresused}))\"; done; done | sort -k4,4nr -k1,3 -u"
    stdout, stderr = retry(script)
    nodeinfo = [x.split() for x in stdout.rstrip("\n").split("\n")]
    return [[x[0], x[1], int(x[2]), int(x[3])] for x in nodeinfo if len(x) == 4]

def get_compnodeCPUs(partitions):
    strpartitions = ",".join(partitions)
    script = "sinfo -h -r -e -p " + strpartitions + " -t 'COMP' -o '%R %N %c' | sort -k3,3nr | while read -r partition nodelist ncores; do [[ ${ncores} != \"\" ]] && scontrol show hostname ${nodelist} | while read -r node; do echo \"${partition} ${node} ${ncores} ${ncores}\"; done; done"
    stdout, stderr = retry(script)
    nodeinfo = [x.split() for x in stdout.rstrip("\n").split("\n")]
    return [[x[0], x[1], int(x[2]), int(x[3])] for x in nodeinfo if len(x) == 4]

def get_maxnnodes(partition):
    script = "scontrol show partition '" + partition + "' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1"
    stdout, stderr = retry(script)
    return int(stdout.rstrip("\n"))

def get_jobstats(job_id):
    script = "sacct -n -j " + job_id + " -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2"
    stdout, stderr = retry(script)
    return stdout.split(",")

def get_controller_path(controllerjob_id):
    script = "squeue -h -j " + controllerjob_id + " -o '%Z' | head -c -1"
    stdout, stderr = retry(script)
    return stdout
'''