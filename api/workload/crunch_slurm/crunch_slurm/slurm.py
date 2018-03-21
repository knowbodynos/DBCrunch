import os
from signal import signal, SIGPIPE, SIG_DFL
from subprocess import Popen, PIPE
from pytz import utc
from datetime import datetime
from math import ceil
from time import sleep

def default_sigpipe():
    signal(SIGPIPE, SIG_DFL)

def get_timestamp():
    return datetime.utcnow().replace(tzinfo = utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"

def retry(script):
    MAX_TRIES = 6
    for i in range(MAX_TRIES):
        proc = Popen(script, shell = True, stdout = PIPE, stderr = PIPE, preexec_fn = default_sigpipe)
        stdout, stderr = proc.communicate()
        if stderr:
            sleep(0.05)
        else:
            return stdout, stderr

def get_maxtimelimit(partition):
    script = "sinfo -h -o '%l %P' | grep -E '" + partition + "\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1"
    stdout, stderr = retry(script)
    return stdout

def get_pendjobnamespaths(username):
    script = "squeue -h -u " + username + " -o '%T %j %.130Z' | grep 'crunch_' | grep 'PENDING' | cut -d' ' -f2,3 | grep -v '_controller' | tr '\n' ',' | head -c -1"
    stdout, stderr = retry(script)
    return [x.split() for x in stdout.split(",")]

def get_nglobaljobs(username):
    script = "squeue -h -r -u " + username + " -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout)

def get_nlocaljobs(username, modname, controllername):
    script = "squeue -h -r -u " + username + " -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | grep 'crunch_" + modname + "_" + controllername + "_job_' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout)

def get_controllerjobsrunningq(username, modname, controllername):
    script = "squeue -h -u " + username + " -o '%j' | grep 'crunch_" + modname + "_" + controllername + "_job' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout) > 0

def get_controllerrunningq(username, modname, controllername):
    script = "squeue -h -u " + username + " -o '%j' | grep 'crunch_" + modname + "_" + controllername + "_controller' | wc -l | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout) > 0

def get_prevcontrollerjobsrunningq(username, dependencies):
    if len(dependencies) == 0:
        njobsrunning = 0
    else:
        grepstr = "\|".join(dependencies)
        script = "squeue -h -u " + username + " -o '%j' | grep 'crunch_\(" + grepstr + "\)_' | wc -l | head -c -1"
        stdout, stderr = retry(script)
        njobsrunning = eval(stdout)
    return njobsrunning > 0

def get_ncontrollerstepsrunning(username, modname, controllername):
    script = "sacct -n -u " + username + " -s 'R' -o 'Jobname%50' | grep -E 'crunch_" + modname + "_" + controllername + "_job_.*_step_' | wc -l"
    stdout, stderr = retry(script)
    return eval(stdout)

def get_allocatejob(allocstring):
    #print("Line: sbatch " + jobpath + "/" + jobname + ".job")
    script = allocstring
    stdout, stderr = retry(script)
    return stdout.rstrip("\n").split()[-1]

def get_submitjob(jobpath, jobname):
    #print("Line: sbatch " + jobpath + "/" + jobname + ".job")
    script = "sbatch " + jobpath + "/" + jobname + ".job"
    stdout, stderr = retry(script)
    return stdout.rstrip("\n").split()[-1]

def get_releaseheldjobs(username, modname, controllername):
    script = "for job in $(squeue -h -u " + username + " -o '%j %A %r' | grep 'crunch_" + modname + "_" + controllername + "_' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done"
    retry(script)

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
    #return eval(stdout)
    script = "echo \"$(scontrol show partition='" + partition + "' | grep 'TotalCPUs=' | perl -pe 's|.*TotalCPUs=([0-9]+).*TotalNodes=([0-9]+).*|\\1/\\2|g')\" | bc | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout)

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

def get_freenodes(partitions):
    nodeinfo = []
    for partition in partitions:
        #script = "sinfo -h -r -p " + partition + " -o '%N' | xargs scontrol show node | grep -B4 -E 'State=(IDLE|MIXED) ' | tr '\n' ' ' | sed 's/--/\\n/g' | perl -pe 's|.*CPUAlloc=(.*?) .*CPUTot=(.*?) .*NodeHostName=(.*?) .*ThreadsPerCore=(.*?) .*|\\1 \\2 \\3 \\4|g'"
        #temp_nodeinfo = stdout.rstrip("\n")
        script = "sinfo -h -r -p " + partition + " -o '%N' | xargs scontrol show node | grep -B4 -E 'State=(IDLE|MIXED|COMPLETING) ' | tr '\n' ' ' | sed 's/--/\\n/g' | perl -pe 's|.*CPUAlloc=(.*?) .*CPUTot=(.*?) .*NodeHostName=(.*?) .*ThreadsPerCore=(.*?) .*|\\1 \\2 \\3 \\4|g'"
        stdout, stderr = retry(script)
        temp_nodeinfo = [x.split() for x in stdout.rstrip("\n").split("\n")]
        temp_nodeinfo = [{"partition": partition, "hostname": x[2], "ntotcpus": int(x[1]), "ncpus": int(x[1]) - int(x[0]), "threadspercpu": int(x[3])} for x in temp_nodeinfo if (len(x) == 4)]
        nodeinfo += temp_nodeinfo
    return sorted(nodeinfo, key = lambda x: x["ncpus"], reverse = True)

def get_maxnnodes(partition):
    script = "scontrol show partition '" + partition + "' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout.rstrip("\n"))

def get_maxjobcount():
    script = "scontrol show config | grep 'MaxJobCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout)

def get_maxstepcount():
    script = "scontrol show config | grep 'MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1"
    stdout, stderr = retry(script)
    return eval(stdout)

def get_controllerstats(controllerjobid):
    script = "sacct -n -j " + controllerjobid + " -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2"
    stdout, stderr = retry(script)
    return stdout.split(",")

def get_controllerpath(controllerjobid):
    script = "squeue -h -j " + controllerjobid + " -o '%Z' | head -c -1"
    stdout, stderr = retry(script)
    return stdout

def get_exitcode(jobid):
    script = "sacct -n -j " + jobid + " -o 'ExitCode' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2"
    stdout, stderr = retry(script)
    return stdout

def get_job_state(jobid):
    script = "scontrol show job " + jobid + " | grep 'Reason=' | sed 's/^.*=\(.*\) .*=\(.*\) .*/\\1 \\2/g' | head -c -1"
    stdout, stderr = retry(script)
    return stdout.split()

def get_canceljob(jobid):
    script = "scancel " + jobid
    retry(script)

def get_writecontrollerjobfile(crunchconfigdoc, controllerconfigdoc, jobname, node):
    #freenodes = get_freenodes(controllerconfigdoc["partitions"])
    #maxind = len(freenodes) - 1
    #maxshift = min(maxind, nodeshift)
    #node = freenodes[maxind - maxshift]
    ncpus = int(ceil(float(crunchconfigdoc["min-controller-threads"]) / node["threadspercpu"]))

    jobstring = "#!/bin/bash\n"
    jobstring += "\n"
    jobstring += "# Created " + get_timestamp() + "\n"
    jobstring += "\n"
    jobstring += "# Job name\n"
    jobstring += "#SBATCH -J \"" + jobname + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Working directory\n"
    jobstring += "#SBATCH -D \"" + controllerconfigdoc["workdir"] + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Job output file\n"
    jobstring += "#SBATCH -o \"" + jobname + ".info\"\n"
    jobstring += "#################\n"
    jobstring += "# Job error file\n"
    jobstring += "#SBATCH -e \"" + jobname + ".err\"\n"
    jobstring += "#################\n"
    jobstring += "# Job file write mode\n"
    jobstring += "#SBATCH --open-mode=\"" + controllerconfigdoc["writemode"] + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Job max time\n"
    jobstring += "#SBATCH --time=\"" + controllerconfigdoc["timelimit"] + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Partition(s) to use for job\n"
    jobstring += "#SBATCH --partition=\"" + node["partition"] + "\"\n"
    jobstring += "#################\n"
    #jobstring += "# Number of nodes to distribute n tasks across\n"
    #jobstring += "#SBATCH -N 1\n"
    #jobstring += "#################\n"
    jobstring += "# Number of tasks allocated for job\n"
    jobstring += "#SBATCH -n 1\n"
    jobstring += "#################\n"
    jobstring += "# Number of CPUs allocated for each task\n"
    jobstring += "#SBATCH -c " + str(ncpus) + "\n"
    jobstring += "#################\n"
    jobstring += "# List of nodes to distribute n tasks across\n"
    jobstring += "#SBATCH -w \"" + str(node["hostname"]) + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Requeue job on node failure\n"
    jobstring += "#SBATCH --requeue\n"
    jobstring += "#################\n"
    jobstring += "\n"
    jobstring += "python ${CRUNCH_ROOT}/bin/controller.py ${SLURM_JOBID} " + controllerconfigdoc["workdir"]

    with open(controllerconfigdoc["workdir"] + "/" + jobname + ".job", "w") as jobstream:
        jobstream.write(jobstring)
        jobstream.flush()

def get_writetooljobfile(crunchconfigdoc, controllerconfigdoc, jobname, toolname, node, kwargs):
    #freenodes = get_freenodes(controllerconfigdoc["partitions"])
    #maxind = len(freenodes) - 1
    #maxshift = min(maxind, kwargs['nodeshift'])
    #node = freenodes[maxind - maxshift]

    jobstring = "#!/bin/bash\n"
    jobstring += "\n"
    jobstring += "# Created " + get_timestamp() + "\n"
    jobstring += "\n"
    jobstring += "# Job name\n"
    jobstring += "#SBATCH -J \"" + jobname + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Working directory\n"
    jobstring += "#SBATCH -D \"" + kwargs['controllerpath'] + "/" + toolname + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Job output file\n"
    jobstring += "#SBATCH -o \"" + jobname + ".info\"\n"
    jobstring += "#################\n"
    jobstring += "# Job error file\n"
    jobstring += "#SBATCH -e \"" + jobname + ".err\"\n"
    jobstring += "#################\n"
    jobstring += "# Job file write mode\n"
    jobstring += "#SBATCH --open-mode=\"truncate\"\n"
    jobstring += "#################\n"
    jobstring += "# Job max time\n"
    jobstring += "#SBATCH --time=\"1-00:00:00\"\n"
    jobstring += "#################\n"
    jobstring += "# Partition(s) to use for job\n"
    jobstring += "#SBATCH --partition=\"" + node["partition"] + "\"\n"
    jobstring += "#################\n"
    #jobstring += "# Number of nodes to distribute n tasks across\n"
    #jobstring += "#SBATCH -N 1\n"
    #jobstring += "#################\n"
    jobstring += "# Number of tasks allocated for job\n"
    jobstring += "#SBATCH -n 1\n"
    jobstring += "#################\n"
    jobstring += "# Number of CPUs allocated for each task\n"
    jobstring += "#SBATCH -c 1\n"
    jobstring += "#################\n"
    jobstring += "# List of nodes to distribute n tasks across\n"
    jobstring += "#SBATCH -w \"" + str(node["hostname"]) + "\"\n"
    jobstring += "#################\n"
    jobstring += "# Requeue job on node failure\n"
    jobstring += "#SBATCH --requeue\n"
    jobstring += "#################\n"
    jobstring += "\n"
    jobstring += "python ${CRUNCH_ROOT}/bin/tools/" + kwargs['tool'] + " "
    if kwargs['job_limit'] != "":
        jobstring += "--job-limit " + kwargs['job_limit'] + " "
    if kwargs['time_limit'] != "":
        jobstring += "--time-limit " + kwargs['time_limit'] + " "
    jobstring += kwargs['in_path'] + " " + kwargs['out_path'] + " " + controllerconfigdoc["modname"] + " " + controllerconfigdoc["controllername"] + " " + kwargs['controllerpath'] + " " + " ".join(kwargs['out_file_names'])

    if not os.path.isdir(kwargs['controllerpath'] + "/" + toolname):
        os.mkdir(kwargs['controllerpath'] + "/" + toolname)

    with open(kwargs['controllerpath'] + "/" + toolname + "/" + jobname + ".job", "w") as jobstream:
        jobstream.write(jobstring)
        jobstream.flush()