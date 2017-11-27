from signal import signal,SIGPIPE,SIG_DFL;
from subprocess import Popen,PIPE;

def default_sigpipe():
    signal(SIGPIPE,SIG_DFL);

def get_maxtimelimit(partition):
    return Popen("sinfo -h -o '%l %P' | grep -E '"+partition+"\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];

def get_pendjobnamespaths(username):
    return [x.split() for x in Popen("squeue -h -u "+username+" -o '%T %j %.130Z' | grep 'crunch_' | grep 'PENDING' | cut -d' ' -f2,3 | grep -v '_controller' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",")];

def get_nglobaljobs(username):
    return eval(Popen("squeue -h -r -u "+username+" -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);

def get_nlocaljobs(username,modname,controllername):
    return eval(Popen("squeue -h -r -u "+username+" -o '%.130j %.2t' | grep 'crunch_' | grep -v ' CG ' | grep 'crunch_"+modname+"_"+controllername+"_job_' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);

def controllerjobsrunningq(username,modname,controllername):
    return eval(Popen("squeue -h -u "+username+" -o '%j' | grep 'crunch_"+modname+"_"+controllername+"_' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0])>0;

def prevcontrollerjobsrunningq(username,dependencies):
    if len(dependencies)==0:
        njobsrunning=0;
    else:
        grepstr="\|".join(dependencies);
        njobsrunning=eval(Popen("squeue -h -u "+username+" -o '%j' | grep 'crunch_\("+grepstr+"\)_' | wc -l | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);
    return njobsrunning>0;

def get_submitjob(jobpath,jobname):
    return Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].rstrip("\n").split()[-1];

def releaseheldjobs(username,modname,controllername):
    Popen("for job in $(squeue -h -u "+username+" -o '%j %A %r' | grep 'crunch_"+modname+"_"+controllername+"_' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done",shell=True,preexec_fn=default_sigpipe);

def get_partitionsidle(partitions):
    greppartitions="|".join(partitions);
    return Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');

def get_partitionscomp(partitions):
    greppartitions="|".join(partitions);
    return Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'comp' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');

def get_partitionsrun(partitions):
    greppartitions="|".join(partitions);
    return Popen("squeue -h -o '%L %T %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'RUNNING' | sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');

def get_partitionspend(partitions):
    greppartitions="|".join(partitions);
    return Popen("sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | grep 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');

def get_partitionncorespernode(partition):
    return eval(Popen("sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);

def get_maxnnodes(partition):
    return eval(Popen("scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].rstrip("\n"));

def get_maxjobcount():
    return eval(Popen("scontrol show config | grep 'MaxJobCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);

def get_maxstepcount():
    return eval(Popen("scontrol show config | grep 'MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0]);

def get_controllerstats(controllerjobid):
    return Popen("sacct -n -j "+controllerjobid+" -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");

def get_controllerpath(controllerjobid):
    return Popen("squeue -h -j "+controllerjobid+" -o '%Z' | head -c -1",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];

def get_exitcode(jobid):
    return Popen("sacct -n -j "+jobid+" -o 'ExitCode' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2",shell=True,stdout=PIPE,preexec_fn=default_sigpipe).communicate()[0];