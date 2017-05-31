# SLURMongo
This package in an API linking the SLURM workload manager with MongoDB, in order to optimize the staged, parallel processing of large amounts of data.
The data is streamed directly from a remote MongoDB database, processed on a high-performance computing cluster running SLURM, and fed directly back to the remote database along with statistics such as CPU time, max memory used, and storage.

------------------------------------------------------------------------------------------------------------

Installation instructions for the Massachusetts Green High Performance Computing Center's Discovery cluster:

1) Make sure that the directory `/gss_gpfs_scratch/${USER}` exists. If not, then create it using:

```
   mkdir /gss_gpfs_scratch/${USER}
```

2) Add the following lines to `${HOME}/.bashrc`:

```
   module load gnu-4.4-compilers 
   module load fftw-3.3.3
   module load platform-mpi
   module load perl-5.20.0
   module load slurm-14.11.8
   module load gnu-4.8.1-compilers
   module load boost-1.55.0
   module load python-2.7.5
   module load mathematica-10

   export SAGE_ROOT=/shared/apps/sage/sage-5.12
   export SLURMONGO_ROOT=/gss_gpfs_scratch/${USER}/SLURMongo
```

3) Restart your Discovery session OR run the command `source ${HOME}/.bashrc`.

4) Activate Mathematica 10.0.2 by performing the following steps:
    
   a) SSH into Discovery using the "-X" flag (i.e. `ssh -X (your_username)@discovery2.neu.edu`)

   b) Allocate a node on some partition (e.g. `interactive-10g`) for an interactive job using the command:

   ```
      salloc --no-shell -N 1 --exclusive -p (some_partition)
   ```

   After a moment, you will see the message *salloc: Granted job allocation (some_JOBID)*.

   c) Enter the command:

   ```
      squeue -u ${USER} -o "%.100N" -j (some_JOBID)
   ```

   In the column labeled *NODELIST*, you will see the hostname of your allocated interactive job.

   d) Login to this host with the command:
    
   ```
      ssh -X (some_hostname)
   ```

   e) Launch the Mathematica 10.0 GUI using the command:

   ```
      mathematica &
   ```
   
      You will see an "*Activate online*" window. Select the "*Other ways to activate*" button. Then select the third option "*Connect to a network license server*". In the Server name box enter `discovery2` and then select the "*Activate*" button. After this check the "*I accept the terms of the agreement*" box and select the "*OK*" button. Now your user account is configured to use Mathematica 10.0.2 on the Discovery Cluster.

5) Install Mathematica and Python components by running the command `${SLURMONGO_ROOT}/install.bash` from a login node.

6) Modify `${SLURMONGO_ROOT}/state/mongouri` by entering the IP address and port of your remote MongoDB database, as well as your username and password in the appropriate URI fields. The format should look like:

```
   mongodb://(username):(password)@(IP address):(port)/(MongoDB database name)
```

7) View the available modules using `ls ${SLURMONGO_ROOT}/templates` and choose the module (i.e., *controller_(some_module_name)_template*) that you wish to run.

   For testing purposes, please modify `controller_(some_module_name)_template.job` by finding the lines defining the variables *dbpush* and *markdone*. Change them to:

```
   dbpush="False"
   markdone=""
```

   This prevents the controller from writing the results back to the remote MongoDB database. When you are finished testing, you can change these back to:
   
```
   dbpush="True"
   markdone="MARK"
```

8) To copy the template to a usable format, run the following command:
   
```
   ${SLURMONGO_ROOT}/scripts/tools/copy_template.bash (some_module_name) (some_controller_name)
```
   
   where, in practice, *(some_controller_name)* is the value of H11 (i.e., 1 through 6) that we wish to run the module on.

   *(Note: If you choose to copy the template manually, you will also have to expand `${SLURMONGO_ROOT}` inside the `#SBATCH -D` keyword of the `controller_(some_module_name)_template.job` file in order for SLURM to be able to process it.)*

9) The template has now been copied to the directory `${SLURMONGO_ROOT}/modules/(some_module_name)/(some_controller_name)`. Navigate here, and you can now submit the job using the command:

```
   sbatch controller_(some_module_name)_(some_controller_name).job
```
   
10) If you need to cancel the controller job for any reason, make sure you run the command:

```
   ./reset.bash
```
   
   before resubmitting the job.
   
------------------------------------------------------------------------------------------------------------

Some useful aliases to keep in the `${HOME}/.bashrc` file:

```
#Functions
_scancelgrep() {
    nums=$(squeue -h -u ${USER} -o "%.100P %.100j %.100i %.100t %.100T" | grep $1 | sed "s/\s\s\s*//g" | cut -d" " -f1)
    for n in $nums
    do
        scancel $n
    done
}

_sfindpart() {
    greppartitions="ser-par-10g|ser-par-10g-2|ser-par-10g-3|ser-par-10g-4|ht-10g|interactive-10g"

    partitionsidle=$(sinfo -h -o '%t %c %D %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'idle' | awk '$0=$1" "$2*$3" "$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1)
    partitionscomp=$(sinfo -h -o '%t %c %D %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'comp' | awk '$0=$1" "$2*$3" "$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1)
    read -r -a orderedpartitions <<< "${partitionsidle} ${partitionscomp}"

    for i in "${!orderedpartitions[@]}"
    do
        flag=true
        for partition in "${orderedpartitions[@]::$i}"
        do
            [[ "$partition" == "${orderedpartitions[$i]}" ]] && flag=false
        done
        if [ "$flag" = true ]
        then
            echo ${orderedpartitions[$i]}
        fi
    done
}

_sfindpartall() {
    greppartitions="ser-par-10g|ser-par-10g-2|ser-par-10g-3|ser-par-10g-4|ht-10g|interactive-10g"

    partitionsidle=$(sinfo -h -o '%t %c %D %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'idle' | awk '$0=$1" "$2*$3" "$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1)
    partitionscomp=$(sinfo -h -o '%t %c %D %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'comp' | awk '$0=$1" "$2*$3" "$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1)
    partitionsrun=$(squeue -h -o '%L %T %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'RUNNING' | sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ' ' | head -c -1)
    partitionspend=$(sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | grep 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1)

    read -r -a orderedpartitions <<< "${partitionsidle} ${partitionscomp} ${partitionsrun} ${partitionspend}"

    for i in "${!orderedpartitions[@]}"
    do
        flag=true
        for partition in "${orderedpartitions[@]::$i}"
        do
            [[ "$partition" == "${orderedpartitions[$i]}" ]] && flag=false
        done
        if [ "$flag" = true ]
        then
            echo ${orderedpartitions[$i]}
        fi
    done
}

_sinteract(){
    jobnum=$(echo $(salloc --no-shell -N 1 --exclusive -p $1 2>&1) | sed "s/.* allocation \([0-9]*\).*/\1/g"); ssh -X $(squeue -h -u ${USER} -j $jobnum -o %.100N | sed "s/\s\s\s*/ /g" | rev | cut -d" " -f1 | rev); scancel $jobnum;
}

_watchjobs() {
    jobs=$(squeue -u ${USER} -o "%.10i %.13P %.130j %.8u %.2t %.10M %.6D %R" -S "P,-t,-p" | tr '\n' '!' 2>/dev/null)
    njobs=$(echo ${jobs} | tr '!' '\n' 2>/dev/null | grep "_steps_" | wc -l)
    if [ "${njobs}" -gt 0 ]
    then
        nrunjobs=$(echo ${jobs} | tr '!' '\n' 2>/dev/null | grep "_steps_" | grep " R " | wc -l)
        npendjobs=$(echo ${jobs} | tr '!' '\n' 2>/dev/null | grep "_steps_" | grep " PD " | wc -l)
        steps=$(sacct -o "JobID%30,JobName%130,State" --jobs=$(echo ${jobs} | tr '!' '\n' 2>/dev/null | grep "_steps_" | sed "s/\s\s*/ /g" | cut -d" " -f2 | tr '\n' ',' | head -c -1) 2>/dev/null | grep -v "stats_" | grep -E "\."  | tr '\n' '!' 2>/dev/null)
        nrunsteps=$(echo ${steps} | tr '!' '\n' 2>/dev/null | grep "RUNNING" | wc -l)
        pendsteps=$(echo ${jobs} | tr '!' '\n' 2>/dev/null | grep "_steps_" | grep " PD " | sed "s/\s\s*/ /g" | cut -d" " -f4 | rev | cut -d"_" -f1 | rev | sed 's/^\(.*\)$/\1-1/g' | tr "\n" "+" | head -c -1)
        if [[ "${pendsteps}" == "" ]]
        then
            npendsteps=0
        else
            npendsteps=$(echo "-(${pendsteps})" | bc)
        fi
    else
        nrunjobs=0
        npendjobs=0
        nrunsteps=0
        npendsteps=0
    fi
    nsteps=$(($nrunsteps+$npendsteps))
    echo "# Jobs: ${njobs}   # Run Jobs: ${nrunjobs}   # Pend Jobs: ${npendjobs}"
    echo "# Steps: ${nsteps}   # Run Steps: ${nrunsteps}   # Pend Steps: ${npendsteps}"
    echo ""
    #squeue -u ${USER} -o "%.10i %.13P %.130j %.8u %.2t %.10M %.6D %R" -S "P,-t,-p"
    echo "${jobs}" | tr '!' '\n' 2>/dev/null
}
export -f _watchjobs

_swatch() {
    watch -n$1 bash -c "_watchjobs"
}

_siwatch(){
    jobnum=$(echo $(salloc --no-shell -N 1 --exclusive -p $1 2>&1) | sed "s/.* allocation \([0-9]*\).*/\1/g");
    ssh -t -X $(squeue -h -u ${USER} -j $jobnum -o %.100N | sed "s/\s\s\s*/ /g" | rev | cut -d" " -f1 | rev) "watch -n$2 bash -c \"_watchjobs\""
    scancel $jobnum
}

_step2job(){
    jobstepname=$1
    modname=$2
    controllername=$3

    herepath=$(pwd)
    if [[ "${modname}" == "" ]] || [[ "${controllername}" == "" ]]
    then
        if [[ "${herepath}" == "${SLURMONGO_ROOT}/modules/"* ]]
        then
            read modname controllername <<<$(echo "${herepath/${SLURMONGO_ROOT}\/modules\//}" | tr '/' ' ' | cut -d' ' -f1,2)
            if [[ "${modname}" != "" ]] && [[ "${controllername}" != "" ]]
            then
                parentids=$(cat "${SLURMONGO_ROOT}/modules/${modname}/${controllername}/controller_${modname}_${controllername}.out" | grep "${jobstepname}" | perl -pe 's/^.*job step (.*?)\..*$/\1/g' | tr '\n' '|' | head -c -1)
                cat "${SLURMONGO_ROOT}/modules/${modname}/${controllername}/controller_${modname}_${controllername}.out" | grep -E "Submitted batch job (${parentids})" | perl -pe 's/^.* as (.*?) on.*$/\1/g'
            fi
        fi
    else
        parentids=$(cat "${SLURMONGO_ROOT}/modules/${modname}/${controllername}/controller_${modname}_${controllername}.out" | grep "${jobstepname}" | perl -pe 's/^.*job step (.*?)\..*$/\1/g' | tr '\n' '|' | head -c -1)
        cat "${SLURMONGO_ROOT}/modules/${modname}/${controllername}/controller_${modname}_${controllername}.out" | grep -E "Submitted batch job (${parentids})" | perl -pe 's/^.* as (.*?) on.*$/\1/g'
    fi
}

#Aliases
alias sage='source /shared/apps/sage/sage-5.12/sage'
alias lsx='watch -n 5 "ls"'
alias sjob='squeue -u ${USER} -o "%.10i %.13P %.130j %.8u %.2t %.10M %.6D %R" -S "P,-t,-p"'
alias djob='jobids=$(squeue -h -u ${USER} | grep -v "(null)" | sed "s/\s\s\s*//g" | cut -d" " -f1); for job in $jobids; do scancel $job; echo "Cancelled Job $job."; done'
alias scancelgrep='_scancelgrep'
alias sfindpart='_sfindpart'
alias sfindpartall='_sfindpartall'
alias sinteract='_sinteract'
alias swatch='_swatch'
alias siwatch='_siwatch'
alias scratch='cd /gss_gpfs_scratch/${USER}'
alias quickclear='perl -e "for(<*>){((stat)[9]<(unlink))}"'
alias step2job='_step2job'
alias statreset='cat *.stat | sed "s/False/True/g" >> ../skippedstate'
```
