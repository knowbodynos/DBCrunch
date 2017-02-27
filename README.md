# SLURMongo
This package links the SLURM workload manager with MongoDB, in order to optimize the staged processing of large amounts of data in parallel.
The data is streamed directly from a remote MongoDB database, processed on a high-performance computing cluster running SLURM, and fed directly back to the remote database along with statistics such as CPU time, max memory used, and storage.

------------------------------------------------------------------------------------------------------------

Installation instructions for the Massachusetts Green High Performance Computing Center's Discovery cluster:

1) Add the following lines to `${HOME}/.bashrc`:

```
   module load gnu-4.4-compilers 
   module load fftw-3.3.3
   module load platform-mpi
   module load perl-5.20.0
   module load slurm-14.11.8
   module load gnu-4.8.1-compilers
   module load boost-1.55.0
   module load python-2.7.5

   export SAGE_ROOT=/shared/apps/sage/sage-5.12
   export SLURMONGO_ROOT=/gss_gpfs_scratch/${USER}/SLURMongo
```

2) Restart your Discovery session OR run the command `source ${HOME}/.bashrc`.

3) Install Mathematica and Python components by running the command `${SLURMMONGO_ROOT}/install.bash` from a login node.

4) Modify `${SLURMONGO_ROOT}/state/mongouri` and enter the IP address and port of your remote MongoDB database, as well as your username and password in the appropriate URI fields.

5) Navigate to `${SLURMONGO_ROOT}/templates` and choose a `controller_(some_module_name)_template.job` template for some module. For testing purposes, find the lines defining the variables dbpush and markdone. Change them to:

```
   dbpush="False"
   markdone=""
```

   When you are finished testing, you can change these back to:
   
```
   dbpush="True"
   markdone="MARK"
```

6) Run the following command:
   
   `${SLURMONGO_ROOT}/scripts/tools/copy_template.bash (some_module_name) (some_controller_name)`
   
   (Note: If you choose to copy the template manually, you will also have to expand `${SLURMONGO_ROOT}` inside the `#SBATCH -D` keyword of the `controller_(some_module_name)_template.job` file in order for SLURM to be able to process it.)

7) Navigate to `${SLURMONGO_ROOT}/modules/(some_module_name)/(some_controller_name)`. You can now submit the job using the command:

   `sbatch controller_(some_module_name)_(some_controller_name).job`
   
8) If you need to cancel the controller job for any reason, make sure you run the command:

   `./reset.bash`
   
   before resubmitting the job.
   
------------------------------------------------------------------------------------------------------------

Some useful aliases to keep in the `${HOME}/.bashrc` file:

```
#Functions
scancelgrep() {
    nums=$(squeue -h -u ${USER} -o "%.100P %.100j %.100i %.100t %.100T" | grep $1 | sed "s/\s\s\s*//
g" | cut -d" " -f1);
    for n in $nums;
    do
        scancel $n;
    done
}

sfindpart() {
    greppartitions="ser-par-10g|ser-par-10g-2|ser-par-10g-3|ser-par-10g-4|ht-10g|interactive-10g"

    partitionsidle=$(sinfo -h -o '%t %c %D %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'idle' 
| awk '$0=$1" "$2*$3" "$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1
)
    partitionscomp=$(sinfo -h -o '%t %c %D %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'comp' 
| awk '$0=$1" "$2*$3" "$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1
)
    partitionsrun=$(squeue -h -o '%L %T %P' | grep -E "(${greppartitions})\*?\s*$" | grep 'RUNNING' 
| sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | 
sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ' ' | 
head -c -1)
    partitionspend=$(sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E "(${greppar
titions})\*?\s*$" | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | gre
p 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ' ' | head -c -1)

    read -r -a orderedpartitions <<< "${partitionsidle} ${partitionscomp} ${partitionsrun} ${partiti
onspend}"

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

#Aliases
alias sage='source /shared/apps/sage/sage-5.12/sage'
alias lsx='watch -n 5 "ls"'
alias sjob='squeue -u ${USER} -o "%.10i %.13P %.30j %.8u %.2t %.10M %.6D %R"'
alias djob='jobids=$(squeue -h -u ${USER} | grep -v "(null)" | sed "s/\s\s\s*//g" | cut -d" " -f1); 
for job in $jobids; do scancel $job; echo "Cancelled Job $job."; done'
alias scancelgrep=scancelgrep
alias sfindpart=sfindpart
alias sinteract='function _sinteract(){ jobnum=$(echo $(salloc --no-shell -N 1 --exclusive -p $1 2>&
1) | sed "s/.* allocation \([0-9]*\).*/\1/g"); ssh -X $(squeue -h -u ${USER} -j $jobnum -o %.100N | 
sed "s/\s\s\s*/ /g" | rev | cut -d" " -f1 | rev); scancel $jobnum; };_sinteract'
alias swatch='function _swatch(){ watch -n$1 "squeue -u ${USER} -o \"%.10i %.13P %.30j %.8u %.2t %.1
0M %.6D %R\" -S \"P,-t,-p\""; };_swatch'
alias siwatch='function _siwatch(){ jobnum=$(echo $(salloc --no-shell -N 1 --exclusive -p $1 2>&1) |
 sed "s/.* allocation \([0-9]*\).*/\1/g"); ssh -t -X $(squeue -h -u ${USER} -j $jobnum -o %.100N | s
ed "s/\s\s\s*/ /g" | rev | cut -d" " -f1 | rev) "watch -n$2 \"squeue -u ${USER}\""; scancel $jobnum;
 };_siwatch'
alias scratch='cd /gss_gpfs_scratch/${USER}'
alias quickclear='perl -e "for(<*>){((stat)[9]<(unlink))}"'
```
