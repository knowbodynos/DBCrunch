# DBCrunch
A package facilitating the staged, parallel processing of large amounts of data stored. `DBCrunch` is unique in that it optimizes performance on a shared/public High-Performance Computing cluster with data stored in a remote database.

In the current version, the local HPC cluster is assumed to be running the `SLURM` workload manager, and the remote data is stored in a `MongoDB` database.

The data is streamed directly from the remote database, processed on the local HPC cluster, and fed directly back to the remote database along with statistics such as CPU time, max memory used, and storage.

------------------------------------------------------------------------------------------------------------

Installation instructions:

1) Download the `DBCrunch` package

```
   git clone https://github.com/knowbodynos/DBCrunch.git
```

2) Navigate into the main directory

```
   cd DBCrunch
```

3) Install `DBCrunch` (optional arguments `--USER_LOCAL` and `--CRUNCH_ROOT`)

```
   ./install [--USER_LOCAL ~/opt] [--CRUNCH_ROOT .]
```

------------------------------------------------------------------------------------------------------------

Using `DBCrunch`:

1) Write a new module (script or compiled program) or use an existing one to process the records in your database and place it in the `${CRUNCH_ROOT}/modules/scripts` directory.

2) If your module is compiled or is written in a scripting language you haven't used before, make sure to enter this information into the `${CRUCH_ROOT}/state/software` file. You must enter the following information, for example:

```
   Script Language,License Required,Extension,Command,Flags
   bash,False,.bash,,
   python,False,.py,python,
   my_program,False,,,
```

3) Write a job script template for your module and add it to the `${CRUNCH_ROOT}/modules/templates` directory. A typical job script template looks like:

```
   #!/bin/bash
   
   #Job name
   #SBATCH -J "controller_<module>_template"
   #################
   #Working directory
   #SBATCH -D "path_to_module/<module>/template"
   #################
   #Job output file
   #SBATCH -o "controller_<module>_template.out"
   #################
   #Job error file
   #SBATCH -e "controller_<module>_template.err"
   #################
   #Job file write mode
   #SBATCH --open-mode="append"
   #################
   #Job max time
   #SBATCH --time="1-00:00:00"
   #################
   #Partition (queue) to use for job
   #SBATCH --partition="ser-par-10g"
   #################
   #Number of tasks (CPUs) allocated for job
   #SBATCH -n 1
   #################
   #Number of nodes to distribute n tasks across
   #SBATCH -N 1
   #################
   #Lock down N nodes for job
   #SBATCH --exclusive
   #################
    
   #Input controller info
   modname="<module>"
   controllername="template"
   controllerjobid="${SLURM_JOBID}"
   controllerbuffertime="00:05:00"
   storagelimit="10G"
   sleeptime="1"
    
   #Input script info
   scriptlanguage="<module_language>"
   partitions="ser-par-10g,ser-par-10g-2,ser-par-10g-3,ser-par-10g-4"
   writemode="truncate"
   scriptmemorylimit="500000000"
   scripttimelimit=""
   scriptbuffertime="00:01:00"
   joblimit=""
    
   #Input database info
   dbtype="mongodb"
   dbusername="<db_username>"
   dbpassword="<db_password>"
   dbhost="<db_host>"
   dbport="<db_port>"
   dbname="<db_name>"
   queries="<mongolink_query>"
   basecollection="<db_collection>"
   nthreadsfield=""
    
   #Options
   blocking="False"
   logging="True"
   templocal="True"
   writelocal="True"
   writedb="True"
   statslocal="True"
   statsdb="True"
   markdone="MARK"
   cleanup="100"
   niters="200"
   nbatch="5"
   nworkers="2"
    
   python ${CRUNCH_ROOT}/bin/controller.py "${modname}" "${controllername}" "${controllerjobid}" "${controllerbuffertime}" "${storagelimit}" "${sleeptime}" "${scriptlanguage}" "${partitions}" "${writemode}" "${scriptmemorylimit}" "${scripttimelimit}" "${scriptbuffertime}" "${joblimit}" "${dbtype}" "${dbusername}" "${dbpassword}" "${dbhost}" "${dbport}" "${dbname}" "${queries}" "${basecollection}" "${nthreadsfield}" "${blocking}" "${logging}" "${templocal}" "${writelocal}" "${writedb}" "${statslocal}" "${statsdb}" "${markdone}" "${cleanup}" "${niters}" "${nbatch}" "${nworkers}"
```

Make sure to replace everything in `<...>` with your job information.

4) If your module depends on previous modules, add these dependencies to the file `${CRUNCH_ROOT}/modules/dependencies/<module>`, where `<module>` is the name of your module.

5) Create your working directory `<dir>` and navigate to it

```
   cd <dir>
```

6) Copy over your template

```
   crunch template <module> <controller>
```

where `<controller>` is a name you choose for the segment of your database that you wish to process.

7) Navigate to the controller directory

```
   cd <module>/<controller>
```

8) Begin processing

```
   crunch
```

9) Monitor your progress

```
   crunch monitor <s>
```

where `<s>` is the refresh interval. Use `Ctrl-C` to exit.

10) If you need to terminate the process for any reason, use:

```
   crunch cancel <module> <controller>
```

11) In order to requeue jobs that may have failed, use:

```
   crunch requeue <module> <controller>
```