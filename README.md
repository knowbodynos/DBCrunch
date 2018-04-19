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

4) Update the `crunch.config` file in the `${CRUNCH_ROOT}` directory to reflect your cluster's workload manager, partition names and RAM resources, installed software, and custom max job/step information.

------------------------------------------------------------------------------------------------------------

Using `DBCrunch`:

1) Write a new module (script or compiled program) or use an existing one to process the records in your database and place it in a new directory `${CRUNCH_ROOT}/modules/modules/<module>`.

2) If your module is compiled or is written in a scripting language you haven't used before, make sure to enter this information into the `${CRUCH_ROOT}/crunch.config` file under the *software* key.

3) Write a controller configuration template for your module called `<module>.config` and add it to the `${CRUNCH_ROOT}/modules/modules/<module>` directory. A typical job script template looks like:

```
# Options for controller job
controller:
  # Controller name
  name: "template"
  # Controller working directory
  path: "path_to_controller"
  # Storage limit
  storagelimit: "10G"
  # Controller STDOUT and STDERR write mode
  writemode: "append"
  # Controller time limit
  timelimit: "1-00:00:00"
  # Controller buffer time
  buffertime: "00:05:00"
  # Lock down node(s) for controller?
  exclusive: false
  # Requeue job on node failure
  requeue: true

# Options for remote database
db:
  # Database type
  api: "db_mongodb"
  # Database name
  name: "<database_name>"
  # Database host
  host: "<host_ip>"
  # Database port
  port: "<port>"
  # Database username
  username: "<username>"
  # Database password
  password: "<password>"
  # Database writeconcern
  writeconcern: "majority"
  # Database fsync
  fsync: false
  # Database collections
  collections:
    - "<collection>"
  # Database query
  query: 
    <field_1>:
      <value_1>
  # Database projection
  projection:
    <field_1>: 1
    <field_2>: 1
  # Database hint
  hint:
  # Database skip
  skip:
  # Database limit
  limit: 100000
  # Database sort
  sort:
  # Base collection
  basecollection: "<collection>"
  # Field in base collection that determines number of tasks
  nprocsfield: 

# Options for batch jobs
job:
  # Job STDOUT and STDERR write mode
  writemode: "truncate"
  # Requeue job on node failure
  requeue: true
  # Job memory limit
  memorylimit: "5G"
  # Job time limit
  timelimit: 
  # Job buffer time
  buffertime: "00:01:00"
  # Job limits
  jobs:
    max: 20

# Options for module
module:
  # Module name
  name: "<module>"
  # Module language
  language: "<script_langauge>"
  # Arguments to module
  args:

# Other options
options:
  # Block until dependencies are finished
  blocking: false
  # Generate intermediate log file
  intermedlog: true
  # Generate intermediate output files
  intermedlocal: true
  # Generate output log file
  outlog: true
  # Generate output files
  outlocal: true
  # Write output to database
  outdb: true
  # Generate output files for statistics
  statslocal: true
  # Write statistics to database
  statsdb: true
  # Write boolean field (modname)+(markdone) in database and set to true when output is written
  markdone: 
  # Clear completed records from input files after (cleanup) records have been processed
  cleanup: 50
  # When nrefill processors have completed, refill each with niters new documents to process
  nrefill: 5
  # Number of records in each input file to a job step
  niters: 200
  # Number of output records for each worker to write
  nbatch: 10
  # Maximum number of workers writing records simultaneously
  nworkers: 100
```

Make sure to replace everything in `<...>` with your job information.

4) If your module depends on previous modules, add these dependencies to the file `${CRUNCH_ROOT}/modules/modules/<module>/dependencies`, where `<module>` is the name of your module.

5) Create your working directory `<work_dir>` and navigate to it

```
   cd <work_dir>
```

6) Add an empty file `.DBCrunch` to the working directory to designate it as such

```
   touch .DBCrunch
```

7) Copy over your template

```
   crunch template <module> <controller>
```

where `<controller>` is a name you choose for the segment of your database that you wish to process.

8) Navigate to the controller directory

```
   cd <module>/<controller>
```

9) Begin processing

```
   crunch submit <module> <controller>
```

10) Monitor your progress

```
   crunch monitor <s>
```

where `<s>` is the refresh interval. Use `Ctrl-C` to exit.

11) If you need to terminate the process for any reason, use:

```
   crunch cancel <module> <controller>
```

12) If you wish to reset the entire controller directory to initial conditions, use:

```
   crunch reset <module> <controller>
```