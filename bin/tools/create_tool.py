#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

#    DBCrunch: create_controller.py
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

import sys, os, yaml

def writetooljobfile(tool, toolname, job_limit, time_limit, in_path, out_path, modname, controllername, controllerpath, out_file_names, partition):
    jobname = "crunch_" + modname + "_" + controllername + "_" + toolname
    jobstring = "#!/bin/bash\n"
    jobstring += "\n"
    jobstring += "#Job name\n"
    jobstring += "#SBATCH -J \"" + jobname + "\"\n"
    jobstring += "#################\n"
    jobstring += "#Working directory\n"
    jobstring += "#SBATCH -D \"" + controllerpath + "/" + toolname + "\"\n"
    jobstring += "#################\n"
    jobstring += "#Job output file\n"
    jobstring += "#SBATCH -o \"" + jobname + ".info\"\n"
    jobstring += "#################\n"
    jobstring += "#Job error file\n"
    jobstring += "#SBATCH -e \"" + jobname + ".err\"\n"
    jobstring += "#################\n"
    jobstring += "#Job file write mode\n"
    jobstring += "#SBATCH --open-mode=\"truncate\"\n"
    jobstring += "#################\n"
    jobstring += "#Job max time\n"
    jobstring += "#SBATCH --time=\"1-00:00:00\"\n"
    jobstring += "#################\n"
    jobstring += "#Partition(s) to use for job\n"
    jobstring += "#SBATCH --partition=\"" + partition + "\"\n"
    jobstring += "#################\n"
    jobstring += "#Number of nodes to distribute n tasks across\n"
    jobstring += "#SBATCH -N 1\n"
    jobstring += "#################\n"
    jobstring += "#Lock down node\n"
    jobstring += "#SBATCH --exclusive\n"
    jobstring += "#################\n"
    jobstring += "#Requeue job on node failure\n"
    jobstring += "#SBATCH --requeue\n"
    jobstring += "#################\n"
    jobstring += "\n"
    jobstring += "python ${CRUNCH_ROOT}/bin/tools/" + tool + " "
    if job_limit != "":
        jobstring += "--job-limit " + job_limit + " "
    if time_limit != "":
        jobstring += "--time-limit " + time_limit + " "
    jobstring += in_path + " " + out_path + " " + modname + " " + controllername + " " + controllerpath + " " + " ".join(out_file_names)

    if not os.path.isdir(controllerpath + "/" + toolname):
        os.mkdir(controllerpath + "/" + toolname)

    with open(controllerpath + "/" + toolname + "/" + jobname + ".job", "w") as jobstream:
        jobstream.write(jobstring)
        jobstream.flush()

tool = sys.argv[1]
job_limit = sys.argv[2]
time_limit = sys.argv[3]
in_path = sys.argv[4]
out_path = sys.argv[5]
modname = sys.argv[6]
controllername = sys.argv[7]
controllerpath = sys.argv[8]
out_file_names = sys.argv[9:]

toolname, toolext = tool.split('.')

rootpath = os.environ['CRUNCH_ROOT']

with open(rootpath + "/crunch.config", "r") as crunchconfigstream:
    crunchconfigdoc = yaml.load(crunchconfigstream)

partitions = crunchconfigdoc["resources"].keys()

if crunchconfigdoc["workload-manager"] == "slurm":
    from crunch_slurm import *
    partition = get_freenodes(partitions)[0]["partition"]
    writetooljobfile(tool, toolname, job_limit, time_limit, in_path, out_path, modname, controllername, controllerpath, out_file_names, partition)
    get_submitjob(controllerpath + "/" + toolname, "crunch_" + modname + "_" + controllername + "_" + toolname)