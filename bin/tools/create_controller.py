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
from time import sleep

rootpath = os.environ['CRUNCH_ROOT']
username = os.environ['USER']

controllerpath = sys.argv[1]

modname, controllername = controllerpath.split("/")[-2:]

with open(rootpath + "/crunch.config", "r") as crunchconfigstream:
    crunchconfigdoc = yaml.load(crunchconfigstream)

with open(controllerpath + "/" + modname + "_" + controllername + ".config", "r") as controllerconfigstream:
    controllerconfigdoc = yaml.load(controllerconfigstream)["controller"]

jobname = "crunch_" + controllerconfigdoc["modname"] + "_" + controllerconfigdoc["controllername"] + "_controller"

with open(controllerpath + "/status", "w") as statusstream:
    statusstream.write("Controller pending.")
    statusstream.flush()

if crunchconfigdoc["workload-manager"] == "slurm":
    from crunch_slurm import *
    nodenum = 0
    while True:
        freenodes = get_freenodes(controllerconfigdoc['partitions'])
        node = freenodes[nodenum]
        get_writecontrollerjobfile(crunchconfigdoc, controllerconfigdoc, jobname, node)
        jobid = get_submitjob(controllerpath, jobname)
        get_releaseheldjobs(username, controllerconfigdoc['modname'], controllerconfigdoc['controllername'])
        jobstate = get_job_state(jobid)
        sleeptime = 0
        while sleeptime < 30 and jobstate[0] == "PENDING" and jobstate[1] == "None":
            sleep(1)
            jobstate = get_job_state(jobid)
            sleeptime += 1

        if jobstate[0] == "RUNNING":
            break
        elif jobstate[0] == "PENDING":
            #if jobstate[1] == "Resources":
            #    get_canceljob(jobid)
            #else:
            get_canceljob(jobid)
            nodenum += 1
            if nodenum == len(freenodes):
                nodenum = 0
                sleep(10)

print(jobid)
sys.stdout.flush()