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

rootpath = os.environ['CRUNCH_ROOT']

controllerpath = sys.argv[1]
nodeshift = sys.argv[2:]

if len(nodeshift) == 0:
	nodeshift = 0
else:
	nodeshift = int(nodeshift[0])

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
    get_writecontrollerjobfile(crunchconfigdoc, controllerconfigdoc, jobname, nodeshift = nodeshift)
    #get_submitjob(controllerpath, jobname)