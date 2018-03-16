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
from argparse import ArgumentParser, REMAINDER

parser = ArgumentParser()

parser.add_argument('controllerpath', help = '')
parser.add_argument('tool', help = '')
parser.add_argument('in_path', help = '')
parser.add_argument('out_path', help = '')
parser.add_argument('--job-limit', dest = 'job_limit', action = 'store', default = None, help = '')
parser.add_argument('--time-limit', dest = 'time_limit', action = 'store', default = None, help = '')
parser.add_argument('--out-files', dest = 'out_file_names', nargs = '+', action = 'store', default = [], help = '')
parser.add_argument('--node-shift', dest = 'nodeshift', action = 'store', default = None, help = '')

kwargs = vars(parser.parse_known_args()[0])

if kwargs['nodeshift'] == None:
    kwargs['nodeshift'] = 0
else:
    kwargs['nodeshift'] = int(kwargs['nodeshift'])

tool = sys.argv[1]
job_limit = sys.argv[2]
time_limit = sys.argv[3]
in_path = sys.argv[4]
out_path = sys.argv[5]
controllerpath = sys.argv[8]
out_file_names = sys.argv[9:]

toolname, toolext = kwargs['tool'].split('.')

modname, controllername = kwargs['controllerpath'].split("/")[-2:]

rootpath = os.environ['CRUNCH_ROOT']

with open(rootpath + "/crunch.config", "r") as crunchconfigstream:
    crunchconfigdoc = yaml.load(crunchconfigstream)

with open(kwargs['controllerpath'] + "/" + modname + "_" + controllername + ".config", "r") as controllerconfigstream:
    controllerconfigdoc = yaml.load(controllerconfigstream)["controller"]

jobname = "crunch_" + controllerconfigdoc["modname"] + "_" + controllerconfigdoc["controllername"] + "_" + toolname

if crunchconfigdoc["workload-manager"] == "slurm":
    from crunch_slurm import *
    get_writetooljobfile(crunchconfigdoc, controllerconfigdoc, jobname, toolname, kwargs)
    get_submitjob(kwargs['controllerpath'] + "/" + toolname, jobname)