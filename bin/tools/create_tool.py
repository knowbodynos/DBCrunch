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
from argparse import ArgumentParser, REMAINDER
from crunch_config import *

parser = ArgumentParser()

parser.add_argument('controller_path', help = '')
parser.add_argument('tool', help = '')
parser.add_argument('in_path', help = '')
parser.add_argument('out_path', help = '')
parser.add_argument('--job-limit', dest = 'job_limit', action = 'store', default = "", help = '')
parser.add_argument('--time-limit', dest = 'time_limit', action = 'store', default = "", help = '')
parser.add_argument('--out-files', dest = 'out_file_names', nargs = '+', action = 'store', default = [], help = '')
#parser.add_argument('--node-shift', dest = 'nodeshift', action = 'store', default = None, help = '')

kwargs = vars(parser.parse_known_args()[0])

# Configure controller

config = Config(controller_path = kwargs["controller_path"])

# Import workload manager API

wm_api = __import__("crunch_" + config.cluster.wm.api)

tool_name = kwargs["tool"].split(".")[0]
job_name = "crunch_" + config.module.name + "_" + config.controller.name + "_" + tool_name

with open(config.controller.path + "/status", "w") as status_stream:
    status_stream.write("Controller pending.")
    status_stream.flush()

start_slot = 0
while True:
    slots = wm_api.get_avail_nodes(config.cluster.resources.keys())
    node = slots[start_slot]
    maxtimelimit = wm_api.get_partition_time_limit(node["partition"])
    if unformat_duration(maxtimelimit) < unformat_duration(config.controller.timelimit):
        config.controller.timelimit = maxtimelimit
    wm_api.write_tool_job_file(config, job_name, node, kwargs)
    job_id = wm_api.submit_job(config.controller.path + "/" + tool_name, job_name)
    wm_api.release_held_jobs(config.cluster.user, config.module.name, config.controller.name)
    job_state = wm_api.get_job_state(job_id)
    start_time = time()
    while time() - start_time < 30 and job_state[0] == "PENDING" and job_state[1] == "None":
        sleep(0.1)
        job_state = wm_api.get_job_state(job_id)
    if job_state[0] in ["RUNNING", "COMPLETING", "COMPLETED"] or (job_state[0] == "PENDING" and job_state[1] == "None"):
        break
    else:
        wm_api.cancel_job(job_id)
        os.remove(config.controller.path + "/" + tool_name + "/" + job_name + ".job")
        start_slot += 1
        if start_slot == len(slots):
            start_slot = 0
            sleep(10)

print(job_id)
sys.stdout.flush()