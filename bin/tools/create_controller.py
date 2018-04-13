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

import sys
from time import sleep
from crunch_config import *

controller_path = sys.argv[1]

# Configure controller

config = Config(controller_path = controller_path)

# Import workload manager API

wm_api = __import__(config.cluster.wm.api)

job_name = "crunch_" + config.module.name + "_" + config.controller.name + "_controller"

with open(config.controller.path + "/status", "w") as status_stream:
    status_stream.write("Controller pending.")
    status_stream.flush()

start_slot = 0
while True:
    nodes = wm_api.get_avail_nodes(config.cluster.resources.keys())
    node = nodes[start_slot]
    maxtimelimit = wm_api.get_partition_time_limit(node["partition"])
    if unformat_duration(maxtimelimit) < unformat_duration(config.controller.timelimit):
        config.controller.timelimit = maxtimelimit
    wm_api.write_controller_job_file(config, job_name, node)
    job_id = wm_api.submit_job(config.controller.path, job_name)
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
        os.remove(config.controller.path + "/" + job_name + ".job")
        start_slot += 1
        if start_slot == len(slots):
            start_slot = 0
            sleep(10)

print(job_id)
sys.stdout.flush()