#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

#    DBCrunch: analyze.py
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

import sys, datetime, matplotlib, warnings, yaml
from argparse import ArgumentParser
from pytz import utc
from glob import iglob
import numpy as np
from scipy.stats import norm#, poisson
import matplotlib.mlab as mlab
matplotlib.use('Agg')
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import MultipleLocator, FuncFormatter
from matplotlib.gridspec import GridSpec
plt.ioff()

def timestamp2unit(timestamp, unit = "seconds"):
    if timestamp.isdigit():
        seconds = int(timestamp)
    else:
        days = 0
        if "-" in timestamp:
            daysstr, timestamp = timestamp.split("-")
            days = int(daysstr)
        hours, minutes, seconds = [int(x) for x in timestamp.split(":")]
        hours += days * 24
        minutes += hours * 60
        seconds += minutes * 60
    if unit == "seconds":
        return seconds
    elif unit == "minutes":
        return float(seconds) / 60.
    elif unit == "hours":
        return float(seconds) / (60. * 60.)
    elif unit == "days":
        return float(seconds) / (60. * 60. * 24.)
    else:
        return 0

def seconds2timestamp(timelevel, seconds):
    timestamp = ""
    days = str(seconds / (60 * 60 * 24)).zfill(2)
    remainder = seconds % (60 * 60 * 24)
    hours = str(remainder / (60 * 60)).zfill(2)
    remainder = remainder % (60 * 60)
    minutes = str(remainder / 60).zfill(2)
    remainder = remainder % 60
    seconds = str(remainder).zfill(2)
    #if days != "0":
    #    timestamp += days + "-"
    #timestamp += hours + ":" + minutes# + ":" + seconds
    if timelevel == 2:
        timestamp += days + ":" + hours
    elif timelevel == 1:
        timestamp += hours + ":" + minutes
    else:
        timestamp += minutes + ":" + seconds
    return timestamp

def create_y_ticker(yscale, yexp):
    return FuncFormatter(lambda y, pos: '%.1f' % (float(y) / (yscale * (10 ** yexp))))

def Freedman_Diaconis_binwidth(data, axis = None):
    """
    Freedman Diaconis rule using IQR for binwidth
    Considered a variation of the Scott rule with more robustness as the Interquartile range
    is less affected by outliers than the standard deviation.
    
    If the IQR is 0, we return the median absolute deviation as defined above, else 1
    """
    iqr = np.subtract(*np.percentile(data, [75, 25]))
    
    if iqr == 0: #unlikely
        iqr = np.median(np.absolute(data - np.median(data, axis)), axis) # replace with something useful
    
    binwidth = (2 * iqr * (data.size ** (-1.0 / 3))) # binwidth

    return max(int(binwidth), 1)
    
    #if iqr > 0:
    #    return np.ceil(data.ptp() / binwidth)

    #return 1 #all else fails

parser = ArgumentParser()

parser.add_argument('in_path', help = '')
parser.add_argument('out_path', help = '')
parser.add_argument('modname', help = '')
parser.add_argument('controllername', help = '')
parser.add_argument('controllerpath', help = '')
parser.add_argument('out_file_name', help = '')
parser.add_argument('--job-limit', '-j', dest = 'job_limit', action = 'store', default = None, help = '')
parser.add_argument('--time-limit', '-t', dest = 'time_limit', action = 'store', default = None, help = '')

args = vars(parser.parse_known_args()[0])

if args['job_limit'] != None:
    args['job_limit'] = int(args['job_limit'])
if args['time_limit'] != None:
    args['time_limit'] = timestamp2unit(args['time_limit'])

with open(args['controllerpath'] +  "/" + args["modname"] + "_" + args["controllername"] + ".config", "r") as controllerconfigstream:
    controllerconfigdoc = yaml.load(controllerconfigstream)

with open(args['controllerpath'] + "/crunch_" + args['modname'] + "_" + args['controllername'] + "_controller.info", "r") as controller_stream:
    controller_line_split = controller_stream.readline().rstrip("\n").split()
    epoch = datetime.datetime.strptime(" ".join(controller_line_split[:4]), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)

#epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

#id_times = {}

job_submit_start_end = {}
step_submit_start_end = {}

#min_time = None
#max_time = 0

intermed = np.zeros(0, dtype = int)
intermed_done = np.zeros(0, dtype = int)
out = np.zeros(0, dtype = int)
out_done = np.zeros(0, dtype = int)
dir_size = np.zeros(0, dtype = int)

print("Analyzing job activity statistics...")
sys.stdout.flush()

#for log_file_path in iglob(args['in_path'] + "/*.log.intermed"):
#    log_filename = log_file_path.split("/")[-1]
#    log_job = int(log_filename.split("_job_")[1].split("_")[0])
#    log_step = int(log_filename.split("_step_")[1].split(".")[0])
#    log_state = '.'.join(log_filename.split(".")[1:])
#    if log_job not in job_submit_start_end.keys():
#        job_submit_start_end[log_job] = [None, 0]
#    if args['job_limit'] == None or log_job <= args['job_limit']:
#        with open(log_file_path, "r") as log_stream:
#            for log_line in log_stream:
#                log_line_split = log_line.rstrip("\n").split()
#                if len(log_line_split) < 3:
#                    break
#                log_timestamp = datetime.datetime.strptime(log_line_split[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
#                log_time = int((log_timestamp - epoch).total_seconds())
#                log_runtime = int(eval(log_line_split[1].rstrip('s')))
#                log_id = log_line_split[2]
#                if job_submit_start_end[log_job][0] == None or log_time - log_runtime < job_submit_start_end[log_job][0]:
#                    job_submit_start_end[log_job][0] = log_time - log_runtime
#                if log_time > job_submit_start_end[log_job][1]:
#                    job_submit_start_end[log_job][1] = log_time
#                #if args['time_limit'] == None or min_time == None or log_time <= min_time + args['time_limit']:
#                #id_times[log_id] = log_time
#                if intermed.shape[0] <= log_time:
#                    intermed.resize(log_time + 1)
#                    intermed_done.resize(log_time + 1)
#                intermed[log_time - log_runtime:log_time + 1] += np.ones(log_runtime + 1, dtype = int)
#                intermed_done[log_time] += 1
#                #if min_time == None or log_time - log_runtime < min_time:
#                #    min_time = log_time - log_runtime

with open(args['controllerpath'] + "/crunch_" + args['modname'] + "_" + args['controllername'] + "_controller.info", "r") as controller_info_stream:
    prev_controller_info_line = ""
    for controller_info_line in controller_info_stream:
        if "Submitted batch job " in controller_info_line:
            controller_info_line_split = controller_info_line.rstrip("\n").split()
            controller_info_timestamp = datetime.datetime.strptime(prev_controller_info_line.rstrip("\n"), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
            controller_info_time = int((controller_info_timestamp - epoch).total_seconds())
            if args['time_limit'] == None or controller_info_time <= args['time_limit']:
                controller_info_job = int(controller_info_line_split[5].split("_job_")[1].split("_")[0])
                controller_info_steps_range = [int(x) for x in controller_info_line_split[5].split("_steps_")[1].split("-")]
                controller_info_steps = [(controller_info_job, x) for x in range(1, controller_info_steps_range[1] - controller_info_steps_range[0] + 2)]
                if controller_info_job not in job_submit_start_end.keys():
                    job_submit_start_end[controller_info_job] = [controller_info_time, None, None]
                for x in controller_info_steps:
                    if x not in step_submit_start_end.keys():
                        step_submit_start_end[x] = [controller_info_time, None, None]
        prev_controller_info_line = controller_info_line

for info_file_path in iglob(args['in_path'] + "/*.info"):
    with open(info_file_path, "r") as info_stream:
        info_start_linecount = 0
        info_end_linecount = 0
        for info_line in info_stream:
            info_line_split = info_line.rstrip("\n").split()
            info_timestamp = datetime.datetime.strptime(info_line_split[0], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
            info_time = int((info_timestamp - epoch).total_seconds())
            if args['time_limit'] == None or info_time <= args['time_limit']:
                info_job = int(info_line_split[1].split("_job_")[1].split("_")[0])
                info_step = (info_job, int(info_line_split[1].split("_step_")[1]))
                if info_job not in job_submit_start_end.keys():
                    job_submit_start_end[info_job] = [None, None, None]
                if info_step not in step_submit_start_end.keys():
                    step_submit_start_end[info_step] = [None, None, None]
                if info_line_split[2] == "START":
                    if info_start_linecount == 0:
                        job_submit_start_end[info_job][1] = info_time
                    step_submit_start_end[info_step][1] = info_time
                    info_start_linecount += 1
                elif info_line_split[2] == "END":
                    step_submit_start_end[info_step][2] = info_time
                    info_end_linecount += 1
        if args['time_limit'] == None or info_time <= args['time_limit']:
            if info_start_linecount == info_end_linecount:
                job_submit_start_end[info_job][2] = info_time

log_ids = []
for log_file_path in iglob(args['in_path'] + "/*.log"):
    log_filename = log_file_path.split("/")[-1]
    log_job = int(log_filename.split("_job_")[1].split("_")[0])
    log_step = (log_job, int(log_filename.split("_step_")[1].split(".")[0]))
    #log_state = '.'.join(log_filename.split(".")[1:])
    if log_job not in job_submit_start_end.keys():
        job_submit_start_end[log_job] = [None, None, None]
    if log_step not in step_submit_start_end.keys():
        step_submit_start_end[log_step] = [None, None, None]
    if args['job_limit'] == None or log_job <= args['job_limit']:
        with open(log_file_path, "r") as log_stream:
            for log_line in log_stream:
                log_line_split = log_line.rstrip("\n").split()
                if len(log_line_split) < 11:
                    break
                intermed_log_end_timestamp = datetime.datetime.strptime(log_line_split[3], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
                intermed_log_end_time = int((intermed_log_end_timestamp - epoch).total_seconds())
                if args['time_limit'] == None or intermed_log_end_time <= args['time_limit']:
                    intermed_log_start_timestamp = datetime.datetime.strptime(log_line_split[4], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
                    intermed_log_start_time = int((intermed_log_start_timestamp - epoch).total_seconds())
                    intermed_log_dir_size = int(log_line_split[5])
                    log_id = log_line_split[10]
                    log_ids += [log_id]

                    if job_submit_start_end[log_job][0] == None or intermed_log_start_time < job_submit_start_end[log_job][0]:
                        job_submit_start_end[log_job][0] = intermed_log_start_time
                    if job_submit_start_end[log_job][1] == None or intermed_log_start_time < job_submit_start_end[log_job][1]:
                        job_submit_start_end[log_job][1] = intermed_log_start_time
                    if job_submit_start_end[log_job][2] != None and out_log_end_time > job_submit_start_end[log_job][2]:
                        job_submit_start_end[log_job][2] = out_log_end_time

                    if step_submit_start_end[log_step][0] == None or intermed_log_start_time < step_submit_start_end[log_step][0]:
                        step_submit_start_end[log_step][0] = intermed_log_start_time
                    if step_submit_start_end[log_step][1] == None or intermed_log_start_time < step_submit_start_end[log_step][1]:
                        step_submit_start_end[log_step][1] = intermed_log_start_time
                    if step_submit_start_end[log_step][2] != None and out_log_end_time > step_submit_start_end[log_step][2]:
                        step_submit_start_end[log_step][2] = out_log_end_time
                    #if args['time_limit'] == None or min_time == None or log_time <= min_time + args['time_limit']:
                    if intermed.shape[0] <= intermed_log_end_time:
                        intermed.resize(intermed_log_end_time + 1)
                        intermed_done.resize(intermed_log_end_time + 1)
                    #print((intermed_log_time, intermed_log_prevtime, intermed_log_runtime))
                    #sys.stdout.flush()
                    intermed[intermed_log_start_time:intermed_log_end_time + 1] += np.ones(intermed_log_end_time - intermed_log_start_time + 1, dtype = int)
                    intermed_done[intermed_log_end_time] += 1

                    out_log_end_timestamp = datetime.datetime.strptime(log_line_split[0], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
                    out_log_end_time = int((out_log_end_timestamp - epoch).total_seconds())
                    if args['time_limit'] == None or out_log_end_time <= args['time_limit']:
                        out_log_start_timestamp = datetime.datetime.strptime(log_line_split[1], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
                        out_log_start_time = int((out_log_start_timestamp - epoch).total_seconds())
                        out_log_dir_size = int(log_line_split[2])

                        if out.shape[0] <= out_log_end_time:
                            out.resize(out_log_end_time + 1)
                            out_done.resize(out_log_end_time + 1)
                            dir_size.resize(out_log_end_time + 1)
                        #out[id_times[log_id] + 1:log_time + 1] += np.ones(log_time - id_times[log_id], dtype = int)
                        out[out_log_start_time:out_log_end_time + 1] += np.ones(out_log_end_time - out_log_start_time + 1, dtype = int)
                        out_done[out_log_end_time] += 1
                        #if log_time > max_time:
                        #    max_time = log_time
                        dir_size[intermed_log_end_time] = intermed_log_dir_size
                        dir_size[out_log_end_time] = out_log_dir_size

for intermed_log_file_path in iglob(args['in_path'] + "/*.log.intermed"):
    intermed_log_filename = intermed_log_file_path.split("/")[-1]
    intermed_log_job = int(intermed_log_filename.split("_job_")[1].split("_")[0])
    intermed_log_step = (intermed_log_job, int(intermed_log_filename.split("_step_")[1].split(".")[0]))
    if intermed_log_job not in job_submit_start_end.keys():
        job_submit_start_end[intermed_log_job] = [None, None, None]
    if intermed_log_step not in step_submit_start_end.keys():
        step_submit_start_end[intermed_log_step] = [None, None, None]
    if args['job_limit'] == None or intermed_log_job <= args['job_limit']:
        with open(intermed_log_file_path, "r") as intermed_log_stream:
            for intermed_log_line in intermed_log_stream:
                intermed_log_line_split = intermed_log_line.rstrip("\n").split()
                if len(intermed_log_line_split) < 8 or intermed_log_line_split[2] in log_ids:
                    break
                intermed_log_end_timestamp = datetime.datetime.strptime(intermed_log_line_split[0], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
                intermed_log_end_time = int((intermed_log_end_timestamp - epoch).total_seconds())
                if args['time_limit'] == None or intermed_log_end_time <= args['time_limit']:
                    intermed_log_start_timestamp = datetime.datetime.strptime(intermed_log_line_split[1], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo = utc)
                    intermed_log_start_time = int((intermed_log_start_timestamp - epoch).total_seconds())
                    intermed_log_dir_size = int(intermed_log_line_split[2])
                    intermed_log_id = intermed_log_line_split[7]

                    if job_submit_start_end[intermed_log_job][0] == None or intermed_log_start_time < job_submit_start_end[intermed_log_job][0]:
                        job_submit_start_end[intermed_log_job][0] = intermed_log_start_time
                    if job_submit_start_end[intermed_log_job][1] == None or intermed_log_start_time < job_submit_start_end[intermed_log_job][1]:
                        job_submit_start_end[intermed_log_job][1] = intermed_log_start_time

                    if step_submit_start_end[intermed_log_step][0] == None or intermed_log_start_time < step_submit_start_end[intermed_log_step][0]:
                        step_submit_start_end[intermed_log_step][0] = intermed_log_start_time
                    if step_submit_start_end[intermed_log_step][1] == None or intermed_log_start_time < step_submit_start_end[intermed_log_step][1]:
                        step_submit_start_end[intermed_log_step][1] = intermed_log_start_time
                    #if args['time_limit'] == None or min_time == None or intermed_log_time <= min_time + args['time_limit']:
                    if intermed.shape[0] <= intermed_log_end_time:
                        intermed.resize(intermed_log_end_time + 1)
                        intermed_done.resize(intermed_log_end_time + 1)
                        dir_size.resize(intermed_log_end_time + 1)
                    #print((intermed_log_time, intermed_log_prevtime, intermed_log_runtime))
                    #sys.stdout.flush()
                    intermed[intermed_log_start_time:intermed_log_end_time + 1] += np.ones(intermed_log_end_time - intermed_log_start_time + 1, dtype = int)
                    intermed_done[intermed_log_end_time] += 1

                    dir_size[intermed_log_end_time] = intermed_log_dir_size

#if max_time < min_time:
#    max_time = min_time
max_time = max([intermed.shape[0], out.shape[0]] + [x for y in job_submit_start_end.values() for x in y if x != None] + [x for y in step_submit_start_end.values() for x in y if x != None])
if args['time_limit'] != None and max_time > args['time_limit']:
    max_time = args['time_limit']
intermed.resize(max_time)
intermed_done.resize(max_time)
out.resize(max_time)
out_done.resize(max_time)
total = intermed + out
dir_size.resize(max_time)

#min_time = (total > 0).argmax()
min_time = min([x for y in job_submit_start_end.values() for x in y if x != None] + [x for y in step_submit_start_end.values() for x in y if x != None])
total = np.delete(total, range(min_time))
intermed = np.delete(intermed, range(min_time))
intermed_done = np.delete(intermed_done, range(min_time))
out = np.delete(out, range(min_time))
out_done = np.delete(out_done, range(min_time))
dir_size = np.delete(dir_size, range(min_time))

intermed_cum = np.zeros(total.shape[0], dtype = int)
intermed_count = 0
out_cum = np.zeros(total.shape[0], dtype = int)
out_count = 0
for i in range(total.shape[0]):
    intermed_count += intermed_done[i]
    intermed_cum[i] = intermed_count
    out_count += out_done[i]
    out_cum[i] = out_count

jobs_pend = np.zeros(total.shape[0], dtype = int)
jobs_run = np.zeros(total.shape[0], dtype = int)
steps_pend = np.zeros(total.shape[0], dtype = int)
steps_run = np.zeros(total.shape[0], dtype = int)
times = np.arange(total.shape[0], dtype = int)

#print("jobs: " + str(job_submit_start_end))
#print("")
#print("steps: " + str(step_submit_start_end))
#sys.stdout.flush()

for j in job_submit_start_end.values():
    if j[0] == None or j[0] > max_time:
        j[0] = max_time
    if j[1] == None or j[1] > max_time:
        j[1] = max_time
    if j[2] == None or j[2] > max_time:
        j[2] = max_time
    if j[0] <= j[1]:
        jobs_pend[j[0] - min_time:j[1] - min_time] += np.ones(j[1] - j[0], dtype = int)
    if j[1] <= j[2]:
        jobs_run[j[1] - min_time:j[2] - min_time] += np.ones(j[2] - j[1], dtype = int)

for s in step_submit_start_end.values():
    if s[0] == None or s[0] > max_time:
        s[0] = max_time
    if s[1] == None or s[1] > max_time:
        s[1] = max_time
    if s[2] == None or s[2] > max_time:
        s[2] = max_time
    if s[0] <= s[1]:
        steps_pend[s[0] - min_time:s[1] - min_time] += np.ones(s[1] - s[0], dtype = int)
    if s[1] <= s[2]:
        steps_run[s[1] - min_time:s[2] - min_time] += np.ones(s[2] - s[1], dtype = int)

#uncert = 2
#intermed_done_range = np.zeros(intermed_done.shape[0] / uncert, dtype = int)
#out_done_range = np.zeros(out_done.shape[0] / uncert, dtype = int)
#for i in range(intermed_done_range.shape[0]):
#    for j in range(uncert):
#        intermed_done_range[i] += intermed_done[uncert * i + j]
        #out_done_range[i] += out_done[uncert * i + j]

breaks = []
for i in range(steps_run.shape[0]):
    if steps_run[i] == 0:
        if i == 0 or steps_run[i - 1] > 0:
            breaks += [[i, i]]
        elif steps_run[i - 1] == 0:
            breaks[-1][1] = i

i = 0
while i < len(breaks):
    if breaks[i][1] - breaks[i][0] + 1 <= 0.1 * steps_run.shape[0]:
        del breaks[i]
        i -= 1
    i += 1

chunks = [[0]]
for b in breaks:
    chunks[-1] += [b[0]]
    chunks += [[b[1]]]
chunks[-1] += [steps_run.shape[0]]

times_zip = np.empty(0, dtype = int)
jobs_pend_zip = np.empty(0, dtype = int)
jobs_run_zip = np.empty(0, dtype = int)
steps_pend_zip = np.empty(0, dtype = int)
steps_run_zip = np.empty(0, dtype = int)
intermed_zip = np.empty(0, dtype = int)
out_zip = np.empty(0, dtype = int)
zip_lines = []
for c in chunks:
    times_zip = np.concatenate((times_zip, times[c[0]:c[1]]), axis = 0)
    jobs_pend_zip = np.concatenate((jobs_pend_zip, jobs_pend[c[0]:c[1]]), axis = 0)
    jobs_run_zip = np.concatenate((jobs_run_zip, jobs_run[c[0]:c[1]]), axis = 0)
    steps_pend_zip = np.concatenate((steps_pend_zip, steps_pend[c[0]:c[1]]), axis = 0)
    steps_run_zip = np.concatenate((steps_run_zip, steps_run[c[0]:c[1]]), axis = 0)
    intermed_zip = np.concatenate((intermed_zip, intermed_cum[c[0]:c[1]]), axis = 0)
    out_zip = np.concatenate((out_zip, out_cum[c[0]:c[1]]), axis = 0)
    if len(zip_lines) > 0:
        zip_lines += [zip_lines[-1] + c[1] - c[0] - 1]
    else:
        zip_lines += [c[1] - c[0] - 1]
del zip_lines[-1]

#times_norm = np.arange(times_zip.shape[0], dtype = int)
times = times_zip
jobs_pend = jobs_pend_zip
jobs_run = jobs_run_zip
steps_pend = steps_pend_zip
steps_run = steps_run_zip
intermed_cum = intermed_zip
out_cum = out_zip

#plot_labels = ["# Jobs", "# Steps", "Processing", "Processed", "Writing", "Written"]
#plot_lists = [jobs, total, run, intermed, wait, out]

#dpi = 10
#xmargin = 0
#ymargin = 0

if times[-1] > (60 * 60):
    timelevel = 2
elif times[-1] <= (60 * 60) and times[-1] > 60:
    timelevel = 1
else:
    timelevel = 0

xscale = int(times[-1] / (60 ** timelevel))
yscale = 5
#xpixels  = xscale * (max_time - min_time + 1)
#ypixels = [yscale * max(p) for p in plot_lists]
#figsize = ((xpixels + xmargin)/dpi, (sum(ypixels) + ymargin)/dpi)

print("Plotting job activity statistics...")
sys.stdout.flush()

width, height = 30., 25.
linewidth = width / times[-1]

ntime = 4
nhist = 2

gs = GridSpec(ntime + nhist, 1)

plt.suptitle('Resource Usage Statistics for ' + args['modname'] + "_" + args['controllername'])
fig = plt.figure(figsize = (width, height))#, dpi = dpi)

# Setup time series

axtime = []
for i in range(ntime):
    if i == 0:
        ax = fig.add_subplot(gs[i])
    else:
        ax = fig.add_subplot(gs[i], sharex = axtime[0])
    ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
    if i == ntime - 1:
        if timelevel == 2:
            ax.set_xlabel('Time (DD:HH)')
        elif timelevel == 1:
            ax.set_xlabel('Time (HH:MM)')
        else:
            ax.set_xlabel('Time (MM:SS)')
        ax.set_xticks([xscale * j for j in range(times.shape[0]) if times[j] % (60 ** timelevel) == 0], minor = False)
        ax.set_xticklabels([seconds2timestamp(timelevel, t) for t in times if t % (60 ** timelevel) == 0], minor = False)
        ax.set_xticks([xscale * j for j in range(times.shape[0]) if times[j] % ((60 ** timelevel) / 2) == 0], minor = True)
        #ax.xaxis.set_major_locator(MultipleLocator(xscale * (60 ** timelevel)))
        #ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(timelevel, int(x / xscale))))
        #ax.xaxis.set_minor_locator(MultipleLocator(xscale * (60 ** timelevel) / 2))
        ax.set_xlim(0, xscale * times.shape[0])#max(times))
    else:
        plt.setp(ax.get_xticklabels(), visible = False)
    for x in zip_lines:
        ax.axvline(x = xscale * x, color = 'k', linestyle = 'dashed', linewidth = 5)
    axtime += [ax]

# Setup histograms

axhist = []
for i in range(nhist):
    ax = fig.add_subplot(ntime + nhist, 1, i + ntime + 1)
    ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
    #ax.xlabel('Time (DD:HH)')
    axhist += [ax]

# 1) Storage

plot_times = np.array([times[i] for i in range(len(times)) if dir_size[i] > 0], dtype = int)
plot_label = "Storage"
plot_list = np.array([dir_size[i] for i in range(len(dir_size)) if dir_size[i] > 0], dtype = int)
color = 'gray'

if plot_list.shape[0] > 0:
    ymax = int(max(plot_list))
else:
    ymax = 0
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[0].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[0].transAxes)
axtime[0].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[0].set_ylabel("Total Storage (bytes)")
axtime[0].set_ylim(0, 1.1 * yscale * ymax)
#for i in range(len(plot_lists)):
#    axtime[0].fill_between(xscale * times, yscale * plot_lists[i], color = colors[i], alpha = 0.5, linewidth = 1, label = plot_labels[i])
axtime[0].fill_between(xscale * plot_times, yscale * plot_list, color = color, alpha = 0.5, linewidth = 0.1, label = plot_label)

# 2) Jobs

plot_labels = ["Pending", "Running"]
plot_lists = [jobs_pend, jobs_run]
colors = ['red', 'green']

concat_lists = np.concatenate(tuple(plot_lists), axis = 0)
if concat_lists.shape[0] > 0:
    ymax = int(max(concat_lists))
else:
    ymax = 0
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[1].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[1].transAxes)
axtime[1].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[1].set_ylabel("# of Jobs")
axtime[1].set_ylim(0, 1.1 * yscale * ymax)
#for i in range(len(plot_lists)):
#    axtime[1].fill_between(xscale * times, yscale * plot_lists[i], color = colors[i], alpha = 0.5, linewidth = 1, label = plot_labels[i])
axtime[1].fill_between(xscale * times, yscale * plot_lists[0], yscale * np.where(plot_lists[0] > plot_lists[1], plot_lists[1], 0), color = colors[0], alpha = 0.5, linewidth = 0.1, label = plot_labels[0])
axtime[1].fill_between(xscale * times, yscale * plot_lists[1], yscale * np.where(plot_lists[1] > plot_lists[0], plot_lists[0], 0), color = colors[1], alpha = 0.5, linewidth = 0.1, label = plot_labels[1])

axtime[1].legend(bbox_to_anchor = (0.85, 1.0, 0.15, 0.1), loc = 'lower left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 3) Steps

plot_labels = ["Pending", "Running"]
plot_lists = [steps_pend, steps_run]
colors = ['red', 'green']

concat_lists = np.concatenate(tuple(plot_lists), axis = 0)
if concat_lists.shape[0] > 0:
    ymax = int(max(concat_lists))
else:
    ymax = 0
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[2].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[2].transAxes)
axtime[2].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[2].set_ylabel("# of Steps")
axtime[2].set_ylim(0, 1.1 * yscale * ymax)
#for i in range(len(plot_lists)):
#    axtime[2].fill_between(xscale * times, yscale * plot_lists[i], color = colors[i], alpha = 0.5, linewidth = 1, label = plot_labels[i])
axtime[2].fill_between(xscale * times, yscale * plot_lists[0], yscale * np.where(plot_lists[0] > plot_lists[1], plot_lists[1], 0), color = colors[0], alpha = 0.5, linewidth = 0.1, label = plot_labels[0])
axtime[2].fill_between(xscale * times, yscale * plot_lists[1], yscale * np.where(plot_lists[1] > plot_lists[0], plot_lists[0], 0), color = colors[1], alpha = 0.5, linewidth = 0.1, label = plot_labels[1])

axtime[2].legend(bbox_to_anchor = (0.85, 1.0, 0.15, 0.1), loc = 'lower left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 4) Processing and Writing

plot_labels = ["Processing", "Writing"]#, "Active"]
plot_lists = [intermed_cum, out_cum]#, total]]
colors = ['green', 'blue']#, 'purple']

concat_lists = np.concatenate(tuple(plot_lists), axis = 0)
if concat_lists.shape[0] > 0:
    ymax = int(max(concat_lists))
else:
    ymax = 0
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[3].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[3].transAxes)
axtime[3].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[3].set_ylabel("# of Docs")
axtime[3].set_ylim(0, 1.1 * yscale * ymax)
#for i in range(len(plot_lists)):
#    axtime[3].fill_between(xscale * times, yscale * plot_lists[i], color = colors[i], alpha = 0.5, linewidth = 1, label = plot_labels[i])
axtime[3].fill_between(xscale * times, yscale * plot_lists[0], yscale * np.where(plot_lists[0] > plot_lists[1], plot_lists[1], 0), color = colors[0], alpha = 0.5, linewidth = 0.1, label = plot_labels[0])
axtime[3].fill_between(xscale * times, yscale * plot_lists[1], yscale * np.where(plot_lists[1] > plot_lists[0], plot_lists[0], 0), color = colors[1], alpha = 0.5, linewidth = 0.1, label = plot_labels[1])


#fit = np.polyfit(times, plot_lists[1], deg = 1)
#fit_pos_x = times[len(times) / 2]
#fit_pos_y = (fit[0] * fit_pos_x + fit[1]) - (0.1 * plot_lists[1][-1])
#axtime[2].plot(xscale * times, yscale * (fit[0] * times + fit[1]), color = 'black', linestyle = '--', alpha = 1, linewidth = 2, label = 'Linear Fit')
#slope = str("%.2f" % fit[0]) + "x"
#if fit[1] > 0:
#    yintercept = " + " + str("%.2f" % fit[1])
#elif fit[1] < 0:
#    yintercept = " - " + str("%.2f" % fit[1])
#else:
#    yintercept = ""
#axtime[2].text(xscale * fit_pos_x, yscale * fit_pos_y, "y = " + slope + yintercept, horizontalalignment = 'left', verticalalignment = 'bottom')

#axtime[2].legend(bbox_to_anchor = (0.775, 1.0, 0.225, 0.1), loc = 'lower left', ncol = 3, mode = "expand", borderaxespad = 0.)
axtime[3].legend(bbox_to_anchor = (0.85, 1.0, 0.15, 0.1), loc = 'lower left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 4) Writing

#plot_labels = ["Writing"]#, "Active"]
#plot_lists = [out_cum]#, total]
#colors = ['green']#, 'purple']

#ymax = int(max(np.concatenate(tuple(plot_lists), axis = 0)))
#yexp = len(str(ymax)) - 1
#if yexp != 0:
#    axtime[3].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[3].transAxes)
#axtime[3].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
#axtime[3].set_ylabel("# Docs Written")
#axtime[3].set_ylim(0, 1.1 * yscale * ymax)
#for i in range(len(plot_lists)):
#    axtime[3].fill_between([xscale * x for x in times_norm], [yscale * y for y in plot_lists[i]], color = colors[i], alpha = 0.5, linewidth = 1, label = plot_labels[i])

#axtime[2].legend(bbox_to_anchor = (0.8, 1.0, 0.2, 0.1), loc = 'lower left', ncol = 2, mode = "expand", borderaxespad = 0.)

## 3) Processing (weighted)
#
#intermed_weighted = np.array([float(intermed[i]) / float(steps_run[i]) if steps_run[i] > 0 else 0 for i in range(intermed.shape[0])], dtype = float)
#out_weighted = np.array([float(out[i]) / float(steps_run[i]) if steps_run[i] > 0 else 0 for i in range(out.shape[0])], dtype = float)
#
#plot_labels = ["Processing", "Writing"]#, "Active"]
#plot_lists = [intermed_weighted, out_weighted]#, total]
#colors = ['red', 'green']#, 'purple']
#
#ymax = int(max(np.concatenate(tuple(plot_lists), axis = 0)))
#yexp = len(str(ymax)) - 1
#if yexp != 0:
#    axtime[3].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[3].transAxes)
#axtime[3].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
#axtime[3].set_ylabel("# Docs")
#axtime[3].set_ylim(0, yscale * ymax)
#for i in range(len(plot_lists)):
#    axtime[3].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], color = colors[i], markersize = linewidth, label = plot_labels[i])
#
#axtime[3].legend(bbox_to_anchor = (0.1, 1.3, 0.9, .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 5) Steps Processed

plot_label = "Processed"
nonzero_list = [x for x in intermed_done if x > 0]
#nonzero_list = [float(intermed_done[i]) / intermed[i] for i in range(intermed_done.shape[0]) if intermed_done[i] > 0 and intermed[i] > 0]
#nonzero_list = []
#uncert = 2
#for i in range(intermed_done.shape[0]):
#    numerator = float(sum(intermed_done[max(0, i - uncert):i + uncert + 1]))
#    denominator = float(sum(intermed[max(0, i - uncert):i + uncert + 1]))
#    if numerator > 0:# and denominator > 0:
#        nonzero_list += [numerator / 2]# / denominator]
if len(nonzero_list) == 0:
    nonzero_list = [0]
plot_list = np.array(nonzero_list, dtype = int)
color = 'green'

#bins = [int(max(plot_list) / 28) * (j + 1) for j in range(28)]
#binwidth = Freedman_Diaconis_binwidth(plot_list)
binwidth = (max(plot_list) - min(plot_list)) / int(width)
#print(binwidth)
#sys.stdout.flush()
binwidth = max(binwidth, 1)
#print(binwidth)
#sys.stdout.flush()
bins = np.arange(min(plot_list), max(plot_list) + 1, binwidth)
counts, bins, patches = axhist[0].hist(plot_list, bins = bins, rwidth = 1, facecolor = color, alpha = 0.5, label = 'Histogram')
axhist[0].set_xticks(bins)
axhist[0].set_xlim(bins[0], bins[-1])# + bins[1] - bins[0])
if len(counts) > 0:
    ymax = int(max(counts))
else:
    ymax = 0
yexp = len(str(ymax)) - 1
if yexp != 0:
    axhist[0].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axhist[0].transAxes)
axhist[0].set_xlabel("# of Docs " + plot_label)
axhist[0].yaxis.set_major_formatter(create_y_ticker(1, yexp))
axhist[0].set_ylabel("Frequency")
axhist[0].set_ylim(0, 1 * ymax)

#mu, sigma = norm.fit(plot_list)
#fit = mlab.normpdf(bins, mu, sigma)
#if len(bins) > 1:
#    fit = fit * (len(plot_list) * (bins[1] - bins[0]))
#fit_pos_x = mu
#if len(fit) > 0:
#    fit_pos_y = max(fit)
#else:
#    fit_pos_y = 0
#fit_pos_y +=  (0.1 * ymax)
#axhist[0].plot(bins, fit, color = 'black', linestyle = '--', linewidth = 2, label = 'Normal Fit')
#axhist[0].text(fit_pos_x, fit_pos_y, r'$\mu=%.2f,\ \sigma=%.2f$' % (mu, sigma))

#axhist[0].legend(bbox_to_anchor = (0.85, 1.0, 0.15, 0.1), loc = 'lower left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 6) Steps Written

plot_label = "Written"
nonzero_list = [x for x in out_done if x > 0]
#nonzero_list = [float(out_done[i]) / out[i] for i in range(out_done.shape[0]) if out_done[i] > 0 and out[i] > 0]
#nonzero_list = []
#uncert = 2
#for i in range(out_done.shape[0]):
#    numerator = float(sum(out_done[max(0, i - uncert):i + uncert + 1]))
#    denominator = float(sum(out[max(0, i - uncert):i + uncert + 1]))
#    if numerator > 0:# and denominator > 0:
#        nonzero_list += [numerator / 2]# / denominator]
if len(nonzero_list) == 0:
    nonzero_list = [0]
plot_list = np.array(nonzero_list, dtype = int)
color = 'blue'
#for i in range(len(plot_lists)):
#axhist[i].fill_between([xscale * x for x in times], 0, [yscale * y for y in plot_lists[i]], color = colors[i], linewidth = linewidth)
#axhist[i].hist(plot_lists[i], int(len(plot_lists[i])/5), facecolor = colors[i], alpha = 0.75)
#bins = [x for x in plot_lists[i] if x > 0]

#bins = [controllerconfigdoc["options"]["nbatch"] * (j + 1) for j in range(int(max(plot_list) / controllerconfigdoc["options"]["nbatch"]) + 1)]
#bins = [int(max(plot_list) / 28) * (j + 1) for j in range(28)]
#binwidth = Freedman_Diaconis_binwidth(plot_list)
binwidth = (max(plot_list) - min(plot_list)) / int(width)
binwidth = max(binwidth, 1)
#print(binwidth)
#sys.stdout.flush()
bins = np.arange(min(plot_list), max(plot_list) + 1, binwidth)
counts, bins, patches = axhist[1].hist(plot_list, bins = bins, rwidth = 1, facecolor = color, alpha = 0.5, label = 'Histogram')
axhist[1].set_xticks(bins)
axhist[1].set_xlim(bins[0], bins[-1])# + bins[1] - bins[0])
if len(counts) > 0:
    ymax = int(max(counts))
else:
    ymax = 0
yexp = len(str(ymax)) - 1
if yexp != 0:
    axhist[1].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axhist[1].transAxes)
axhist[1].set_xlabel("# of Docs " + plot_label)
axhist[1].yaxis.set_major_formatter(create_y_ticker(1, yexp))
axhist[1].set_ylabel("Frequency")
axhist[1].set_ylim(0, 1 * ymax)

#mu, sigma = norm.fit(plot_list)
#fit = mlab.normpdf(bins, mu, sigma)
#if len(bins) > 1:
#    fit = fit * (len(plot_list) * (bins[1] - bins[0]))
#print("bins: " + str(bins.tolist()))
#print("counts: " + str(counts.tolist()))
#sys.stdout.flush()
#fit_pos_x = mu
#if len(fit) > 0:
#    fit_pos_y = max(fit)
#else:
#    fit_pos_y = 0
#fit_pos_y +=  (0.1 * ymax)
#axhist[1].plot(bins, fit, color = 'black', linestyle = '--', linewidth = 2, label = 'Normal Fit')
#axhist[1].text(fit_pos_x, fit_pos_y, r'$\mu=%.2f,\ \sigma=%.2f$' % (mu, sigma))

#axhist[1].legend(bbox_to_anchor = (0.85, 1.0, 0.15, 0.1), loc = 'lower left', ncol = 2, mode = "expand", borderaxespad = 0.)

#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
#plt.legend(bbox_to_anchor = (0., -0.4, 1., .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)
#plt.subplots_adjust(bottom = 0.2)

plt.subplots_adjust(hspace = 0.5)

with PdfPages(args['out_path'] + "/" + args['out_file_name']) as pdf:
    pdf.savefig(fig)

print("Finished generating job activity graphs for " + args["modname"] + "_" + args["controllername"] + ".")
sys.stdout.flush()