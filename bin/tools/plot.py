#    DBCrunch: plot.py
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

import sys, datetime, matplotlib, warnings
from pytz import utc
from glob import iglob
from argparse import ArgumentParser
import numpy as np
matplotlib.use('Agg')
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import MultipleLocator, FuncFormatter
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

parser = ArgumentParser()

parser.add_argument('--job-limit', '-j', dest = 'job_limit', action = 'store', default = None, help = '')
parser.add_argument('--time-limit', '-t', dest = 'time_limit', action = 'store', default = None, help = '')
parser.add_argument('in_path', help = '')
parser.add_argument('out_path', help = '')
parser.add_argument('modname', help = '')
parser.add_argument('controllername', help = '')
parser.add_argument('controllerpath', help = '')
parser.add_argument('intermed_file_name', help = '')
parser.add_argument('out_file_name', help = '')

args = vars(parser.parse_known_args()[0])

if args['job_limit'] != None:
    args['job_limit'] = int(args['job_limit'])
if args['time_limit'] != None:
    args['time_limit'] = timestamp2unit(args['time_limit'])

with open(args['controllerpath'] + "/crunch_" + args['modname'] + "_" + args['controllername'] + "_controller.info", "r") as controller_stream:
    controller_line_split = controller_stream.readline().rstrip("\n").split()
    epoch = datetime.datetime.strptime(" ".join(controller_line_split[:4]), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)

#epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

data ={}
all_jobs = []
all_steps = []

min_time = None
max_time = 0

print("Analyzing job activity statistics...")
sys.stdout.flush()

#for log_file_path in iglob(args['in_path'] + "/*.log.intermed"):
#    log_filename = log_file_path.split("/")[-1]
#    log_job = int(log_filename.split("_job_")[1].split("_")[0])
#    log_step = int(log_filename.split("_step_")[1].split(".")[0])
#    log_state = '.'.join(log_filename.split(".")[1:])
#    if log_job not in all_jobs:
#        all_jobs += [log_job]
#    all_steps += [(log_job, log_step)]
#    if args['job_limit'] == None or log_job <= args['job_limit']:
#        with open(log_file_path,"r") as log_stream:
#            for log_line in log_stream:
#                log_line_split = log_line.rstrip("\n").split()
#                if len(log_line_split) < 6:
#                    break
#                log_timestamp = datetime.datetime.strptime(log_line_split[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
#                log_time = int((log_timestamp - epoch).total_seconds())
#                log_runtime = int(eval(log_line_split[1].rstrip('s')))
#                log_id = log_line_split[2]
#                if min_time == None or log_time - log_runtime < min_time:
#                    min_time = log_time - log_runtime
#                if log_time > max_time:
#                    max_time = log_time
#                if args['time_limit'] == None or min_time == None or log_time <= min_time + args['time_limit']:
#                    if log_id in data.keys():
#                        data[log_id].update({'JOB': log_job, 'STEP': log_step, 'START_INTERMED_TIME': log_time - log_runtime, 'END_INTERMED_TIME': log_time})
#                    else:
#                        data[log_id] = {'JOB': log_job, 'STEP': log_step, 'START_INTERMED_TIME': log_time - log_runtime, 'END_INTERMED_TIME': log_time}

for log_file_path in iglob(args['in_path'] + "/*.log"):
    log_filename = log_file_path.split("/")[-1]
    log_job = int(log_filename.split("_job_")[1].split("_")[0])
    log_step = int(log_filename.split("_step_")[1].split(".")[0])
    log_state = '.'.join(log_filename.split(".")[1:])
    if log_job not in all_jobs:
        all_jobs += [log_job]
    all_steps += [(log_job, log_step)]
    if args['job_limit'] == None or log_job <= args['job_limit']:
        with open(log_file_path,"r") as log_stream:
            for log_line in log_stream:
                log_line_split = log_line.rstrip("\n").split()
                if len(log_line_split) < 5:
                    break
                log_out_timestamp = datetime.datetime.strptime(log_line_split[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
                log_out_time = int((log_out_timestamp - epoch).total_seconds())
                log_out_runtime = int(eval(log_line_split[1].rstrip('s')))
                log_intermed_timestamp = datetime.datetime.strptime(log_line_split[2], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
                log_intermed_time = int((log_out_timestamp - epoch).total_seconds())
                log_intermed_runtime = int(eval(log_line_split[3].rstrip('s')))
                log_id = log_line_split[4]
                if min_time == None or log_intermed_time - log_intermed_runtime < min_time:
                    min_time = log_intermed_time - log_intermed_runtime
                if log_out_time > max_time:
                    max_time = log_out_time
                if args['time_limit'] == None or min_time == None or log_out_time <= min_time + args['time_limit']:
                    if log_id in data.keys():
                        data[log_id].update({'JOB': log_job, 'STEP': log_step, 'START_INTERMED_TIME': log_intermed_time - log_intermed_runtime, 'END_INTERMED_TIME': log_intermed_time, 'START_OUT_TIME': log_out_time - log_out_runtime, 'END_OUT_TIME': log_out_time})
                    else:
                        data[log_id] = {'JOB': log_job, 'STEP': log_step, 'START_INTERMED_TIME': log_intermed_time - log_intermed_runtime, 'END_INTERMED_TIME': log_intermed_time, 'START_OUT_TIME': log_out_time - log_out_runtime, 'END_OUT_TIME': log_out_time}

nsteps = []
nsteps_tot = 0
for job in sorted(all_jobs):
    for step in all_steps:
        if step[0] == job:
            nsteps_tot += 1
    nsteps += [nsteps_tot]

if max_time - min_time + 1 > (60 * 60):
    timelevel = 2
elif max_time - min_time + 1 < (60 * 60) and max_time - min_time + 1 > 60:
    timelevel = 1
else:
    timelevel = 0

time_labels = [seconds2timestamp(timelevel, x) for x in range(max_time - min_time + 1)]
step_labels = sorted(all_steps)

intermed_img = np.ones((len(all_steps), max_time - min_time + 1, 3))

for d in sorted([x for x in data.values() if 'END_OUT_TIME' in x.keys()], key = lambda x: (x['JOB'], x['STEP'], -x['END_INTERMED_TIME'])):
    s = step_labels.index((d['JOB'], d['STEP']))
    for t in range(d['START_INTERMED_TIME'] - min_time, d['END_INTERMED_TIME'] - min_time):
        intermed_img[s][t] = [0.75, 0, 0]

    intermed_img[s][d['END_INTERMED_TIME'] - min_time] = [0, 0, 0]

print("Plotting intermediate job activity statistics...")
sys.stdout.flush()

intermed_dpi = 100
intermed_xmargin = 0
intermed_ymargin = 100
intermed_xscale = 20
intermed_yscale = 20
intermed_xpixels, intermed_ypixels = intermed_xscale * (max_time - min_time + 1), intermed_yscale * len(step_labels)
intermed_figsize = ((intermed_xpixels + intermed_xmargin) / intermed_dpi, (intermed_ypixels + intermed_ymargin) / intermed_dpi)

intermed_fig = plt.figure(figsize = intermed_figsize, dpi = intermed_dpi)
intermed_ax = plt.gca()
intermed_ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
intermed_ax.set_title('Job Latency (INTERMED)')
if timelevel == 2:
    intermed_ax.set_xlabel('Time (DD:HH)')
elif timelevel == 1:
    intermed_ax.set_xlabel('Time (HH:MM)')
else:
    intermed_ax.set_xlabel('Time (MM:SS)')
intermed_ax.set_ylabel('(Job, Step)')
intermed_ax.set_xticks(np.arange(0, intermed_xpixels, intermed_xscale * (60 ** timelevel)))
intermed_ax.set_xticklabels(time_labels)
intermed_ax.xaxis.set_minor_locator(MultipleLocator(intermed_xscale * (60 ** timelevel) / 2))
intermed_ax.xaxis.set_minor_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(timelevel, int(x / intermed_xscale))))
intermed_ax.set_yticks(np.arange(intermed_yscale * 0.5, intermed_ypixels + intermed_yscale * 0.5, intermed_yscale))
intermed_ax.set_yticklabels(step_labels)
plt.imshow(intermed_img, interpolation = 'nearest', extent = [0, intermed_xpixels, 0, intermed_ypixels])
for y in nsteps:
    intermed_ax.axhline(y = intermed_yscale * y)

with PdfPages(args['out_path'] + "/" + args['intermed_file_name']) as pdf:
    pdf.savefig(intermed_fig)

out_img = np.ones((len(all_steps), max_time - min_time + 1, 3))

for d in sorted([x for x in data.values() if 'END_OUT_TIME' in x.keys()], key = lambda x: (x['JOB'], x['STEP'], -x['END_OUT_TIME'])):
    s = step_labels.index((d['JOB'], d['STEP']))
    for t in range(d['START_INTERMED_TIME'] - min_time, d['END_INTERMED_TIME'] - min_time):
        out_img[s][t] = [0, 0.75, 0]

    out_img[s][d['START_OUT_TIME'] - min_time] = [0, 0.5, 0]

    for t in range(d['START_OUT_TIME'] - min_time, d['END_OUT_TIME'] - min_time):
        out_img[s][t] = [0, 0.25, 0]

    out_img[s][d['END_OUT_TIME'] - min_time] = [0, 0, 0]

print("Plotting final job activity statistics...")
sys.stdout.flush()

out_dpi = 100
out_xmargin = 0
out_ymargin = 100
out_xscale = 20
out_yscale = 20
out_xpixels, out_ypixels = out_xscale * (max_time - min_time + 1), out_yscale * len(step_labels)
out_figsize = ((out_xpixels + out_xmargin) / out_dpi, (out_ypixels + out_ymargin) / out_dpi)

out_fig = plt.figure(figsize = out_figsize, dpi = out_dpi)
out_ax = plt.gca()
out_ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
out_ax.set_title('Job Latency (OUT)')
if timelevel == 2:
    out_ax.set_xlabel('Time (DD:HH)')
elif timelevel == 1:
    out_ax.set_xlabel('Time (HH:MM)')
else:
    out_ax.set_xlabel('Time (MM:SS)')
out_ax.set_ylabel('(Job, Step)')
out_ax.set_xticks(np.arange(0, out_xpixels, out_xscale * (60 ** timelevel)))
out_ax.set_xticklabels(time_labels)
out_ax.xaxis.set_minor_locator(MultipleLocator(out_xscale * (60 ** timelevel) / 2))
out_ax.xaxis.set_minor_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(timelevel, int(x / out_xscale))))
out_ax.set_yticks(np.arange(out_yscale * 0.5, out_ypixels + out_yscale * 0.5, out_yscale))
out_ax.set_yticklabels(step_labels)
plt.imshow(out_img, interpolation = 'nearest', extent = [0, out_xpixels, 0, out_ypixels])
for y in nsteps:
    out_ax.axhline(y = out_yscale * y)

with PdfPages(args['out_path'] + "/" + args['out_file_name']) as pdf:
    pdf.savefig(out_fig)

print("Finished generating job activity for " + args["modname"] + "_" + args["controllername"] + ".")
sys.stdout.flush()