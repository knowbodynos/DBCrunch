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

def create_y_ticker(yscale, yexp):
    return FuncFormatter(lambda y, pos: '%.1f' % (float(y)/(yscale * (10 ** yexp))))

parser = ArgumentParser()

parser.add_argument('--job-limit', '-j', dest = 'job_limit', action = 'store', default = None, help = '')
parser.add_argument('--time-limit', '-t', dest = 'time_limit', action = 'store', default = None, help = '')
parser.add_argument('in_path', help = '')
parser.add_argument('out_path', help = '')
parser.add_argument('modname', help = '')
parser.add_argument('controllername', help = '')
parser.add_argument('controllerpath', help = '')
parser.add_argument('out_file_name', help = '')

args = vars(parser.parse_known_args()[0])

if args['job_limit'] != None:
    args['job_limit'] = int(args['job_limit'])
if args['time_limit'] != None:
    args['time_limit'] = timestamp2unit(args['time_limit'])

with open(args['controllerpath'] +  "/" + args["modname"] + "_" + args["controllername"] + ".config", "r") as controllerconfigstream:
    controllerconfigdoc = yaml.load(controllerconfigstream)

with open(args['controllerpath'] + "/crunch_" + args['modname'] + "_" + args['controllername'] + "_controller.info", "r") as controller_stream:
    controller_line_split = controller_stream.readline().rstrip("\n").split()
    epoch = datetime.datetime.strptime(" ".join(controller_line_split[:4]), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)

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
            controller_info_timestamp = datetime.datetime.strptime(prev_controller_info_line.rstrip("\n"), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
            controller_info_time = int((controller_info_timestamp - epoch).total_seconds())
            controller_info_job = int(controller_info_line_split[5].split("_job_")[1].split("_")[0])
            controller_info_steps_range = [int(x) for x in controller_info_line_split[5].split("_steps_")[1].split("-")]
            controller_info_steps = [(controller_info_job, x) for x in range(controller_info_steps_range[0], controller_info_steps_range[1] + 1)]
            if controller_info_job not in job_submit_start_end.keys():
                job_submit_start_end[controller_info_job] = [controller_info_time, None, None]
            for x in controller_info_steps:
                if x not in step_submit_start_end.keys():
                    step_submit_start_end[x] = [controller_info_time, None, None]
        prev_controller_info_line = controller_info_line

for info_file_path in iglob(args['in_path'] + "/*.info"):
    with open(info_file_path, "r") as info_stream:
        info_linecount = 0
        for info_line in info_stream:
            info_line_split = info_line.rstrip("\n").split()
            info_timestamp = datetime.datetime.strptime(info_line_split[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
            info_time = int((info_timestamp - epoch).total_seconds())
            info_job = int(info_line_split[1].split("_job_")[1].split("_")[0])
            info_step = (info_job, int(info_line_split[1].split("_step_")[1]))
            if log_job not in job_submit_start_end.keys():
                job_submit_start_end[log_job] = [None, None, None]
            if log_step not in step_submit_start_end.keys():
                step_submit_start_end[log_step] = [None, None, None]
            if info_line_split[2] == "START":
                if info_linecount == 0:
                    if info_job in job_submit_start_end.keys():
                        job_submit_start_end[info_job][1] = info_time
                if info_step not in step_submit_start_end.keys():
                    step_submit_start_end[info_step][1] = info_time
            elif info_line_split[2] == "END":
                if info_step not in step_submit_start_end.keys():
                    step_submit_start_end[info_step][2] = info_time
            info_linecount += 1
        if info_line_split[2] == "END":
            if info_job in job_submit_start_end.keys():
                job_submit_start_end[info_job][2] = info_time

for log_file_path in iglob(args['in_path'] + "/*.log"):
    log_filename = log_file_path.split("/")[-1]
    log_job = int(log_filename.split("_job_")[1].split("_")[0])
    log_step = (log_job, int(log_filename.split("_step_")[1].split(".")[0]))
    log_state = '.'.join(log_filename.split(".")[1:])
    if log_job not in job_submit_start_end.keys():
        job_submit_start_end[log_job] = [None, None, None]
    if log_step not in step_submit_start_end.keys():
        step_submit_start_end[log_step] = [None, None, None]
    if args['job_limit'] == None or log_job <= args['job_limit']:
        with open(log_file_path, "r") as log_stream:
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
                if job_submit_start_end[log_job][0] == None or log_intermed_time - log_intermed_runtime < job_submit_start_end[log_job][0]:
                    job_submit_start_end[log_job][0] = log_intermed_time - log_intermed_runtime
                if job_submit_start_end[log_job][1] == None or log_intermed_time - log_intermed_runtime < job_submit_start_end[log_job][1]:
                    job_submit_start_end[log_job][1] = log_intermed_time - log_intermed_runtime
                if log_out_time > job_submit_start_end[log_job][2]:
                    job_submit_start_end[log_job][2] = log_out_time
                if step_submit_start_end[log_step][0] == None or log_intermed_time - log_intermed_runtime < step_submit_start_end[log_step][0]:
                    step_submit_start_end[log_step][0] = log_intermed_time - log_intermed_runtime
                if step_submit_start_end[log_step][1] == None or log_intermed_time - log_intermed_runtime < step_submit_start_end[log_step][1]:
                    step_submit_start_end[log_step][1] = log_intermed_time - log_intermed_runtime
                if log_out_time > step_submit_start_end[log_step][2]:
                    step_submit_start_end[log_step][2] = log_out_time
                #if args['time_limit'] == None or min_time == None or log_time <= min_time + args['time_limit']:
                if intermed.shape[0] <= log_intermed_time:
                    intermed.resize(log_intermed_time + 1)
                    intermed_done.resize(log_intermed_time + 1)
                intermed[log_intermed_time - log_intermed_runtime:log_intermed_time + 1] += np.ones(log_intermed_runtime + 1, dtype = int)
                intermed_done[log_intermed_time] += 1
                if out.shape[0] <= log_out_time:
                    out.resize(log_out_time + 1)
                    out_done.resize(log_out_time + 1)
                #out[id_times[log_id] + 1:log_time + 1] += np.ones(log_time - id_times[log_id], dtype = int)
                out[log_out_time - log_out_runtime:log_out_time + 1] += np.ones(log_out_runtime + 1, dtype = int)
                out_done[log_out_time] += 1
                #if log_time > max_time:
                #    max_time = log_time

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

#min_time = (total > 0).argmax()
min_time = min([x for y in job_submit_start_end.values() for x in y if x != None] + [x for y in step_submit_start_end.values() for x in y if x != None])
total = np.delete(total, range(min_time))
intermed = np.delete(intermed, range(min_time))
intermed_done = np.delete(intermed_done, range(min_time))
out = np.delete(out, range(min_time))
out_done = np.delete(out_done, range(min_time))

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
    if j[1] == None or j[1] > max_time:
        j[1] = max_time
    if j[2] == None or j[2] > max_time:
        j[2] = max_time
    if j[0] <= j[1]:
        jobs_pend[j[0] - min_time:j[1] - min_time] += np.ones(j[1] - j[0], dtype = int)
    if j[1] <= j[2]:
        jobs_run[j[1] - min_time:j[2] - min_time] += np.ones(j[2] - j[1], dtype = int)

for s in step_submit_start_end.values():
    if s[1] == None or s[1] > max_time:
        s[1] = max_time
    if s[2] == None or s[2] > max_time:
        s[2] = max_time
    if s[0] <= s[1]:
        steps_pend[s[0] - min_time:s[1] - min_time] += np.ones(s[1] - s[0], dtype = int)
    if s[1] <= s[2]:
        steps_run[s[1] - min_time:s[2] - min_time] += np.ones(s[2] - s[1], dtype = int)

#plot_labels = ["# Jobs", "# Steps", "Processing", "Processed", "Writing", "Written"]
#plot_lists = [jobs, total, run, intermed, wait, out]

#dpi = 10
#xmargin = 0
#ymargin = 0

if times[-1] > (60 * 60):
    timelevel = 2
elif times[-1] < (60 * 60) and times[-1] > 60:
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

width, height = 30., 8.
linewidth = width / times[-1]

ntime = 3
nhist = 2

plt.suptitle('Resource Usage Statistics for ' + args['modname'] + "_" + args['controllername'])
fig = plt.figure(figsize = (width, height))#, dpi = dpi)

axtime = []
for i in range(ntime):
    if i == 0:
        ax = fig.add_subplot(ntime + nhist, 1, i + 1)
    else:
        ax = fig.add_subplot(ntime + nhist, 1, i + 1, sharex = axtime[0])
    ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
    if i == ntime - 1:
        if timelevel == 2:
            ax.set_xlabel('Time (DD:HH)')
        elif timelevel == 1:
            ax.set_xlabel('Time (HH:MM)')
        else:
            ax.set_xlabel('Time (MM:SS)')
        ax.xaxis.set_major_locator(MultipleLocator(xscale * (60 ** timelevel)))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(timelevel, int(x / xscale))))
        ax.xaxis.set_minor_locator(MultipleLocator(xscale * (60 ** timelevel) / 2))
        ax.set_xlim(0, xscale * max(times))
    else:
        plt.setp(ax.get_xticklabels(), visible = False)
    axtime += [ax]

# 1) Jobs

plot_labels = ["Pending", "Running"]
plot_lists = [jobs_pend, jobs_run]
colors = ['red', 'green']

ymax = int(max(np.concatenate((jobs_pend, jobs_run), axis=0)))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[0].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[0].transAxes)
axtime[0].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[0].set_ylabel("# Jobs")
axtime[0].set_ylim(0, yscale * ymax)
for i in range(len(plot_lists)):
    axtime[0].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], color = colors[i], alpha = 0.75, label = plot_labels[i])#, markersize = linewidth)

axtime[0].legend(bbox_to_anchor = (0.1, 1.3, 0.9, .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 2) Steps

plot_labels = ["Pending", "Running"]
plot_lists = [steps_pend, steps_run]
colors = ['red', 'green']

ymax = int(max(np.concatenate(tuple(plot_lists), axis=0)))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[1].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[1].transAxes)
axtime[1].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[1].set_ylabel("# Steps")
axtime[1].set_ylim(0, yscale * ymax)
for i in range(len(plot_lists)):
    axtime[1].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], color = colors[i], alpha = 0.75, label = plot_labels[i])#, markersize = linewidth)

axtime[1].legend(bbox_to_anchor = (0.1, 1.3, 0.9, .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 3) Processing

plot_labels = ["Processing", "Writing"]#, "Active"]
plot_lists = [intermed, out]#, total]
colors = ['red', 'green']#, 'purple']

ymax = int(max(total))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[2].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[2].transAxes)
axtime[2].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[2].set_ylabel("# Docs")
axtime[2].set_ylim(0, yscale * ymax)
for i in range(len(plot_lists)):
    axtime[2].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], color = colors[i], markersize = linewidth, label = plot_labels[i])

axtime[2].legend(bbox_to_anchor = (0.1, 1.3, 0.9, .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)

# 4) Steps Processed/Written

axhist = []
for i in range(nhist):
    ax = fig.add_subplot(ntime + nhist, 1, i + ntime + 1)
    ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
    #ax.xlabel('Time (DD:HH)')
    axhist += [ax]

plot_labels = ["Processed", "Written"]
plot_lists = [intermed_done, out_done]
colors = ['orange', 'green']

for i in range(len(plot_lists)):
    #axhist[i].fill_between([xscale * x for x in times], 0, [yscale * y for y in plot_lists[i]], color = colors[i], linewidth = linewidth)
    #axhist[i].hist(plot_lists[i], int(len(plot_lists[i])/5), facecolor = colors[i], alpha = 0.75)
    #bins = [x for x in plot_lists[i] if x > 0]
    bins = [controllerconfigdoc["options"]["nbatch"] * (j + 1) for j in range(int(max(plot_lists[i]) / controllerconfigdoc["options"]["nbatch"]) + 1)]
    counts, bins, patches = axhist[i].hist([x for x in plot_lists[i] if x > 0], bins = bins, rwidth = 1, facecolor = colors[i], alpha = 0.75)
    axhist[i].set_xticks(bins)
    axhist[i].set_xlim(bins[0], bins[-1] + bins[1] - bins[0])
    ymax = int(max(counts))
    yexp = len(str(ymax)) - 1
    if yexp != 0:
        axhist[i].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axhist[i].transAxes)
    axhist[i].set_xlabel("# Docs " + plot_labels[i])
    axhist[i].yaxis.set_major_formatter(create_y_ticker(1, yexp))
    axhist[i].set_ylabel("Frequency")
    axhist[i].set_ylim(0, 1 * ymax)

#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
#plt.legend(bbox_to_anchor = (0., -0.4, 1., .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)
#plt.subplots_adjust(bottom = 0.2)

plt.subplots_adjust(hspace = 0.5)

with PdfPages(args['out_path'] + "/" + args['out_file_name']) as pdf:
    pdf.savefig(fig)

print("Finished generating job activity graphs for " + args["modname"] + "_" + args["controllername"] + ".")
sys.stdout.flush()