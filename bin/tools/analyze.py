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

import sys, datetime, matplotlib, warnings
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

with open(args['controllerpath'] + "/crunch_" + args['modname'] + "_" + args['controllername'] + "_controller.log", "r") as controller_stream:
    controller_line_split = controller_stream.readline().rstrip("\n").split()
    epoch = datetime.datetime.strptime(" ".join(controller_line_split[:4]), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)

#epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

#id_times = {}

job_min_max = {}

#min_time = None
#max_time = 0

intermed = np.zeros(0, dtype = int)
intermed_done = np.zeros(0, dtype = int)
out = np.zeros(0, dtype = int)
out_done = np.zeros(0, dtype = int)

print("Analyzing resource usage statistics...")

for log_file_path in iglob(args['in_path'] + "/*.log.intermed"):
    log_filename = log_file_path.split("/")[-1]
    log_job = int(log_filename.split("_job_")[1].split("_")[0])
    log_step = int(log_filename.split("_step_")[1].split(".")[0])
    log_state = '.'.join(log_filename.split(".")[1:])
    if log_job not in job_min_max.keys():
        job_min_max[log_job] = [None, 0]
    if args['job_limit'] == None or log_job <= args['job_limit']:
        with open(log_file_path, "r") as log_stream:
            for log_line in log_stream:
                log_line_split = log_line.rstrip("\n").split()
                if len(log_line_split) < 3:
                    break
                log_timestamp = datetime.datetime.strptime(log_line_split[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
                log_time = int((log_timestamp - epoch).total_seconds())
                log_runtime = int(eval(log_line_split[1].rstrip('s')))
                log_id = log_line_split[2]
                if job_min_max[log_job][0] == None or log_time - log_runtime < job_min_max[log_job][0]:
                    job_min_max[log_job][0] = log_time - log_runtime
                if log_time > job_min_max[log_job][1]:
                    job_min_max[log_job][1] = log_time
                #if args['time_limit'] == None or min_time == None or log_time <= min_time + args['time_limit']:
                #id_times[log_id] = log_time
                if intermed.shape[0] <= log_time:
                    intermed.resize(log_time + 1)
                    intermed_done.resize(log_time + 1)
                intermed[log_time - log_runtime:log_time + 1] += np.ones(log_runtime + 1, dtype = int)
                intermed_done[log_time] += 1
                #if min_time == None or log_time - log_runtime < min_time:
                #    min_time = log_time - log_runtime

for log_file_path in iglob(args['in_path'] + "/*.log"):
    log_filename = log_file_path.split("/")[-1]
    log_job = int(log_filename.split("_job_")[1].split("_")[0])
    log_step = int(log_filename.split("_step_")[1].split(".")[0])
    log_state = '.'.join(log_filename.split(".")[1:])
    if log_job not in job_min_max.keys():
        job_min_max[log_job] = [None, 0]
    if args['job_limit'] == None or log_job <= args['job_limit']:
        with open(log_file_path, "r") as log_stream:
            for log_line in log_stream:
                log_line_split = log_line.rstrip("\n").split()
                if len(log_line_split) < 4:
                    break
                log_timestamp = datetime.datetime.strptime(log_line_split[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo = utc)
                log_time = int((log_timestamp - epoch).total_seconds())
                log_writetime = int(eval(log_line_split[2].rstrip('s')))
                #log_runtime = int(eval(log_line_split[2].rstrip('s')))
                log_id = log_line_split[3]
                if job_min_max[log_job][0] == None or log_time - log_runtime < job_min_max[log_job][0]:
                    job_min_max[log_job][0] = log_time - log_runtime
                if log_time > job_min_max[log_job][1]:
                    job_min_max[log_job][1] = log_time
                #if args['time_limit'] == None or min_time == None or log_time <= min_time + args['time_limit']:
                if out.shape[0] <= log_time:
                    out.resize(log_time + 1)
                    out_done.resize(log_time + 1)
                #out[id_times[log_id] + 1:log_time + 1] += np.ones(log_time - id_times[log_id], dtype = int)
                out[log_time - log_writetime:log_time + 1] += np.ones(log_writetime + 1, dtype = int)
                out_done[log_time] += 1
                #if log_time > max_time:
                #    max_time = log_time

#if max_time < min_time:
#    max_time = min_time
max_time = max(intermed.shape[0], out.shape[0])
if args['time_limit'] != None and max_time > args['time_limit']:
    max_time = args['time_limit']
intermed.resize(max_time)
intermed_done.resize(max_time)
out.resize(max_time)
out_done.resize(max_time)
steps = intermed + out

min_time = (steps > 0).argmax()
steps = np.delete(steps, range(min_time))
intermed = np.delete(intermed, range(min_time))
intermed_done = np.delete(intermed_done, range(min_time))
out = np.delete(out, range(min_time))
out_done = np.delete(out_done, range(min_time))

jobs = np.zeros(steps.shape[0], dtype = int)
times = np.arange(steps.shape[0], dtype = int)

for j in job_min_max.values():
    if j[1] > max_time:
        j[1] = max_time
    if j[0] <= max_time and j[0] <= j[1]:
        jobs[j[0] - min_time:j[1] - min_time] += np.ones(j[1] - j[0], dtype = int)

#plot_labels = ["# Jobs", "# Steps", "Processing", "Processed", "Writing", "Written"]
#plot_lists = [jobs, steps, run, intermed, wait, out]

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

print("Plotting resource usage statistics...")

width, height = 30., 8.
linewidth = width / times[-1]

ntime = 2
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
        ax.xaxis.set_minor_locator(MultipleLocator(xscale * (60 ** timelevel)/2))
        ax.set_xlim(0, xscale * max(times))
    else:
        plt.setp(ax.get_xticklabels(), visible = False)
    axtime += [ax]

# 1) Jobs In Use

plot_label = "In Use"
plot_list = jobs
color = 'gray'

ymax = int(max(jobs))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[0].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[0].transAxes)
axtime[0].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[0].set_ylabel("# Active Jobs")
axtime[0].set_ylim(0, yscale * ymax)
axtime[0].fill_between([xscale * x for x in times], [yscale * y for y in plot_list], color = color, alpha = 0.75)#, markersize = linewidth)

# 2) Steps

plot_labels = ["Processing", "Writing", "Active"]
plot_lists = [intermed, out, steps]
colors = ['blue', 'red', 'purple']

ymax = int(max(steps))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axtime[1].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axtime[1].transAxes)
axtime[1].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axtime[1].set_ylabel("# Docs")
axtime[1].set_ylim(0, yscale * ymax)
for i in range(len(plot_lists)):
    axtime[1].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], color = colors[i], markersize = linewidth, label = plot_labels[i])

axtime[1].legend(bbox_to_anchor = (0.1, 1.21, 0.9, .102), loc = 'upper left', ncol = 3, mode = "expand", borderaxespad = 0.)

# 3) Steps Processed/Written

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
    counts, bins, patches = axhist[i].hist([x for x in plot_lists[i] if x > 0], rwidth = 1, facecolor = colors[i], alpha = 0.75)
    axhist[i].set_xticks(bins)
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