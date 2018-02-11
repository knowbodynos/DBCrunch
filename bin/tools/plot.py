#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

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

def seconds2timestamp(seconds):
    timestamp = ""
    days = str(seconds / (60 * 60 * 24))
    remainder = seconds % (60 * 60 * 24)
    hours = str(remainder / (60 * 60)).zfill(2)
    remainder = remainder % (60 * 60)
    minutes = str(remainder / 60).zfill(2)
    remainder = remainder % 60
    seconds = str(remainder).zfill(2)
    if days != "0":
        timestamp += days + "-"
    timestamp += hours + ":" + minutes# + ":" + seconds
    return timestamp

epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

in_path = sys.argv[1]
out_path = sys.argv[2]
intermed_file_name = sys.argv[3]
out_file_name = sys.argv[4]
try:
    job_limit = int(sys.argv[5])
except IndexError:
    job_limit = None
    time_limit = None
    pass
else:
    try:
        time_limit = timestamp2unit(sys.argv[6])
    except IndexError:
        time_limit = None
        pass

data ={}
all_jobs = []
all_steps = []

min_time = None
max_time = 0

for log_file_path in iglob(in_path + "/*.log.*"):
    log_filename = log_file_path.split("/")[-1]
    log_job = int(log_filename.split("_job_")[1].split("_")[0])
    log_step = int(log_filename.split("_step_")[1].split(".")[0])
    log_state = log_filename.split(".")[1]
    if log_job not in all_jobs:
        all_jobs += [log_job]
    if job_limit == None or log_job <= job_limit:
        with open(log_file_path,"r") as log_stream:
            for log_line in log_stream:
                log_line_split = log_line.rstrip("\n").split()
                if len(log_line_split) < 6:
                    break
                log_timestamp = datetime.datetime.strptime(" ".join(log_line_split[:2]), '%d/%m/%Y %H:%M:%S').replace(tzinfo = utc)
                log_time = int((log_timestamp - epoch).total_seconds())
                log_duration = int(eval(log_line_split[4].rstrip(':')))
                log_id = log_line_split[5]
                if time_limit == None or min_time == None or log_time <= min_time + time_limit:
                    if log_state == ".log.intermed":
                        data[log_id] = {'JOB': log_job, 'STEP': log_step, 'START_TIME': log_time - log_duration, 'INTERMED_TIME': log_time, 'OUT_TIME': None}
                        if (log_job, log_step) not in all_steps:
                            all_steps += [(log_job, log_step)]
                        if min_time == None or log_time - log_duration < min_time:
                            min_time = log_time - log_duration
                    elif log_state == ".log":
                        data[log_id]['OUT_TIME'] = log_time
                        if log_time > max_time:
                            max_time = log_time
                    else:
                        raise Exception("Logs should only consist of .log.intermed and .log files.")

nsteps = []
nsteps_tot = 0
for job in sorted(all_jobs):
    for step in all_steps:
        if step[0] == job:
            nsteps_tot += 1
    nsteps += [nsteps_tot]

time_labels = [seconds2timestamp(x) for x in range(max_time - min_time + 1)]
step_labels = sorted(all_steps)

intermed_img = np.ones((len(all_steps), max_time - min_time + 1, 3))

for d in sorted([x for x in data.values() if x['OUT_TIME'] != None], key = lambda x: (x['JOB'], x['STEP'], -x['INTERMED_TIME'])):
    s = step_labels.index((d['JOB'], d['STEP']))
    for t in range(d['START_TIME'] - min_time, d['INTERMED_TIME'] - min_time):
        intermed_img[s][t] = [0.75, 0, 0]

    intermed_img[s][d['INTERMED_TIME'] - min_time] = [0, 0, 0]

intermed_dpi = 100
intermed_xmargin = 0
intermed_ymargin = 100
intermed_xscale = 20
intermed_yscale = 20
intermed_xpixels, intermed_ypixels = intermed_xscale * (max_time - min_time + 1), intermed_yscale * len(step_labels)
intermed_figsize = ((intermed_xpixels + intermed_xmargin)/intermed_dpi, (intermed_ypixels + intermed_ymargin)/intermed_dpi)

intermed_fig = plt.figure(figsize = intermed_figsize, dpi = intermed_dpi)
intermed_ax = plt.gca()
intermed_ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
plt.title('Job Latency (INTERMED)')
plt.xlabel('Time (s)')
plt.ylabel('(Job, Step)')
intermed_ax.set_xticks(np.arange(0, intermed_xpixels, intermed_xscale * 60 * 60))
intermed_ax.set_xticklabels(time_labels)
intermed_ax.xaxis.set_minor_locator(MultipleLocator(intermed_xscale * 60))
intermed_ax.xaxis.set_minor_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(int(x/intermed_xscale))))
intermed_ax.set_yticks(np.arange(intermed_yscale * 0.5, intermed_ypixels + intermed_yscale * 0.5, intermed_yscale))
intermed_ax.set_yticklabels(step_labels)
plt.imshow(intermed_img, interpolation = 'nearest', extent = [0, intermed_xpixels, 0, intermed_ypixels])
for y in nsteps:
    intermed_ax.axhline(y = intermed_yscale * y)

with PdfPages(out_path + "/" + intermed_file_name) as pdf:
    pdf.savefig(intermed_fig)

out_img = np.ones((len(all_steps), max_time - min_time + 1, 3))

for d in sorted([x for x in data.values() if x['OUT_TIME'] != None], key = lambda x: (x['JOB'], x['STEP'], -x['OUT_TIME'])):
    s = step_labels.index((d['JOB'], d['STEP']))
    for t in range(d['START_TIME'] - min_time, d['INTERMED_TIME'] - min_time):
        out_img[s][t] = [0, 0.75, 0]

    out_img[s][d['INTERMED_TIME'] - min_time] = [0, 0.5, 0]

    for t in range(d['INTERMED_TIME'] - min_time, d['OUT_TIME'] - min_time):
        out_img[s][t] = [0, 0.25, 0]

    out_img[s][d['OUT_TIME'] - min_time] = [0, 0, 0]

out_dpi = 100
out_xmargin = 0
out_ymargin = 100
out_xscale = 20
out_yscale = 20
out_xpixels, out_ypixels = out_xscale * (max_time - min_time + 1), out_yscale * len(step_labels)
out_figsize = ((out_xpixels + out_xmargin)/out_dpi, (out_ypixels + out_ymargin)/out_dpi)

out_fig = plt.figure(figsize = out_figsize, dpi = out_dpi)
out_ax = plt.gca()
out_ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
plt.title('Job Latency (INTERMED)')
plt.xlabel('Time (s)')
plt.ylabel('(Job, Step)')
out_ax.set_xticks(np.arange(0, out_xpixels, out_xscale * 60 * 60))
out_ax.set_xticklabels(time_labels)
out_ax.xaxis.set_minor_locator(MultipleLocator(out_xscale * 60))
out_ax.xaxis.set_minor_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(int(x/out_xscale))))
out_ax.set_yticks(np.arange(out_yscale * 0.5, out_ypixels + out_yscale * 0.5, out_yscale))
out_ax.set_yticklabels(step_labels)
plt.imshow(out_img, interpolation = 'nearest', extent = [0, out_xpixels, 0, out_ypixels])
for y in nsteps:
    out_ax.axhline(y = out_yscale * y)

with PdfPages(out_path + "/" + out_file_name) as pdf:
    pdf.savefig(out_fig)