import sys, datetime, matplotlib, warnings
from pytz import utc
from glob import iglob
import numpy as np
matplotlib.use('Agg')
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
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

epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

in_path = sys.argv[1]
out_path = sys.argv[2]
temp_file_name = sys.argv[3]
out_file_name = sys.argv[4]
try:
    job_limit = int(sys.argv[5])
except IndexError:
    job_limit = None
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

for log_file_path in iglob(in_path + "/*.log"):
    log_job = int(log_file_path.split("/")[-1].split("_job_")[1].split("_")[0])
    if log_job not in all_jobs:
        all_jobs += [log_job]
    if job_limit == None or log_job <= job_limit:
        with open(log_file_path,"r") as log_stream:
            for log_line in log_stream:
                log_line_split = log_line.rstrip("\n").split()
                if len(log_line_split) < 8:
                    break
                log_timestamp = datetime.datetime.strptime(" ".join(log_line_split[:2]), '%d/%m/%Y %H:%M:%S').replace(tzinfo = utc)
                log_time = int((log_timestamp - epoch).total_seconds())
                log_step = int(log_line_split[4].rstrip(':'))
                log_duration = int(eval(log_line_split[6].rstrip(':')))
                log_id, log_state = log_line_split[7].split('>')
                if time_limit == None or min_time == None or log_time <= min_time + time_limit:
                    if log_state == "TEMP":
                        data[log_id] = {'JOB': log_job, 'STEP': log_step, 'START_TIME': log_time - log_duration, 'TEMP_TIME': log_time, 'OUT_TIME': None}
                        if (log_job, log_step) not in all_steps:
                            all_steps += [(log_job, log_step)]
                        if min_time == None or log_time - log_duration < min_time:
                            min_time = log_time - log_duration
                    elif log_state == "OUT":
                        data[log_id]['OUT_TIME'] = log_time
                        if log_time > max_time:
                            max_time = log_time
                    else:
                        raise Exception("Log files should only contain \">TEMP\" and \">OUT\" lines.")

nsteps = []
nsteps_tot = 0
for job in sorted(all_jobs):
    for step in all_steps:
        if step[0] == job:
            nsteps_tot += 1
    nsteps += [nsteps_tot]

step_labels = sorted(all_steps)

temp_img = np.ones((len(all_steps), max_time - min_time + 1, 3))

for d in sorted([x for x in data.values() if x['OUT_TIME'] != None], key = lambda x: (x['JOB'], x['STEP'], -x['TEMP_TIME'])):
    s = step_labels.index((d['JOB'], d['STEP']))
    for t in range(d['START_TIME'] - min_time, d['TEMP_TIME'] - min_time):
        temp_img[s][t] = [0.75, 0, 0]

    temp_img[s][d['TEMP_TIME'] - min_time] = [0, 0, 0]

temp_dpi = 100
temp_xmargin = 0
temp_ymargin = 10
temp_xscale = 20
temp_yscale = 20
temp_xpixels, temp_ypixels = temp_xscale * (max_time - min_time + 1), temp_yscale * len(step_labels)
temp_figsize = ((temp_xpixels + temp_xmargin)/temp_dpi, (temp_ypixels + temp_ymargin)/temp_dpi)

temp_fig = plt.figure(figsize = temp_figsize, dpi = temp_dpi)
temp_ax = plt.gca()
temp_ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
plt.title('Job Latency (TEMP)')
plt.xlabel('Time (s)')
plt.ylabel('(Job, Step)')
temp_ax.set_yticks(np.arange(temp_yscale * 0.5, temp_ypixels + temp_yscale * 0.5, temp_yscale))
temp_ax.set_yticklabels(step_labels)
plt.imshow(temp_img, interpolation = 'nearest', extent = [0, temp_xpixels, 0, temp_ypixels])
for y in nsteps:
    temp_ax.axhline(y = temp_yscale * y)

with PdfPages(out_path + "/" + temp_file_name) as pdf:
    pdf.savefig(temp_fig)

out_img = np.ones((len(all_steps), max_time - min_time + 1, 3))

for d in sorted([x for x in data.values() if x['OUT_TIME'] != None], key = lambda x: (x['JOB'], x['STEP'], -x['OUT_TIME'])):
    s = step_labels.index((d['JOB'], d['STEP']))
    for t in range(d['START_TIME'] - min_time, d['TEMP_TIME'] - min_time):
        out_img[s][t] = [0, 0.75, 0]

    out_img[s][d['TEMP_TIME'] - min_time] = [0, 0.5, 0]

    for t in range(d['TEMP_TIME'] - min_time, d['OUT_TIME'] - min_time):
        out_img[s][t] = [0, 0.25, 0]

    out_img[s][d['OUT_TIME'] - min_time] = [0, 0, 0]

out_dpi = 100
out_xmargin = 0
out_ymargin = 10
out_xscale = 20
out_yscale = 20
out_xpixels, out_ypixels = out_xscale * (max_time - min_time + 1), out_yscale * len(step_labels)
out_figsize = ((out_xpixels + out_xmargin)/out_dpi, (out_ypixels + out_ymargin)/out_dpi)

out_fig = plt.figure(figsize = out_figsize, dpi = out_dpi)
out_ax = plt.gca()
out_ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
plt.title('Job Latency (TEMP)')
plt.xlabel('Time (s)')
plt.ylabel('(Job, Step)')
out_ax.set_yticks(np.arange(out_yscale * 0.5, out_ypixels + out_yscale * 0.5, out_yscale))
out_ax.set_yticklabels(step_labels)
plt.imshow(out_img, interpolation = 'nearest', extent = [0, out_xpixels, 0, out_ypixels])
for y in nsteps:
    out_ax.axhline(y = out_yscale * y)

with PdfPages(out_path + "/" + out_file_name) as pdf:
    pdf.savefig(out_fig)