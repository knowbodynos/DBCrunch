import sys, datetime, matplotlib
from pytz import utc
from glob import iglob
import numpy as np
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
plt.ioff()

epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

in_path = sys.argv[1]
out_path = sys.argv[2]
temp_file_name = sys.argv[3]
out_file_name = sys.argv[4]

temp_times = []
temp_steps = []
out_times = []
out_steps = []

all_jobs = []
all_steps = []
temp_min_timestamp = None
temp_max_timestamp = 0
out_min_timestamp = None
out_max_timestamp = 0
for log_file_path in iglob(in_path + "/*.log"):
    log_job = int(log_file_path.split("/")[-1].split("_job_")[1].split("_")[0])
    all_jobs += [log_job]
    with open(log_file_path,"r") as log_stream:
        for log_line in log_stream:
            log_line_split = log_line.split()
            log_step = int(log_line_split[1].rstrip(':'))
            if (log_job, log_step) not in all_steps:
                all_steps += [(log_job, log_step)]
            log_time = datetime.datetime.strptime(" ".join(log_line_split[2:4]), '%d/%m/%Y %H:%M:%S').replace(tzinfo = utc)
            log_timestamp = int((log_time - epoch).total_seconds())
            if ">TEMP" in log_line:
                if temp_min_timestamp == None or log_timestamp < temp_min_timestamp:
                    temp_min_timestamp = log_timestamp
                if log_timestamp > temp_max_timestamp:
                    temp_max_timestamp = log_timestamp
                temp_times += [log_timestamp]
                temp_steps += [(log_job, log_step)]
            elif ">OUT" in log_line:
                if out_min_timestamp == None or log_timestamp < out_min_timestamp:
                    out_min_timestamp = log_timestamp
                if log_timestamp > out_max_timestamp:
                    out_max_timestamp = log_timestamp
                out_times += [log_timestamp]
                out_steps += [(log_job, log_step)]

nsteps = []
nsteps_tot = 0
for job in sorted(all_jobs):
    for step in all_steps:
        if step[0] == job:
            nsteps_tot += 1
    nsteps += [nsteps_tot]

job_zpad = 0
step_zpad = 0
for step in all_steps:
    if len(str(step[0])) > job_zpad:
        job_zpad = len(str(step[0]))
    if len(str(step[1])) > step_zpad:
        step_zpad = len(str(step[1]))

step_labels = []
for step in sorted(all_steps):
    step_labels += [str(step[0]).zfill(job_zpad) + "." + str(step[1]).zfill(step_zpad)]

temp_indexes = []
for i in range(len(temp_steps)):
    temp_indexes += [step_labels.index(str(temp_steps[i][0]).zfill(job_zpad) + "." + str(temp_steps[i][1]).zfill(step_zpad))]

temp_img = np.ones((len(step_labels), temp_max_timestamp - temp_min_timestamp + 1, 3))

dpi = 100
xmargin = 0
ymargin = 10
xscale = 20
yscale = 20
xpixels, ypixels = xscale * (temp_max_timestamp - temp_min_timestamp + 1), yscale * len(step_labels)
figsize = ((xpixels + xmargin)/dpi, (ypixels + ymargin)/dpi)

for i in range(len(temp_times)):
    temp_times[i] -= temp_min_timestamp
    time = temp_times[i]
    step = temp_indexes[i]
    temp_img[step][time] = [0, 0, 0]

#fig, ax = plt.subplots(figsize = (8, 20))#ypixels/dpi, xpixels/dpi), dpi = dpi)
fig = plt.figure(figsize = figsize, dpi = dpi)
ax = plt.gca()
ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
plt.title('TEMP Jobs')
plt.xlabel('Time (s)')
plt.ylabel('Step')
ax.set_yticks(np.arange(yscale * 0.5, ypixels + yscale * 0.5, yscale))
ax.set_yticklabels(step_labels)
plt.imshow(temp_img, interpolation = 'nearest', extent = [0, xpixels, 0, ypixels])#, aspect = 0.01)
for y in nsteps:
    ax.axhline(y = yscale * y)

with PdfPages(out_path + "/" + temp_file_name) as pdf:
    pdf.savefig(fig)

out_indexes = []
for i in range(len(out_steps)):
    out_indexes += [step_labels.index(str(out_steps[i][0]).zfill(job_zpad) + "." + str(out_steps[i][1]).zfill(step_zpad))]

out_img = np.ones((len(step_labels), out_max_timestamp - out_min_timestamp + 1, 3))

dpi = 100
xmargin = 0
ymargin = 10
xscale = 20
yscale = 20
xpixels, ypixels = xscale * (out_max_timestamp - out_min_timestamp + 1), yscale * len(step_labels)
figsize = ((xpixels + xmargin)/dpi, (ypixels + ymargin)/dpi)

for i in range(len(out_times)):
    out_times[i] -= out_min_timestamp
    time = out_times[i]
    step = out_indexes[i]
    out_img[step][time] = [0, 0, 0]

#fig, ax = plt.subplots(figsize = (8, 20))#ypixels/dpi, xpixels/dpi), dpi = dpi)
fig = plt.figure(figsize = figsize, dpi = dpi)
ax = plt.gca()
ax.grid(color = 'k', linestyle = '-', linewidth = 0.01)
plt.title('OUT Jobs')
plt.xlabel('Time (s)')
plt.ylabel('Step')
ax.set_yticks(np.arange(yscale * 0.5, ypixels + yscale * 0.5, yscale))
ax.set_yticklabels(step_labels)
plt.imshow(temp_img, interpolation = 'nearest', extent = [0, xpixels, 0, ypixels])#, aspect = 0.01)
for y in nsteps:
    ax.axhline(y = yscale * y)

with PdfPages(out_path + "/" + out_file_name) as pdf:
    pdf.savefig(fig)