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
    timestamp += days + ":" + hours
    return timestamp

def create_y_ticker(yscale, yexp):
    return FuncFormatter(lambda y, pos: '%.1f' % (float(y)/(yscale * (10 ** yexp))))

in_path = sys.argv[1]
out_path = sys.argv[2]
modname = sys.argv[3]
controllername = sys.argv[4]
out_file_name = sys.argv[5]
try:
    job_limit = int(sys.argv[6])
except IndexError:
    job_limit = None
    time_limit = None
    pass
else:
    try:
        time_limit = timestamp2unit(sys.argv[5])
    except IndexError:
        time_limit = None
        pass

with open(out_path + "/crunch_" + modname + "_" + controllername + "_controller.out", "r") as controller_stream:
    controller_line_split = controller_stream.readline().rstrip("\n").split()
    epoch = datetime.datetime.strptime(" ".join(controller_line_split[:4]), '%Y %m %d %H:%M:%S').replace(tzinfo = utc)

#epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

id_times = {}

job_min_max = {}

#min_time = None
#max_time = 0

proc = np.zeros(0, dtype = int)
proc_done = np.zeros(0, dtype = int)
write = np.zeros(0, dtype = int)
write_done = np.zeros(0, dtype = int)

print("Analyzing resource usage statistics...")

for log_file_path in iglob(in_path + "/*.log"):
    with open(log_file_path,"r") as log_stream:
        for log_line in log_stream:
            log_line_split = log_line.rstrip("\n").split()
            if len(log_line_split) < 10:
                break
            log_timestamp = datetime.datetime.strptime(" ".join(log_line_split[:2]), '%d/%m/%Y %H:%M:%S').replace(tzinfo = utc)
            log_time = int((log_timestamp - epoch).total_seconds())
            log_job = int(log_line_split[4].rstrip(':'))
            log_step = int(log_line_split[6].rstrip(':'))
            log_duration = int(eval(log_line_split[8].rstrip(':')))
            log_id, log_state = log_line_split[9].split('>')
            if job_limit == None or log_job <= job_limit:
                if time_limit == None or min_time == None or log_time <= min_time + time_limit:
                    if log_job not in job_min_max.keys():
                        job_min_max[log_job] = [None, 0]
                    if log_state == "TEMP":
                        id_times[log_id] = log_time
                        if proc.shape[0] <= log_time:
                            proc.resize(log_time + 1)
                            proc_done.resize(log_time + 1)
                        proc[log_time - log_duration:log_time + 1] += np.ones(log_duration + 1, dtype = int)
                        proc_done[log_time] += 1
                        #if min_time == None or log_time - log_duration < min_time:
                        #    min_time = log_time - log_duration
                        if job_min_max[log_job][0] == None or log_time - log_duration < job_min_max[log_job][0]:
                            job_min_max[log_job][0] = log_time - log_duration
                    elif log_state == "OUT":
                        if write.shape[0] <= log_time:
                            write.resize(log_time + 1)
                            write_done.resize(log_time + 1)
                        write[id_times[log_id] + 1:log_time + 1] += np.ones(log_time - id_times[log_id], dtype = int)
                        write_done[log_time] += 1
                        #if log_time > max_time:
                        #    max_time = log_time
                        if log_time > job_min_max[log_job][1]:
                            job_min_max[log_job][1] = log_time
                    else:
                        raise Exception("Log files should only contain \">TEMP\" and \">OUT\" lines.")

#if max_time < min_time:
#    max_time = min_time
max_time = max(proc.shape[0], write.shape[0])
proc.resize(max_time)
proc_done.resize(max_time)
write.resize(max_time)
write_done.resize(max_time)
steps = proc + write

min_time = (steps > 0).argmax()
steps = np.delete(steps, range(min_time))
proc = np.delete(proc, range(min_time))
proc_done = np.delete(proc_done, range(min_time))
write = np.delete(write, range(min_time))
write_done = np.delete(write_done, range(min_time))

jobs = np.zeros(steps.shape[0], dtype = int)
times = np.arange(steps.shape[0], dtype = int)

for j in job_min_max.values():
    if j[1] < j[0]:
        j[1] = j[0]
    jobs[j[0] - min_time:j[1] - min_time] += np.ones(j[1] - j[0], dtype = int)

#plot_labels = ["# Jobs", "# Steps", "Processing", "Processed", "Writing", "Written"]
#plot_lists = [jobs, steps, run, temp, wait, out]

#dpi = 10
#xmargin = 0
#ymargin = 0
xscale = int(times[-1] / (60 * 60))
yscale = 5
#xpixels  = xscale * (max_time - min_time + 1)
#ypixels = [yscale * max(p) for p in plot_lists]
#figsize = ((xpixels + xmargin)/dpi, (sum(ypixels) + ymargin)/dpi)

print("Plotting resource usage statistics...")

width, height = 15., 8.
linewidth = width / times[-1]

fig, axarr = plt.subplots(4, sharex = True, figsize = (width, height))#, dpi = dpi)
plt.suptitle('Resource Usage Statistics for ' + modname + "_" + controllername)
plt.xlabel('Time (DD:HH)')

for i in range(len(axarr)):
    axarr[i].grid(color = 'k', linestyle = '-', linewidth = 0.01)
    axarr[i].xaxis.set_major_locator(MultipleLocator(xscale * 60 * 60))
    axarr[i].xaxis.set_major_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(int(x/xscale))))
    axarr[i].xaxis.set_minor_locator(MultipleLocator(xscale * 30 * 60))
    axarr[i].set_xlim(0, xscale * max(times))

# 1) # Jobs In Use

plot_label = "In Use"
plot_list = jobs
color = 'black'

ymax = int(max(jobs))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axarr[0].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axarr[0].transAxes)
axarr[0].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axarr[0].set_ylabel("# Active Jobs")
axarr[0].set_ylim(0, yscale * ymax)
axarr[0].plot([xscale * x for x in times], [yscale * y for y in plot_list], color = color, markersize = linewidth)

# 2) # Steps

plot_labels = ["Processing", "Writing", "Active"]
plot_lists = [proc, write, steps]
colors = ['blue', 'red', 'purple']

ymax = int(max(steps))
yexp = len(str(ymax)) - 1
if yexp != 0:
    axarr[1].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axarr[1].transAxes)
axarr[1].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
axarr[1].set_ylabel("# Docs")
axarr[1].set_ylim(0, yscale * ymax)
for i in range(len(plot_lists)):
    axarr[1].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], color = colors[i], markersize = linewidth, label = plot_labels[i])

axarr[1].legend(bbox_to_anchor = (0.1, 1.21, 0.9, .102), loc = 'upper left', ncol = 3, mode = "expand", borderaxespad = 0.)

# 3) # Steps Processed/Written

plot_labels = ["Processed", "Written"]
plot_lists = [proc_done, write_done]
colors = ['orange', 'green']

for i in range(len(plot_lists)):
    ymax = int(max(plot_lists[i]))
    yexp = len(str(ymax)) - 1
    if yexp != 0:
        axarr[i + 2].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment = 'left', verticalalignment = 'bottom', transform = axarr[i + 2].transAxes)
    axarr[i + 2].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
    axarr[i + 2].set_ylabel("# Docs " + plot_labels[i])
    axarr[i + 2].set_ylim(0, yscale * ymax)
    axarr[i + 2].fill_between([xscale * x for x in times], 0, [yscale * y for y in plot_lists[i]], color = colors[i], linewidth = linewidth)
    #axarr[i + 2].fill_between([xscale * x for x in times], 0, [yscale * (ymax - y) if y < 5 else 0 for y in plot_lists[i]], color = colors[i], linewidth = linewidth)

#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
#plt.legend(bbox_to_anchor = (0., -0.4, 1., .102), loc = 'upper left', ncol = 2, mode = "expand", borderaxespad = 0.)
#plt.subplots_adjust(bottom = 0.2)

plt.subplots_adjust(hspace = 0.4)

with PdfPages(out_path + "/" + out_file_name) as pdf:
    pdf.savefig(fig)