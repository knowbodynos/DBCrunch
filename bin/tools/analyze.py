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

def create_y_ticker(yscale, yexp):
    return FuncFormatter(lambda y, pos: '%.1f' % (float(y)/(yscale * (10 ** yexp))))

epoch = datetime.datetime(1970, 1, 1, tzinfo = utc)

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

data = {}

job_min_max = {}

min_time = None
max_time = 0

print("Reading log files...")

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
                        if log_id in data.keys():
                            data[log_id].update({'JOB': log_job, 'STEP': log_step, 'START_TIME': log_time - log_duration, 'TEMP_TIME': log_time})
                        else:
                            data[log_id] = {'JOB': log_job, 'STEP': log_step, 'START_TIME': log_time - log_duration, 'TEMP_TIME': log_time, 'OUT_TIME': None}
                        if min_time == None or log_time - log_duration < min_time:
                            min_time = log_time - log_duration
                        if job_min_max[log_job][0] == None or log_time - log_duration < job_min_max[log_job][0]:
                            job_min_max[log_job][0] = log_time - log_duration
                    elif log_state == "OUT":
                        if log_id in data.keys():
                            data[log_id].update({'OUT_TIME': log_time})
                        else:
                            data[log_id] = {'OUT_TIME': log_time}
                        if log_time > max_time:
                            max_time = log_time
                        if log_time > job_min_max[log_job][1]:
                            job_min_max[log_job][1] = log_time
                    else:
                        raise Exception("Log files should only contain \">TEMP\" and \">OUT\" lines.")

duration = max_time - min_time + 1

print("Analyzing resource usage statistics...")

jobs = np.zeros(duration, dtype = int)
steps = np.zeros(duration, dtype = int)
run = np.zeros(duration, dtype = int)
temp = np.zeros(duration, dtype = int)
wait = np.zeros(duration, dtype = int)
out = np.zeros(duration, dtype = int)
temp_arr = np.zeros(duration, dtype = int)

for j in job_min_max.values():
        temp_arr.fill(0)
        temp_arr[j[0] - min_time:j[1] - min_time] = np.ones(j[1] - j[0])
        jobs += temp_arr

for d in data.values():
    if d['OUT_TIME'] != None:
        temp_arr.fill(0)
        temp_arr[d['START_TIME'] - min_time:d['OUT_TIME'] - min_time] = np.ones(d['OUT_TIME'] - d['START_TIME'])
        steps += temp_arr

        temp_arr.fill(0)
        temp_arr[d['START_TIME'] - min_time:d['TEMP_TIME'] - min_time] = np.ones(d['TEMP_TIME'] - d['START_TIME'])
        run += temp_arr

        temp_arr.fill(0)
        temp_arr[d['TEMP_TIME'] - min_time] = 1
        temp += temp_arr

        temp_arr.fill(0)
        temp_arr[d['TEMP_TIME'] - min_time:d['OUT_TIME'] - min_time] = np.ones(d['OUT_TIME'] - d['TEMP_TIME'])
        wait += temp_arr

        temp_arr.fill(0)
        temp_arr[d['OUT_TIME'] - min_time] = 1
        out += temp_arr

times = np.arange(max_time - min_time + 1)

plot_labels = ["# Jobs", "# Steps", "Processing", "Processed", "Writing", "Written"]
plot_lists = [jobs, steps, run, temp, wait, out]

#dpi = 10
#xmargin = 0
#ymargin = 0
xscale = 10
yscale = 5
#xpixels  = xscale * (max_time - min_time + 1)
#ypixels = [yscale * max(p) for p in plot_lists]
#figsize = ((xpixels + xmargin)/dpi, (sum(ypixels) + ymargin)/dpi)

print("Plotting resource usage statistics...")

fig, axarr = plt.subplots(len(plot_labels), sharex = True, figsize = (8, 10))#, dpi = dpi)
plt.suptitle('Resource Usage Statistics for ' + modname + "_" + controllername)
plt.xlabel('Time (HH:MM)')
for i in range(len(axarr)):
    axarr[i].grid(color = 'k', linestyle = '-', linewidth = 0.01)
    if i == len(axarr) - 1:
        axarr[i].xaxis.set_major_locator(MultipleLocator(xscale * 60 * 60))
        axarr[i].xaxis.set_major_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(int(x/xscale))))
        axarr[i].xaxis.set_minor_locator(MultipleLocator(xscale * 60))
        axarr[i].set_xlim(0, xscale * max(times))
        #axarr[i].xaxis.set_minor_formatter(FuncFormatter(lambda x, pos: seconds2timestamp(int(x))))
    #axarr[i].set_yticks(np.arange(0, ypixels[i], yscale))
    #axarr[i].set_yticklabels(step_labels)
    #axarr[i].yaxis.set_major_locator(LinearLocator(10))
    ymax = int(max(plot_lists[i]))
    yexp = len(str(ymax)) - 1
    if yexp != 0:
        axarr[i].text(0, 1, u"\u00D7" + " 10^" + str(yexp), horizontalalignment='left', verticalalignment='bottom', transform=axarr[i].transAxes)
    axarr[i].yaxis.set_major_formatter(create_y_ticker(yscale, yexp))
    #'%.2f' % (float(y)/(yscale * factor))))
    axarr[i].set_ylabel(plot_labels[i])
    axarr[i].set_ylim(0, yscale * max(plot_lists[i]))
    #axarr[i].scatter([xscale * x for x in times], [yscale * y for y in plot_lists[i]], s = 1)
    axarr[i].plot([xscale * x for x in times], [yscale * y for y in plot_lists[i]], markersize = 1)

with PdfPages(out_path + "/" + out_file_name) as pdf:
    pdf.savefig(fig)