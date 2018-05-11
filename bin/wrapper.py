#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

#    DBCrunch: wrapper.py
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

import sys, os, json, yaml, tempfile, shutil, traceback
from bson import BSON
from fcntl import fcntl, flock, LOCK_EX, LOCK_NB, LOCK_UN, F_GETFL, F_SETFL
from errno import EAGAIN, ENOENT
from datetime import datetime, timedelta
from pytz import utc
from glob import iglob
from pprint import pprint
from time import time, sleep
from signal import signal, SIGPIPE, SIG_DFL
from subprocess import Popen, PIPE
from threading import Thread, active_count
from locale import getpreferredencoding
from argparse import ArgumentParser, REMAINDER
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
from crunch_config import *

def default_sigpipe():
    signal(SIGPIPE, SIG_DFL)

def time_left(config):
    if config.step.maxtime:
        return config.starttime + config.step.maxtime - config.step.buffertime - time()
    else:
        return 1

def nonblocking_readlines(f):
    """Generator which yields lines from F (a file object, used only for
       its fileno()) without blocking.  If there is no data, you get an
       endless stream of empty strings until there is data again (caller
       is expected to sleep for a while).
       Newlines are normalized to the Unix standard.
    """

    #Copyright 2014 Zack Weinberg

    #Permission is hereby granted, free of charge, to any person obtaining a copy
    #of this software and associated documentation files (the "Software"), to deal in 
    #the Software without restriction, including without limitation the rights to use, copy,
    #modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
    #to permit persons to whom the Software is furnished to do so, subject to the following conditions:

    #The above copyright notice and this permission notice shall be included in all copies or
    #substantial portions of the Software.

    #THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
    #BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
    #IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    #WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
    #OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

    fd = f.fileno()
    fl = fcntl(fd, F_GETFL)
    fcntl(fd, F_SETFL, fl | os.O_NONBLOCK)
    enc = getpreferredencoding(False)

    buf = bytearray()
    while True:
        try:
            block = os.read(fd, 8192)
        except OSError:
            yield ""
            continue

        if not block:
            if buf:
                yield buf.decode(enc)
                buf.clear()
            break

        buf.extend(block)

        while True:
            r = buf.find(b'\r')
            n = buf.find(b'\n')
            if r == -1 and n == -1: break

            if r == -1 or r > n:
                yield buf[:(n+1)].decode(enc)
                buf = buf[(n+1):]
            elif n == -1 or n > r:
                yield buf[:r].decode(enc) + '\n'
                if n == r+1:
                    buf = buf[(r+2):]
                else:
                    buf = buf[(r+1):]


class WrapperConfig(Config):
    def __init__(self, **kwargs):
        controller_path = kwargs["controller_path"]
        step_name = kwargs["step_name"]
        step_id = kwargs["step_id"]
        controller_id = step_id.split(".")[0]

        # Initialize Config

        Config.__init__(self, controller_path = controller_path, controller_id = controller_id)

        while True:
            try:
                with open(controller_path + "/jobs/" + step_name + ".step", "r") as step_stream:
                    stepdoc = yaml.load(step_stream)
                    break
            except IOError as e:
                if e.errno != ENOENT:
                    raise
                else:
                    sleep(0.1)

        # Initialize Step

        self.step = self.Objectify(**stepdoc)
        self.step.buffertime = self.job.buffertime
        self.step.id = step_id

        if not "memorylimit" in self.step:
            self.step.memorylimit = None

        if not "timelimit" in self.step:
            self.step.timelimit = None

class AsyncIOStatsStream(WrapperConfig, Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, wm_api, intermed_queue, out_queue, stats_queue, process, stat_names, **kwargs):
        # Assertions

        assert isinstance(intermed_queue, Queue)
        assert isinstance(out_queue, Queue)
        assert isinstance(stats_queue, Queue)
        assert callable(process.stdin.write)

        # Initialize WrapperConfig

        WrapperConfig.__init__(self, **kwargs)

        # Initialize Thread
        
        Thread.__init__(self)

        # Import workload manager module API

        self.__wm_api = wm_api

        # Initialize private variables

        self.__intermed_queue = intermed_queue
        self.__out_queue = out_queue
        self.__stats_queue = stats_queue
        self.__process = process
        self.__out_gen = nonblocking_readlines(process.stdout)
        self.__err_gen = nonblocking_readlines(process.stderr)
        self.__stdin_path = self.controller.path + "/docs/" + self.step.name
        self.__stdin_file = open(self.__stdin_path + ".docs", "a+")
        self.__cleanup_counter = 0
        #self.__refill_reported = False
        self.__proc_smaps = open("/proc/" + str(self.__process.pid) + "/smaps", "r")
        self.__proc_stat = open("/proc/" + str(self.__process.pid) + "/stat", "r")
        self.__proc_uptime = open("/proc/uptime", "r")
        self.__in_elapsed_time = 0
        self.__in_parent_cpu_time = 0
        self.__in_child_cpu_time = 0
        self.__time_keys = ["elapsedtime", "totalcputime", "parentcputime", "childcputime"]
        self.__hz = float(os.sysconf(os.sysconf_names['SC_CLK_TCK']))
        self.__n_stats = 0
        self.__stats = {}
        self.__tot_stats = {}
        self.__max_stats = {}
        for k in stat_names:
            self.__stats[k] = 0
            self.__tot_stats[k] = 0
            self.__max_stats[k] = 0

        self.daemon = True

    def cleanup(self):
        if (self.options.cleanup and self.__cleanup_counter >= self.options.cleanup and not self.__stdin_file.closed) or not time_left(self) > 0:
            while True:
                try:
                    flock(self.__stdin_file, LOCK_EX | LOCK_NB)
                    break
                except IOError as e:
                    if e.errno != EAGAIN:
                        raise
                    else:
                        sleep(0.1)
            with tempfile.NamedTemporaryFile(dir = self.controller.path + "/docs", delete = False) as temp_stream:
                in_line = self.__stdin_file.readline()
                while in_line != "":
                    temp_stream.write(in_line)
                    temp_stream.flush()
                    in_line = self.__stdin_file.readline()
                self.__stdin_file.close()
                os.rename(temp_stream.name, self.__stdin_file.name)
                self.__stdin_file = open(self.__stdin_file.name, "a+")
            flock(self.__stdin_file, LOCK_UN)
            self.__cleanup_counter = 0

    def write_stdin(self):
        #with open(self.controller.path + "/logs/" + self.step.name + ".qqq", "a") as qqq_stream:
        #    qqq_stream.write(str({"refillreported": self.__refill_reported, "stdinfile": os.path.exists(self.__stdin_file.name), "stdinfile_refill": os.path.exists(self.__stdin_file.name.replace(".docs", ".refill")), "stdinfile_closed": self.__stdin_file.closed}) + "\n")
        #    qqq_stream.flush()
        #if self.__refill_reported:
        if self.__stdin_file.closed:
            if os.path.exists(self.__stdin_path + ".docs"):
                self.__stdin_file = open(self.__stdin_path + ".docs", "a+")
                #self.__refill_reported = False
            elif os.path.exists(self.__stdin_path + ".done"):
                self.__process.stdin.write("")#\n")
                self.__process.stdin.flush()
                self.__process.stdin.close()
                os.remove(self.__stdin_path + ".done")
                #self.__refill_reported = False
            
        if not self.__stdin_file.closed:
            in_line = self.__stdin_file.readline().rstrip("\n")
            if in_line == "":
                #if not self.__refill_reported:
                self.__stdin_file.truncate(0)
                if self.options.nrefill and self.__wm_api.is_controller_running(self.cluster.user, self.module.name, self.controller.name):
                    self.__stdin_file.write(json.dumps(self.step.to_dict(), separators = (',', ':')))
                    self.__stdin_file.flush()
                    self.__stdin_file.close()
                    #try:
                    os.rename(self.__stdin_path + ".docs", self.__stdin_path + ".refill")
                    #except OSError:
                    #    print("Fail: " + str((self.__refill_reported, self.__stdin_file.closed)))
                    #    sys.stdout.flush()
                else:
                    self.__stdin_file.close()
                    os.rename(self.__stdin_path + ".docs", self.__stdin_path + ".done")
                self.__refill_reported = True
                self.__cleanup_counter = 0
            else:
                self.__intermed_queue.put(in_line)
                #sys.stdout.write(in_line + "\n")
                #sys.stdout.flush()
                self.__process.stdin.write(in_line + "\n")
                self.__process.stdin.flush()
                self.__cleanup_counter += 1

    def get_stats(self, in_timestamp = None, out_timestamp = None):
        if self.__stats:
            try:
                self.__proc_stat.seek(0)
                stat_line = self.__proc_stat.read()
                self.__proc_uptime.seek(0)
                uptime_line = self.__proc_uptime.read()
                self.__proc_smaps.seek(0)
                smaps_lines = self.__proc_smaps.readlines()
            except IOError:
                pass
            else:
                for k in self.__stats:
                    if not k in self.__time_keys:
                        self.__stats[k.lower()] = 0
                if in_timestamp and any([k in self.__time_keys + ["parentcpuusage", "childcputime", "totalcpuusage"] for k in self.__stats]):
                    stat_line_split = stat_line.split()
                    utime, stime, cutime, cstime = [(float(f) / self.__hz) for f in stat_line_split[13:17]]
                    
                    starttime = float(stat_line_split[21]) / self.__hz
                    uptime = float(uptime_line.split()[0])

                    elapsed_time = uptime - starttime
                    parent_cpu_time = utime + stime
                    child_cpu_time = cutime + cstime
                    total_cpu_time = parent_cpu_time + child_cpu_time
                    #with open(self.controller.path + "/" + self.step.name + ".qqq", "a") as q_stream:
                    #    q_stream.write("Elapsed Time: " + str(elapsed_time) + "\n")
                    #    q_stream.write("Total CPU Time: " + str(total_cpu_time) + "\n\n")
                    #    q_stream.flush()

                    iter_elapsed_time = elapsed_time - self.__in_elapsed_time
                    iter_parent_cpu_time = parent_cpu_time - self.__in_parent_cpu_time
                    iter_child_cpu_time = child_cpu_time - self.__in_child_cpu_time
                    iter_total_cpu_time = iter_parent_cpu_time + iter_child_cpu_time

                    if "parentcpuusage" in self.__stats:
                        iter_parent_cpuusage = 100 * iter_parent_cpu_time / iter_elapsed_time if iter_elapsed_time > 0 else 0
                        self.__stats["parentcpuusage"] = iter_parent_cpuusage
                    if "childcpuusage" in self.__stats:
                        iter_child_cpuusage = 100 * iter_child_cpu_time / iter_elapsed_time if iter_elapsed_time > 0 else 0
                        self.__stats["childcpuusage"] = iter_child_cpuusage
                    if "totalcpuusage" in self.__stats:
                        iter_total_cpuusage = 100 * iter_total_cpu_time / iter_elapsed_time if iter_elapsed_time > 0 else 0
                        self.__stats["totalcpuusage"] = iter_total_cpuusage

                    if not out_timestamp:
                        self.__in_elapsed_time = elapsed_time
                        self.__in_parent_cpu_time = parent_cpu_time
                        self.__in_child_cpu_time = child_cpu_time
                    else:                    
                        if "elapsedtime" in self.__stats:
                            self.__stats["elapsedtime"] = iter_elapsed_time
                        if "parentcputime" in self.__stats:
                            self.__stats["parentcputime"] = iter_parent_cpu_time
                        if "childcputime" in self.__stats:
                            self.__stats["childcputime"] = iter_child_cpu_time
                        if "totalcputime" in self.__stats:
                            self.__stats["totalcputime"] = iter_total_cpu_time

                for smaps_line in smaps_lines:
                    smaps_line_split = smaps_line.split()
                    if len(smaps_line_split) == 3:
                        stat_name, stat_size, stat_unit = smaps_line_split
                        stat_name = stat_name.rstrip(':')
                        if stat_name.lower() in self.__stats:
                            stat_size = int(stat_size)
                            if stat_unit.lower() == 'b':
                                multiplier = 1
                            elif stat_unit.lower() in ['k', 'kb']:
                                multiplier = 1024
                            elif stat_unit.lower() in ['m', 'mb']:
                                multiplier = 1000 * 1024
                            elif stat_unit.lower() in ['g', 'gb']:
                                multiplier = 1000 * 1000 * 1024
                            else:
                                raise Exception(stat_name + " in " + self.__proc_smaps.name + " has unrecognized unit: " + stat_unit)
                            self.__stats[stat_name.lower()] += multiplier * stat_size
                self.__n_stats += 1
                avg_stats = {}
                for k in self.__stats:
                    self.__tot_stats[k] += self.__stats[k]
                    avg_stats.update({k: self.__tot_stats[k] / self.__n_stats if self.__n_stats > 0 else 0})
                    if self.__stats[k] > self.__max_stats[k]:
                        self.__max_stats[k] = self.__stats[k]

            if out_timestamp:
                self.__stats_queue.put({"stats": self.__stats, "max": self.__max_stats, "total": self.__tot_stats, "avg": avg_stats, "in_timestamp": in_timestamp, "out_timestamp": out_timestamp})
                self.__n_stats = 0
                for k in self.__stats:
                    if not k in self.__time_keys:
                        self.__tot_stats[k] = 0
                        self.__max_stats[k] = 0

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        err_flag = False
        self.write_stdin()
        in_timestamp = self.__wm_api.get_timestamp()
        self.get_stats(in_timestamp = in_timestamp)
        self.cleanup()
        while self.__process.poll() == None:
            #if self.__refill_reported:
            if self.__stdin_file.closed:
                self.reload()
                #print("NThreads: " + str(active_count()))
                #sys.stdout.flush()
                self.write_stdin()
                in_timestamp = self.__wm_api.get_timestamp()
                self.get_stats(in_timestamp = in_timestamp)
                self.cleanup()
            try:
                err_line = self.__err_gen.next()
            except StopIteration:
                err_line = ""
                pass
            while err_line != "":
                err_flag = True;
                err_line = err_line.rstrip("\n")
                if not (self.module.ignore and err_line in self.module.ignore):
                    with open(self.controller.path + "/logs/" + self.step.name + ".err", "a") as err_file_stream:
                        err_file_stream.write(err_line + "\n")
                        err_file_stream.flush()
                    sys.stderr.write(self.step.name + ": " + err_line + "\n")
                    sys.stderr.flush()
                self.get_stats()
                try:
                    err_line = self.__err_gen.next()
                except StopIteration:
                    break
                        
            try:
                out_line = self.__out_gen.next()
            except StopIteration:
                out_line = ""
                pass
            while out_line != "":
                out_line = out_line.rstrip("\n")
                if out_line == "":
                    out_timestamp = self.__wm_api.get_timestamp()
                    self.get_stats(in_timestamp = in_timestamp, out_timestamp = out_timestamp)
                    self.reload()
                    #print("NThreads: " + str(active_count()))
                    #sys.stdout.flush()
                    self.write_stdin()
                    in_timestamp = self.__wm_api.get_timestamp()
                    self.get_stats(in_timestamp = in_timestamp)
                    self.cleanup()
                else:
                    self.get_stats(in_timestamp)
                self.__out_queue.put(out_line.rstrip("\n"))
                try:
                    out_line = self.__out_gen.next()
                except StopIteration:
                    break
            self.get_stats()

        if err_flag:
            with open(self.controller.path + "/logs/" + self.step.name + ".err", "a") as err_file_stream:
                if config.step.id:
                    exit_code = self.__wm_api.get_exit_code(self.step.id)
                    err_file_stream.write("Exit Code: " + exit_code)

        self.__stdin_file.close()

class AsyncBulkWriteStream(WrapperConfig, Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, wm_api, db_writer, **kwargs):
        # Initialize WrapperConfig

        WrapperConfig.__init__(self, **kwargs)

        # Initialize Thread
        
        Thread.__init__(self)

        # Import workload manager module API

        self.__wm_api = wm_api

        # Import database writer

        self.__db_writer = db_writer

        # Initialize private variables

        self.__signal = False
        self.__count_bkp_ext_out = {}
        self.__count_log_out = 0
        self.__count_batches_out = 0

        self.daemon = True

    def write_batch(self):
        if self.options.outlog:
            in_write_timestamp = datetime.utcnow().replace(tzinfo = utc)
            in_write_time = time()

        count, log_lines, bkp_ext_lines = self.__db_writer.get_batch(upsert = True)

        self.__count_batches_out += count

        if self.options.outlog:
            write_time = time() - in_write_time
            avg_write_time = write_time / len(log_lines)
            out_log_line_count = 1
            for out_log_line in log_lines:
                if out_log_line != "":
                    start_write_timestamp = (in_write_timestamp + timedelta(seconds = (out_log_line_count - 1) * avg_write_time)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"
                    end_write_timestamp = (in_write_timestamp + timedelta(seconds = out_log_line_count * avg_write_time)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"
                    with open(self.controller.path + "/logs/" + self.step.name + ".log", "a") as out_log_stream:
                        out_log_stream.write(end_write_timestamp + " " + start_write_timestamp + " " + str(dir_size(self.controller.path)) + " " + out_log_line + "\n")
                        out_log_stream.flush()
                    out_log_line_count += 1
                    self.__count_log_out += 1
            
            if self.options.intermedlog and self.__count_batches_out >= self.options.cleanup:
                self.cleanup_intermed_log()
                self.__count_log_out = 0

        if self.options.outlocal or self.options.statslocal:
            for bkp_ext, bkp_lines in bkp_ext_lines.items():
                with open(self.controller.path + "/bkps/" + self.step.name + "." + bkp_ext, "a") as out_bkp_stream:
                    for out_bkp_line in bkp_lines:
                        if out_bkp_line != "":
                            out_bkp_stream.write(out_bkp_line + "\n")
                            out_bkp_stream.flush()
                            if bkp_ext in self.__count_bkp_ext_out:
                                self.__count_bkp_ext_out[bkp_ext] += 1
                            else:
                                self.__count_bkp_ext_out[bkp_ext] = 1

                if self.options.intermedlocal and self.__count_batches_out >= self.options.cleanup:
                    self.cleanup_intermed_bkp()
                    self.__count_bkp_ext_out = {}

            if self.__count_batches_out >= self.options.cleanup:
                self.__count_batches_out = 0

        os.remove(self.controller.path + "/locks/" + self.step.name + ".lock")

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        while not self.__signal:
            while self.__wm_api.is_controller_running(self.cluster.user, self.module.name, self.controller.name) and not self.__db_writer.empty():
                if not os.path.exists(self.controller.path + "/locks/" + self.step.name + ".lock"):
                    if not os.path.exists(self.controller.path + "/locks/" + self.step.name + ".ready"):
                        ready_stream = open(self.controller.path + "/locks/" + self.step.name + ".ready", "w").close()
                else:
                    self.write_batch()
                self.reload()
                #print("NThreads: " + str(active_count()))
                #sys.stdout.flush()

        while self.__wm_api.is_controller_running(self.cluster.user, self.module.name, self.controller.name) and not self.__db_writer.empty():
            if not os.path.exists(self.controller.path + "/locks/" + self.step.name + ".lock"):
                if not os.path.exists(self.controller.path + "/locks/" + self.step.name + ".ready"):
                    ready_stream = open(self.controller.path + "/locks/" + self.step.name + ".ready", "w").close()
            else:
                self.write_batch()
            self.reload()
            #print("NThreads: " + str(active_count()))
            #sys.stdout.flush()

        self.__db_writer.close()

    def cleanup_intermed_log(self):
        with open(self.controller.path + "/logs/" + self.step.name + ".log.intermed", "r") as intermed_log_stream:
            while True:
                try:
                    flock(intermed_log_stream, LOCK_EX | LOCK_NB)
                    break
                except IOError as e:
                    if e.errno != EAGAIN:
                        raise
            with tempfile.NamedTemporaryFile(dir = self.controller.path + "/logs", delete = False) as temp_stream:
                count = 0
                for intermed_log_line in intermed_log_stream:
                    if count >= self.__count_log_out:
                        temp_stream.write(intermed_log_line)
                        temp_stream.flush()
                    count += 1
                os.rename(temp_stream.name, intermed_log_stream.name)
            flock(intermed_log_stream, LOCK_UN)

    def cleanup_intermed_bkp(self):
        for bkp_ext in self.__count_bkp_ext_out.keys():
            with open(self.controller.path + "/bkps/" + self.step.name + "." + bkp_ext + ".intermed", "r") as intermed_bkp_stream:
                while True:
                    try:
                        flock(intermed_bkp_stream, LOCK_EX | LOCK_NB)
                        break
                    except IOError as e:
                        if e.errno != EAGAIN:
                            raise
                with tempfile.NamedTemporaryFile(dir = self.controller.path + "/bkps", delete = False) as temp_stream:
                    count = 0
                    for intermed_bkp_line in intermed_bkp_stream:
                        if count >= self.__count_bkp_ext_out[bkp_ext]:
                            temp_stream.write(intermed_bkp_line)
                            temp_stream.flush()
                        count += 1
                    os.rename(temp_stream.name, intermed_bkp_stream.name)
                flock(intermed_bkp_stream, LOCK_UN)

    def signal(self):
        self.__signal = True

def process_module_output(config, db_writer, intermed_queue, out_queue, stats_queue):
    while not out_queue.empty():
        line = out_queue.get()
        if not (config.module.ignore and line in config.module.ignore):
            line_split = line.split()
            if len(line_split) > 0:
                action = line_split[0]
            else:
                action = "set"
            if line == "":
                if config.options.statsdb:
                    action = "stat"
                else:
                    action = "none"
                stats = stats_queue.get()
                cpu_time = eval("%.4f" % stats["stats"]["totalcputime"])
                max_rss = stats["max"]["rss"]
                max_vmsize = stats["max"]["size"]
                in_intermed_time = stats["in_timestamp"]
                out_intermed_time = stats["out_timestamp"]
                collection = config.db.basecollection
                doc = json.loads(intermed_queue.get())
                index_doc = dict([(x, doc[x]) for x in db_writer.indexes])
                log_line = ""
                if config.options.intermedlog or config.options.outlog:
                    if config.options.intermedlog:
                        with open(config.controller.path + "/logs/" + config.step.name + ".log.intermed", "a") as intermed_log_stream:
                            intermed_log_stream.write(out_intermed_time + " " + in_intermed_time + " " + str(dir_size(config.controller.path)) + " " + ("%.2f" % cpu_time) + " " + str(max_rss) + " " + str(max_vmsize) + " " + str(db_writer.bson_size) + " " + json.dumps(index_doc, separators = (',', ':')) + "\n")
                            intermed_log_stream.flush()
                    if config.options.outlog:
                        log_line = out_intermed_time + " " + in_intermed_time + " " + str(dir_size(config.controller.path)) + " " + ("%.2f" % cpu_time) + " " + str(max_rss) + " " + str(max_vmsize) + " " + str(db_writer.bson_size) + " " + json.dumps(index_doc, separators = (',', ':'))
                stats_mark = {}
                if config.options.statslocal or config.options.statsdb:
                    stats_mark.update({config.module.name + "STATS": {"CPUTIME": cpu_time, "MAXRSS": max_rss, "MAXVMSIZE": max_vmsize, "BSONSIZE": db_writer.bson_size}})
                if config.options.markdone:
                    stats_mark.update({config.module.name + config.options.markdone: True})
                if len(stats_mark) > 0:
                    bkp_ext, bkp_line = db_writer.new_request(action, collection, index_doc, stats_mark)
                    if config.options.statslocal and config.options.intermedlocal:
                        with open(config.controller.path + "/bkps/" + config.step.name + "." + bkp_ext + ".intermed", "a") as intermed_bkp_stream:
                            intermed_bkp_stream.write(bkp_line + "\n")
                            intermed_bkp_stream.flush()
                db_writer.add_to_batch(log_line)
                if db_writer.count == config.options.nbatch:
                    db_writer.put_batch()
                    config.reload()
                    #print("NThreads: " + str(active_count()))
                    #sys.stdout.flush()
            elif line[0] == "#":
                if config.options.intermedlog or config.options.outlog:
                    if config.options.intermedlog:
                        with open(config.controller.path + "/logs/" + config.step.name + ".log.intermed", "a") as intermed_log_stream:
                            intermed_log_stream.write(line + "\n")
                            intermed_log_stream.flush()
                    if config.options.outlog:
                        with open(config.controller.path + "/logs/" + config.step.name + ".log", "a") as out_log_stream:
                            out_log_stream.write(line + "\n")
                            out_log_stream.flush()
            elif len(line_split) == 4:
                collection = line_split[1]
                index_doc = json.loads(line_split[2])
                doc = json.loads(line_split[3])
                bkp_ext, bkp_line = db_writer.new_request(action, collection, index_doc, doc)
                if config.options.intermedlocal:
                    with open(config.controller.path + "/bkps/" + config.step.name + "." + bkp_ext + ".intermed", "a") as intermed_bkp_stream:
                        intermed_bkp_stream.write(bkp_line + "\n")
                        intermed_bkp_stream.flush()
            else:
                try:
                    #print(line)
                    #sys.stdout.flush()
                    raise IndexError("Modules should only output commented lines, blank lines separating processed input documents, or line with 4 columns representing: collection name, update action, index document, output document.")
                except IndexError as e:
                    with open(config.controller.path + "/logs/" + config.step.name + ".err", "a") as err_file_stream:
                        traceback.print_exc(file = err_file_stream)
                        err_file_stream.flush()
                    raise

# Load arguments

parser = ArgumentParser()

parser.add_argument('controller_path', help = '')
parser.add_argument('step_id', help = '')
parser.add_argument('step_name', help = '')
parser.add_argument('stats', nargs = '+', default = [], help = '')

kwargs = vars(parser.parse_known_args()[0])

# Configure wrapper

config = WrapperConfig(**kwargs)

# Import workload manager API

wm_api = __import__("crunch_wm_" + config.cluster.wm.api)

# Initialize stats

stat_names = [x.lower() for x in kwargs["stats"]]
for req_stat in ["totalcputime", "rss", "size"]:
    if not req_stat in stat_names:
        stat_names.append(req_stat)

# Initialize queues

intermed_queue = Queue()
out_queue = Queue()
stats_queue = Queue()

# Run module
script = ""
if config.module.command:
    script += config.module.command + " "
if config.module.flags:
    script += " ".join(config.module.flags) + " "
script += config.cluster.root + "/modules/modules/" + config.module.name + "/" + config.module.name + config.module.extension
if config.module.args:
    script += " " + " ".join(config.module.args)
process = Popen(script, shell = True, stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize = 1, preexec_fn = default_sigpipe)

# Initialize database writer

db_api = __import__("crunch_db_" + config.db.api)

db_writer = db_api.DatabaseWriter(config.db, out_local = config.options.outlocal,
                                             out_db = config.options.outdb,
                                             stats_local = config.options.statslocal,
                                             stats_db = config.options.statsdb,
                                             ordered = False)

# Initialize stats thread

reader = AsyncIOStatsStream(wm_api, intermed_queue, out_queue, stats_queue, process, stat_names, **kwargs)
reader.start()

# Initialize bulk write thread

writer = AsyncBulkWriteStream(wm_api, db_writer, **kwargs)
writer.start()

# Remove step file

step_path = kwargs["controller_path"] + "/jobs/" + kwargs["step_name"] + ".step"
while not os.path.exists(step_path):
    sleep(0.1)
os.remove(step_path)

# Start wrapper body

while True:
    try:
        flock(sys.stdout, LOCK_EX | LOCK_NB)
        break
    except IOError as e:
        if e.errno != EAGAIN:
            raise
        else:
            sleep(0.1)

print(wm_api.get_timestamp() + " " + config.step.name + " START")
sys.stdout.flush()
flock(sys.stdout, LOCK_UN)

while reader.is_alive():
    process_module_output(config, db_writer, intermed_queue, out_queue, stats_queue)

reader.join()
process.stdin.close()
process.stdout.close()
process.stderr.close()

process_module_output(config, db_writer, intermed_queue, out_queue, stats_queue)

if db_writer.count > 0:
    db_writer.put_batch()

writer.signal()
writer.join()

# End wrapper body

config.reload()
#print("NThreads: " + str(active_count()))
#sys.stdout.flush()

while True:
    try:
        flock(sys.stdout, LOCK_EX | LOCK_NB)
        break
    except IOError as e:
        if e.errno != EAGAIN:
            raise
        else:
            sleep(0.1)

print(wm_api.get_timestamp() + " " + config.step.name + " END")
sys.stdout.flush()
flock(sys.stdout, LOCK_UN)

# Tie up loose ends

db_writer.close()

if config.options.intermedlog:
    os.remove(config.controller.path + "/logs/" + config.step.name + ".log.intermed")

if config.options.intermedlocal:
    for intermed_bkp_file_name in iglob(config.controller.path + "/bkps/" + config.step.name + ".*.intermed"):
        os.remove(intermed_bkp_file_name)