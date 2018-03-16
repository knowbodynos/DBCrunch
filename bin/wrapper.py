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

import sys, os, json, yaml, traceback, tempfile, datetime, linecache
from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN
from errno import EAGAIN
from pytz import utc
from glob import iglob
from pprint import pprint
from time import time, sleep
from random import randint
from signal import signal, SIGPIPE, SIG_DFL
from subprocess import Popen, PIPE
from threading import Thread, active_count
from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read
from locale import getpreferredencoding
from argparse import ArgumentParser, REMAINDER
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

def PrintException():
    "If an exception is raised, print traceback of it to output log."
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
    print "More info: ", traceback.format_exc()

def default_sigpipe():
    signal(SIGPIPE, SIG_DFL)

def get_timestamp():
    return datetime.datetime.utcnow().replace(tzinfo = utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"

def dir_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.stat(fp).st_blocks * 512
            except OSError:
                pass
    return total_size

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
    fcntl(fd, F_SETFL, fl | O_NONBLOCK)
    enc = getpreferredencoding(False)

    buf = bytearray()
    while True:
        try:
            block = read(fd, 8192)
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

'''
class AsynchronousThreadStreamReader(Thread):
    "Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread."
    def __init__(self, stream, queue):
        assert isinstance(queue, Queue)
        assert callable(stream.readline)
        Thread.__init__(self)
        self._stream = stream
        self._queue = queue
        self.daemon = True

    def run(self):
        "The body of the thread: read lines and put them on the queue."
        for line in iter(self._stream.readline, ''):
            self._queue.put(line)

    def eof(self):
        "Check whether there is no more content to expect."
        return (not self.is_alive()) and self._queue.empty()
'''

class AsyncIOStatsStream(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, modname, controllername, controllerpath, jobstepname, pid, in_stream, out_stream, err_stream, stdin_args, in_queue, intermed_queue, out_queue, nrefill = None, partition = None, nsteptasks = None, nstepcpus = None, stepmem = None, stepid = None, ignoredstrings = [], stats = None, stats_queue = None, delimiter = '', cleanup = None, time_limit = None, start_time = None):
        assert hasattr(stdin_args, '__iter__')
        #assert isinstance(stdin_file, file) or stdin_file == None
        assert isinstance(in_queue, Queue)
        assert isinstance(out_queue, Queue)
        #assert isinstance(err_queue, Queue)
        assert callable(in_stream.write)
        #assert callable(out_stream.readline)
        #assert callable(err_stream.readline)
        #assert callable(err_stream.write)
        Thread.__init__(self)
        self._controllerpath = controllerpath
        self._jobstepname = jobstepname
        self._pid = str(pid)
        self._instream = in_stream
        #self._outstream = out_stream
        self._outgen = nonblocking_readlines(out_stream)
        self._errgen = nonblocking_readlines(err_stream)
        self._stdinargs = stdin_args
        #self._stdinfile = stdin_file
        self._stdinfilename = self._controllerpath + "/docs/" + self._jobstepname + ".docs"
        if os.path.exists(self._stdinfilename):
            self._stdinfile = open(self._stdinfilename, "a+")
            self._stdinfile.seek(0)
        else:
            self._stdinfile = None
        self._stdinargsflag = True
        #self._stdinfileflag = False
        self._inqueue = in_queue
        self._intermedqueue = intermed_queue
        self._outqueue = out_queue
        #self._errqueue = err_queue
        self._username = os.environ['USER']
        self._modname = modname
        self._controllername = controllername
        self._delimiter = delimiter
        self._cleanup = cleanup
        self._cleanup_counter = 0
        self._timelimit = time_limit
        self._refillreported = False
        #self._prevtimestamp = get_timestamp()
        self._starttime = start_time
        self._nrefill = nrefill
        self._partition = partition
        self._nsteptasks = nsteptasks
        #self._cpuspertask = cpuspertask
        self._nstepcpus = nstepcpus
        self._stepmem = stepmem
        self._stepid = stepid
        #self._nlocks = len(os.listdir(self._controllerpath + "/locks"))
        self._ignoredstrings = ignoredstrings
        self._signal = False
        if stats == None or stats_queue == None:
            self._stats = stats
        else:
            self._stats = dict((s.lower(), 0) for s in stats)
            self._proc_smaps = open("/proc/" + str(pid) + "/smaps", "r")
            self._proc_stat = open("/proc/" + str(pid) + "/stat", "r")
            self._proc_uptime = open("/proc/uptime", "r")
            self._in_elapsedtime = 0
            self._in_parent_cputime = 0
            self._in_child_cputime = 0
            #self._prev_total_cputime = 0
            self._maxstats = dict((s.lower(), 0) for s in stats)
            self._totstats = dict((s.lower(), 0) for s in stats)
            self._nstats = 0
            self._time_keys = ["elapsedtime", "totalcputime", "parentcputime", "childcputime"]
            self._hz = float(os.sysconf(os.sysconf_names['SC_CLK_TCK']))
            self._statsqueue = stats_queue

        self.daemon = True

    '''
    def write_stdin(self):
        try:
            in_line = self._stdinargs.next()
        except StopIteration:
            self._stdinargsflag = True
            if self._stdinfile == None or self._stdinfile.closed:
                #in_line = self._inqueue.get()
                in_line = sys.stdin.readline().rstrip("\n")
                if in_line == "":
                    self._instream.close()
                    #print("finally!")
                    #sys.stdout.flush()
                else:
                    self._intermedqueue.put(in_line)
                    self._instream.write(in_line + self._delimiter)
                    #print("a: " + in_line + self._delimiter)
                    #sys.stdout.flush()
                    self._instream.flush()
            else:
                in_line = self._stdinfile.readline().rstrip("\n")
                if in_line == "":
                    name = self._stdinfile.name
                    self._stdinfile.close()
                    os.remove(name)
                    #self._stdinfileflag = True
                    #in_line = self._inqueue.get()
                    in_line = sys.stdin.readline().rstrip("\n")
                    if in_line == "":
                        self._instream.write(in_line + self._delimiter)
                        self._instream.close()
                        #print("finally!")
                        #sys.stdout.flush()
                    else:
                        self._intermedqueue.put(in_line)
                        self._instream.write(in_line + self._delimiter)
                        #print("a: " + in_line + self._delimiter)
                        #sys.stdout.flush()
                        self._instream.flush()
                else:
                    self._intermedqueue.put(in_line)
                    self._instream.write(in_line + self._delimiter)
                    #print("a: " + in_line + self._delimiter)
                    #sys.stdout.flush()
                    self._instream.flush()
                    if (self._cleanup != None and self._cleanup_counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time() - self._starttime >= self._timelimit):
                        with tempfile.NamedTemporaryFile(dir = "/".join(self._stdinfile.name.split("/")[:-1]), delete = False) as tempstream:
                            in_line = self._stdinfile.readline()
                            while in_line != "":
                                tempstream.write(in_line)
                                tempstream.flush()
                                in_line = self._stdinfile.readline()
                            name = self._stdinfile.name
                            self._stdinfile.close()
                            os.rename(tempstream.name, name)
                            self._stdinfile = open(name, 'r')
                        self._cleanup_counter = 0
        else:
            self._intermedqueue.put(in_line)
            self._instream.write(in_line + self._delimiter)
            #print("a: " + in_line + self._delimiter)
            #sys.stdout.flush()
            self._instream.flush()
            #self._instream.close()
            #print(in_line)
            #sys.stdout.flush()
    '''

    #def print_here(self, line):
    #    with open(self._controllerpath + "/logs/" + self._jobstepname + ".q", "a") as here:
    #        here.write(line + "\n")
    #        here.flush()

    def update_config(self):
        with open(self._controllerpath + "/" + self._modname + "_" + self._controllername + ".config", "r") as controllerconfigstream:
            controllerconfigdoc = yaml.load(controllerconfigstream)
        self._cleanup = controllerconfigdoc['options']['cleanup']

    def cleanup(self):
        if (self._cleanup != None and self._cleanup_counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time() - self._starttime >= self._timelimit):
            #with open(self._stdinfilename + "2", "a") as docstream2:
            #    docstream2.write("CLEAN\n")
            #    docstream2.flush()
            while True:
                try:
                    flock(self._stdinfile, LOCK_EX | LOCK_NB)
                    break
                except IOError as e:
                    if e.errno != EAGAIN:
                        raise
                    else:
                        sleep(0.1)
            with tempfile.NamedTemporaryFile(dir = "/".join(self._stdinfile.name.split("/")[:-1]), delete = False) as tempstream:
                in_line = self._stdinfile.readline()
                while in_line != "":
                    tempstream.write(in_line)
                    tempstream.flush()
                    in_line = self._stdinfile.readline()
                name = self._stdinfile.name
                self._stdinfile.close()
                os.rename(tempstream.name, name)
                self._stdinfile = open(name, "a+")
            self._stdinfile.seek(0)
            flock(self._stdinfile, LOCK_UN)
            self._cleanup_counter = 0

    def write_stdin(self):
        #self.print_here(str((self._stdinfile.name, self._stdinfile.closed, self._stdinfile)))
        #    self._stdinfile = open(self._stdinfile.name, "r")
        if self._refillreported:
            if os.path.exists(self._stdinfilename):
                self._stdinfile = open(self._stdinfilename, "a+")
                self._stdinfile.seek(0)
                self._refillreported = False
                self._cleanup_counter = 0
            elif os.path.exists(self._stdinfilename.replace(".docs", ".done")):
                self._stdinfile = open(self._stdinfilename.replace(".docs", ".done"), "a+")
                self._stdinfile.seek(0)
                self._refillreported = False
                self._cleanup_counter = 0

            #self._refillreported = False
        if self._stdinfile != None and not self._stdinfile.closed:
            #if self._refillreported:
            #    self._stdinfile.seek(0)
            in_line = self._stdinfile.readline().rstrip("\n")
            #if self._refillreported:
            #    self.print_here(get_timestamp() + " FIRST " + in_line)
            if in_line == "":
                stdindone = os.path.exists(self._stdinfilename.replace(".docs", ".done"))
                if self._nrefill != None and (not stdindone) and get_controllerrunningq(self._username, self._modname, self._controllername):
                    if not self._refillreported:
                        #with open(self._stdinfilename + "2", "a") as docstream2:
                        #    docstream2.write("END\n")
                        #    docstream2.flush()
                        #self.print_here(get_timestamp() + " REFILL")
                        self._stdinfile.truncate(0)
                        self._stdinfile.write(self._jobstepname)
                        if self._stepid != None:
                            self._stdinfile.write(" " + self._stepid)
                        if self._partition != None:
                            self._stdinfile.write(" " + self._partition)
                        if self._nsteptasks != None:
                            self._stdinfile.write(" " + self._nsteptasks)
                        #if self._cpuspertask != None:
                        #    self._stdinfile.write(" " + self._cpuspertask)
                        if self._nstepcpus != None:
                            self._stdinfile.write(" " + self._nstepcpus)
                        if self._stepmem != None:
                            self._stdinfile.write(" " + self._stepmem)
                        self._stdinfile.flush()
                        name = self._stdinfile.name
                        self._stdinfile.close()
                        newname = name.replace(".docs", ".refill")
                        os.rename(name, newname)
                        #with open(self._controllerpath + "/refill", "a") as refillstream:
                        #    while True:
                        #        try:
                        #            flock(refillstream, LOCK_EX | LOCK_NB)
                        #            break
                        #        except IOError as e:
                        #            if e.errno != EAGAIN:
                        #                raise
                        #            else:
                        #                sleep(0.1)
                        #    refillstream.seek(0, 2)
                        #    refillstream.write(self._jobstepname)
                        #    if self._stepid != None:
                        #        refillstream.write(" " + self._stepid)
                        #    if self._partition != None:
                        #        refillstream.write(" " + self._partition)
                        #    if self._nsteptasks != None:
                        #        refillstream.write(" " + self._nsteptasks)
                        #    #if self._cpuspertask != None:
                        #    #    refillstream.write(" " + self._cpuspertask)
                        #    if self._nstepcpus != None:
                        #        refillstream.write(" " + self._nstepcpus)
                        #    if self._stepmem != None:
                        #        refillstream.write(" " + self._stepmem)
                        #    refillstream.write("\n")
                        #    refillstream.flush()
                        #    flock(refillstream, LOCK_UN)
                        #    #self._stdinfile.truncate(0)
                        self._refillreported = True
                else:
                    #self.print_here(get_timestamp() + " END")
                    self._instream.write(in_line)
                    self._instream.flush()
                    self._instream.close()
                    name = self._stdinfile.name
                    if stdindone:
                        name = name.replace(".docs", ".done")
                    self._stdinfile.close()
                    os.remove(name)
            else:
                #if self._refillreported:
                    #a = json.loads(in_line)
                    #self.print_here(get_timestamp() + " FIRSTLINE " + in_line)#str({"POLYID": a["POLYID"]}))
                #    self._refillreported = False
                #    self._cleanup_counter = 0
                    #self.print_here(get_timestamp() + " FIRST")
                self._intermedqueue.put(in_line)
                self._instream.write(in_line + self._delimiter)
                self._instream.flush()
                self._cleanup_counter += 1

    #def set_stats(self):
    #    try:
    #        self._proc_stat.seek(0)
    #        stat_line = self._proc_stat.read()
    #        self._proc_uptime.seek(0)
    #        uptime_line = self._proc_uptime.read()
    #    except IOError:
    #        pass
    #    else:
    #        if any([k in self._stats.keys() for k in self._time_keys]):
    #            stat_line_split = stat_line.split()
    #            utime, stime, cutime, cstime = [(float(f) / self._hz) for f in stat_line_split[13:17]]
    #            
    #            starttime = float(stat_line_split[21]) / self._hz
    #            if self._in_elapsedtime == 0:
    #                self._in_elapsedtime = starttime
    #            uptime = float(uptime_line.split()[0])
    #            elapsedtime = uptime - starttime
    #
    #            parent_cputime = utime + stime
    #            child_cputime = cutime + cstime
    #
    #            self._in_elapsedtime = uptime
    #            self._in_parent_cputime = parent_cputime
    #            self._in_child_cputime = child_cputime

    def get_stats(self, in_timestamp, out_timestamp = None):
        if self._stats != None:
            if out_timestamp != None:
                prev_elapsedtime = self._in_elapsedtime
                prev_parent_cputime = self._in_parent_cputime
                prev_child_cputime = self._in_child_cputime

            try:
                self._proc_stat.seek(0)
                stat_line = self._proc_stat.read()
                self._proc_uptime.seek(0)
                uptime_line = self._proc_uptime.read()
                self._proc_smaps.seek(0)
                smaps_lines = self._proc_smaps.readlines()
            except IOError:
                pass
            else:
                for k in self._stats.keys():
                    if not k in self._time_keys:
                        self._stats[k.lower()] = 0
                if any([k in self._time_keys + ["parentcpuusage", "childcputime", "totalcpuusage"] for k in self._stats.keys()]):
                    stat_line_split = stat_line.split()
                    utime, stime, cutime, cstime = [(float(f) / self._hz) for f in stat_line_split[13:17]]
                    
                    starttime = float(stat_line_split[21]) / self._hz
                    uptime = float(uptime_line.split()[0])

                    self._in_elapsedtime = uptime - starttime
                    self._in_parent_cputime = utime + stime
                    self._in_child_cputime = cutime + cstime
                    total_cputime = self._in_parent_cputime + self._in_child_cputime

                    if out_timestamp != None:
                        iter_elapsedtime = self._in_elapsedtime - prev_elapsedtime
                        iter_parent_cputime = self._in_parent_cputime - prev_parent_cputime
                        iter_child_cputime = self._in_child_cputime - prev_child_cputime
                        iter_total_cputime = iter_parent_cputime + iter_child_cputime
                    
                        for k in self._stats.keys():
                            if k.lower() == "elapsedtime":
                                self._stats[k] = iter_elapsedtime
                            if k.lower() == "totalcputime":
                                self._stats[k] = iter_total_cputime
                            if k.lower() == "parentcputime":
                                self._stats[k] = iter_parent_cputime
                            if k.lower() == "childcputime":
                                self._stats[k] = iter_child_cputime
                            if k.lower() == "parentcpuusage":
                                iter_parent_cpuusage = 100 * iter_parent_cputime / iter_elapsedtime if iter_elapsedtime > 0 else 0
                                self._stats[k] = iter_parent_cpuusage
                            if k.lower() == "childcpuusage":
                                iter_child_cpuusage = 100 * iter_child_cputime / iter_elapsedtime if iter_elapsedtime > 0 else 0
                                self._stats[k] = iter_child_cpuusage
                            if k.lower() == "totalcpuusage":
                                iter_total_cpuusage = 100 * iter_total_cputime / iter_elapsedtime if iter_elapsedtime > 0 else 0
                                self._stats[k] = iter_total_cpuusage

                for smaps_line in smaps_lines:
                    smaps_line_split = smaps_line.split()
                    if len(smaps_line_split) == 3:
                        stat_name, stat_size, stat_unit = smaps_line_split
                        stat_name = stat_name.rstrip(':')
                        if stat_name.lower() in self._stats.keys() and not stat_name.lower() in self._time_keys:
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
                                raise Exception(stat_name + " in " + self._proc_smaps.name + " has unrecognized unit: " + stat_unit)
                            self._stats[stat_name.lower()] += multiplier * stat_size
                self._nstats += 1
                avgstats = {}
                for k in self._stats.keys():
                    self._totstats[k] += self._stats[k]
                    avgstats.update({k: self._totstats[k] / self._nstats if self._nstats > 0 else 0})
                    if self._stats[k] > self._maxstats[k]:
                        self._maxstats[k] = self._stats[k]

            if out_timestamp != None:
                self._statsqueue.put({"stats": self._stats, "max": self._maxstats, "total": self._totstats, "avg": avgstats, "in_timestamp": in_timestamp, "out_timestamp": out_timestamp})
                self._nstats = 0
                for k in self._stats.keys():
                    if not k in self._time_keys:
                        self._totstats[k] = 0
                        self._maxstats[k] = 0
            #sleep(self._statsdelay)

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        errflag = False
        self.update_config()
        self.write_stdin()
        in_timestamp = get_timestamp()
        self.get_stats(in_timestamp)
        self.cleanup()
        while self.is_inprog():
            if self._refillreported:
                self.update_config()
                self.write_stdin()
                in_timestamp = get_timestamp()
                self.get_stats(in_timestamp)
                self.cleanup()
            #print("NThreads: " + str(active_count()))
            #sys.stdout.flush()
            #self.update_config()
            #self.write_stdin()
            try:
                err_line = self._errgen.next()
            except StopIteration:
                err_line = ""
                pass
            while err_line != "":
                #print("NThreads: " + str(active_count()))
                #sys.stdout.flush()
                errflag = True;
                err_line = err_line.rstrip("\n")
                if err_line not in self._ignoredstrings:
                    with open(self._controllerpath + "/logs/" + self._jobstepname + ".err", "a") as errfilestream:
                        errfilestream.write(err_line + "\n")
                        errfilestream.flush()
                    sys.stderr.write(self._jobstepname + ": " + err_line + "\n")
                    sys.stderr.flush()
                self.get_stats(in_timestamp)
                try:
                    err_line = self._errgen.next()
                except StopIteration:
                    break
                #self._nlocks = len(os.listdir(self._controllerpath + "/locks"))
                        
            try:
                out_line = self._outgen.next()
            except StopIteration:
                out_line = ""
                pass
            while out_line != "":
                #print("NThreads: " + str(active_count()))
                #sys.stdout.flush()
                #for out_line in iter(self._outstream.readline, ''):
                out_line = out_line.rstrip("\n")
                #with open(self._controllerpath + "/logs/" + self._jobstepname + ".test", "a") as teststream:
                #    teststream.write(get_timestamp() + " UTC: Out: " + out_line + "\n")
                #    teststream.flush()
                #if out_line == "\n".decode('string_escape'):
                #sys.stdout.write(self._controllerpath + "/logs/" + self._jobstepname + " out_line: \"" + out_line + "\"\n")
                #sys.stdout.flush()
                if out_line == "":
                    out_timestamp = get_timestamp()
                    self.get_stats(in_timestamp, out_timestamp = out_timestamp)
                    #self._cleanup_counter += 1
                    self.update_config()
                    self.write_stdin()
                    in_timestamp = get_timestamp()
                    self.get_stats(in_timestamp)
                    self.cleanup()
                else:
                    self.get_stats(in_timestamp)
                self._outqueue.put(out_line.rstrip("\n"))
                try:
                    out_line = self._outgen.next()
                except StopIteration:
                    break
                #self.update_config()
                #self.write_stdin()
                #self._nlocks = len(os.listdir(self._controllerpath + "/locks"))
            #self._nlocks = len(os.listdir(self._controllerpath + "/locks"))
            self.get_stats(in_timestamp)

        if errflag:
            with open(self._controllerpath + "/logs/" + self._jobstepname + ".err", "a") as errfilestream:
                if self._stepid != None:
                    exitcode = get_exitcode(self._stepid)
                    errfilestream.write("ExitCode: " + exitcode)

        self._stdinfile.close()

        #while not self._signal:
        #    self._nlocks = len(os.listdir(self._controllerpath + "/locks"))

    #def waiting(self):
    #    return self.is_alive() and self._stdinargsflag and self._stdinfile.closed and self._inqueue.empty() and self._outqueue.empty()

    def nlocks(self):
        return self._nlocks

    def is_inprog(self):
        '''Check whether there is no more content to expect.'''
        try:
            return os.path.exists("/proc/" + self._pid) and self.is_alive()
        except IOError:
            return False

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return self._stdinargsflag and self._stdinfile.closed and self._inqueue.empty() and self._outqueue.empty()

    def signal(self):
        self._signal = True

    def stat(self, stat_name):
        return self._stats[stat_name.lower()]

    def stats(self):
        return self._stats

    def max_stat(self, stat_name):
        return self._maxstats[stat_name.lower()]

    def max_stats(self):
        return self._maxstats

    def avg_stat(self, stat_name):
        return self._totstats[stat_name.lower()] / self._nstats if self._nstats > 0 else 0

    def avg_stats(self):
        return dict((k, self._totstats[k] / self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())

class AsyncBulkWriteStream(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, modname, controllername, controllerpath, jobstepname, write_queue, controllerconfigdoc, stepid = None):
        #assert isinstance(outlogstream, file) or outlogstream == None
        assert isinstance(write_queue, Queue)
        Thread.__init__(self)
        self._controllerpath = controllerpath
        self._modname = modname
        self._controllername = controllername
        self._jobstepname = jobstepname
        self._writequeue = write_queue
        self._configdb = controllerconfigdoc['db']
        self._configopt = controllerconfigdoc['options']
        self._stepid = stepid
        #self._nlocks = len(os.listdir(self._controllerpath + "/locks"))
        #self._locksstream = open(self._controllerpath + "/locks", "r+")
        self._signal = False
        self._bulkcolls = {}
        self._countioouts = {}
        self._countlogout = 0
        self._countbatchesout = 0

        if self._configdb['type'] == "mongodb":
            if controllerconfigdoc['db']['username'] == None:
                self._dbclient = MongoClient("mongodb://" + str(controllerconfigdoc['db']['host']) + ":" + str(controllerconfigdoc['db']['port']) + "/" + str(controllerconfigdoc['db']['name']))
            else:
                self._dbclient = MongoClient("mongodb://" + str(controllerconfigdoc['db']['username']) + ":" + str(controllerconfigdoc['db']['password']) + "@" + str(controllerconfigdoc['db']['host']) + ":" + str(controllerconfigdoc['db']['port']) + "/" + str(controllerconfigdoc['db']['name']) + "?authMechanism=SCRAM-SHA-1")
            self._db = self._dbclient[self._configdb['name']]
        else:
            raise Exception("Only \"mongodb\" is currently supported.")

        self.daemon = True

    def update_config(self):
        with open(self._controllerpath + "/" + self._modname + "_" + self._controllername + ".config", "r") as controllerconfigstream:
            controllerconfigdoc = yaml.load(controllerconfigstream)
        self._configdb = controllerconfigdoc['db']
        self._configopt = controllerconfigdoc['options']

    #def print_here(self, line):
    #    with open(self._controllerpath + "/logs/" + self._jobstepname + ".q", "a") as here:
    #        here.write(line + "\n")
    #        here.flush()

    def write_batch(self):
        #while True:
        #    try:
        #        flock(self._locksstream, LOCK_EX | LOCK_NB)
        #        break
        #    except IOError as e:
        #        if e.errno != EAGAIN:
        #            raise
        #        else:
        #            sleep(0.1)
        #self._locksstream.seek(0)
        #locksline = self._locksstream.readline()
        #if locksline.isdigit():
        #    nlocks = int(locksline)
        #    if nlocks >= self._configopt['nworkers']:
        #        flock(self._locksstream, LOCK_UN)
        #    else:
        #        self._locksstream.seek(0)
        #        self._locksstream.truncate(0)
        #        self._locksstream.write(str(nlocks + 1))
        #        self._locksstream.flush()
        #        flock(self._locksstream, LOCK_UN)

        #if len(os.listdir(self._controllerpath + "/locks")) < self._configopt['nworkers']:
        write_dict = self._writequeue.get()
        #lockfile = self._controllerpath + "/locks/" + self._jobstepname + ".lock"
        #with open(lockfile, 'w') as lockstream:
        #    if self._stepid != None:
        #        lockstream.write(self._stepid + ": ")
        #        lockstream.flush()
        #    lockstream.write("Writing " + str(write_dict['count']) + " items.")
        #    lockstream.flush()
        
        self._countbatchesout += write_dict['count']

        if self._configopt['outlog']:
            in_writetimestamp = datetime.datetime.utcnow().replace(tzinfo = utc)
            in_writetime = time()

        if self._configdb['type'] == "mongodb":
            #self.print_here("a: " + get_timestamp() + " " + str(write_dict['count']))
            for coll, requests in write_dict['requests'].items():
                try:
                    #bulkdict[bulkcoll].execute()
                    if coll not in self._bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        self._bulkcolls[coll] = self._db.get_collection(coll, write_concern = WriteConcern(w = self._configdb['writeconcern'], fsync = self._configdb['fsync']))
                    self._bulkcolls[coll].bulk_write(requests, ordered = False)
                except BulkWriteError as bwe:
                    pprint(bwe.details)
            #self.print_here("b: " + get_timestamp() + " " + str(write_dict['count']))

        if self._configopt['outlog']:
            writetime = time() - in_writetime
            outlogiosplit = write_dict['log'].rstrip("\n").split("\n")
            avgwritetime = writetime / len(outlogiosplit)
            outlogiolinecount = 1
            for outlogioline in outlogiosplit:
                if outlogioline != "":
                    start_writetimestamp = (in_writetimestamp + datetime.timedelta(seconds = (outlogiolinecount - 1) * avgwritetime)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"
                    end_writetimestamp = (in_writetimestamp + datetime.timedelta(seconds = outlogiolinecount * avgwritetime)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:(2 - 6)] + "Z"
                    with open(self._controllerpath + "/logs/" + self._jobstepname + ".log", "a") as outlogstream:
                        outlogstream.write(end_writetimestamp + " " + start_writetimestamp + " " + str(dir_size(self._controllerpath)) + " " + outlogioline + "\n")
                        outlogstream.flush()
                    outlogiolinecount += 1
                    self._countlogout += 1
            
            if self._configopt['intermedlog'] and self._countbatchesout >= self._configopt['cleanup']:
                self.cleanup_intermedlog()
                self._countlogout = 0

        if self._configopt['outlocal'] or self._configopt['statslocal']:
            for outext, outio in write_dict['io'].items():
                with open(self._controllerpath + "/bkps/" + self._jobstepname + "." + outext, "a") as outiostream:
                    outiosplit = outio.rstrip("\n").split("\n")
                    for outioline in outiosplit:
                        if outioline != "":
                            outiostream.write(outioline + "\n")
                            outiostream.flush()
                            if outext in self._countioouts.keys():
                                self._countioouts[outext] += 1
                            else:
                                self._countioouts[outext] = 1

            if self._configopt['intermedlocal'] and self._countbatchesout >= self._configopt['cleanup']:
                self.cleanup_intermedio()
                self._countioouts = {}

        if self._countbatchesout >= self._configopt['cleanup']:
            self._countbatchesout = 0

        #os.remove(lockfile)
        #while True:
        #    try:
        #        flock(self._locksstream, LOCK_EX | LOCK_NB)
        #        break
        #    except IOError as e:
        #        if e.errno != EAGAIN:
        #            raise
        #        else:
        #            sleep(0.1)
        #self._locksstream.seek(0)
        #locksline = self._locksstream.readline()
        #if locksline.isdigit():
        #    nlocks = int(locksline)
        #    self._locksstream.seek(0)
        #    self._locksstream.truncate(0)
        #    self._locksstream.write(str(nlocks - 1))
        #    self._locksstream.flush()
        #flock(self._locksstream, LOCK_UN)
        os.remove(self._controllerpath + "/locks/" + self._jobstepname + ".lock")

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        while self.is_alive() and not self._signal:
            while not self._writequeue.empty():
                if not os.path.exists(self._controllerpath + "/locks/" + self._jobstepname + ".lock"):
                    if not os.path.exists(self._controllerpath + "/locks/" + self._jobstepname + ".ready"):
                        readystream = open(self._controllerpath + "/locks/" + self._jobstepname + ".ready", "w").close()
                else:
                    self.write_batch()
                self.update_config()

        while not self._writequeue.empty():
            if not os.path.exists(self._controllerpath + "/locks/" + self._jobstepname + ".lock"):
                if not os.path.exists(self._controllerpath + "/locks/" + self._jobstepname + ".ready"):
                    readystream = open(self._controllerpath + "/locks/" + self._jobstepname + ".ready", "w").close()
            else:
                self.write_batch()
            self.update_config()

        if self._configdb['type'] == "mongodb":
            self._dbclient.close()

        #self._locksstream.close()

    #def nlocks(self):
    #    return self._nlocks

    def signal(self):
        self._signal = True

    def cleanup_intermedlog(self):
        with open(self._controllerpath + "/logs/" + self._jobstepname + ".log.intermed", "r") as intermedlogstream:
            while True:
                try:
                    flock(intermedlogstream, LOCK_EX | LOCK_NB)
                    break
                except IOError as e:
                    if e.errno != EAGAIN:
                        raise
            with tempfile.NamedTemporaryFile(dir = self._controllerpath + "/logs", delete = False) as tempstream:
                count = 0
                for intermedlogline in intermedlogstream:
                    if count >= self._countlogout:
                        tempstream.write(intermedlogline)
                        tempstream.flush()
                    count += 1
                os.rename(tempstream.name, intermedlogstream.name)
            flock(intermedlogstream, LOCK_UN)

    def cleanup_intermedio(self):
        for outext in self._countioouts.keys():
            with open(self._controllerpath + "/bkps/" + self._jobstepname + "." + outext + ".intermed", "r") as intermediostream:
                while True:
                    try:
                        flock(intermediostream, LOCK_EX | LOCK_NB)
                        break
                    except IOError as e:
                        if e.errno != EAGAIN:
                            raise
                with tempfile.NamedTemporaryFile(dir = self._controllerpath + "/bkps", delete = False) as tempstream:
                    count = 0
                    for intermedioline in intermediostream:
                        if count >= self._countioouts[outext]:
                            tempstream.write(intermedioline)
                            tempstream.flush()
                        count += 1
                    os.rename(tempstream.name, intermediostream.name)
                flock(intermediostream, LOCK_UN)

'''
class AsynchronousThreadStatsReader(Thread):
    "Class to implement asynchronously read output of
        a separate thread. Pushes read lines on a queue to
        be consumed in another thread."
    def __init__(self, pid, stats, stats_delay = 0):
        Thread.__init__(self)
        self._pid = str(pid)
        self._proc_smaps = open("/proc/" + str(pid) + "/smaps", "r")
        self._proc_stat = open("/proc/" + str(pid) + "/stat", "r")
        self._proc_uptime = open("/proc/uptime", "r")
        self._stats = dict((s, 0) for s in stats)
        self._maxstats = dict((s, 0) for s in stats)
        self._totstats = dict((s, 0) for s in stats)
        self._nstats = 0
        self._statsdelay = stats_delay
        self.daemon = True

    def is_inprog(self):
        "Check whether there is no more content to expect."
        return self.is_alive() and os.path.exists("/proc/" + self._pid + "/smaps")

    def run(self):
        "The body of the thread: read lines and put them on the queue."
        lower_keys = [k.lower() for k in self._stats.keys()]
        time_keys = ["elapsedtime", "totalcputime", "parentcputime", "childcputime", "parentcpuusage", "childcpuusage", "totalcpuusage"]
        if any([k in lower_keys for k in time_keys]):
            hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        while self.is_inprog():
            #for stat_name in self._stats.keys():
            #    self._stats[stat_name] = 0
            if any([k in self._stats.keys() for k in self._time_keys]):
                self._proc_stat.seek(0)
                stat_line = self._proc_stat.read() if self.is_inprog() else ""
                #print(stat_line)
                #sys.stdout.flush()
                if stat_line != "":
                    stat_line_split = stat_line.split()
                    utime, stime, cutime, cstime = [(float(f) / hz) for f in stat_line_split[13:17]]
                    starttime = float(stat_line_split[21]) / hz
                    parent_cputime = utime + stime
                    child_cputime = cutime + cstime
                    total_cputime = parent_cputime + child_cputime
                    self._proc_uptime.seek(0)
                    uptime_line = self._proc_uptime.read()
                    uptime = float(uptime_line.split()[0])
                    elapsedtime = uptime-starttime
                    parent_cpuusage = 100 * parent_cputime / elapsedtime if elapsedtime > 0 else 0
                    child_cpuusage = 100 * child_cputime / elapsedtime if elapsedtime > 0 else 0
                    total_cpuusage = 100 * total_cputime / elapsedtime if elapsedtime > 0 else 0
                    for k in self._stats.keys():
                        if k.lower() == "elapsedtime":
                            self._stats[k] = elapsedtime
                        if k.lower() == "totalcputime":
                            self._stats[k] = total_cputime
                        if k.lower() == "parentcputime":
                            self._stats[k] = parent_cputime
                        if k.lower() == "childcputime":
                            self._stats[k] = child_cputime
                        if k.lower() == "parentcpuusage":
                            self._stats[k] = parent_cpuusage
                        if k.lower() == "childcpuusage":
                            self._stats[k] = child_cpuusage
                        if k.lower() == "totalcpuusage":
                            self._stats[k] = total_cpuusage
            self._proc_smaps.seek(0)
            smaps_lines = self._proc_smaps.readlines() if self.is_inprog() and os.path.exists("/proc/" + self._pid) else ""
            for smaps_line in smaps_lines:
                smaps_line_split = smaps_line.split()
                if len(smaps_line_split) == 3:
                    stat_name, stat_size, stat_unit = smaps_line_split
                    stat_name = stat_name.rstrip(':')
                    if stat_name.lower() in self._stats.keys():
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
                            raise Exception(stat_name + " in " + self._proc_smaps.name + " has unrecognized unit: " + stat_unit)
                        self._stats[stat_name] += multiplier * stat_size
                #smaps_line = self._proc_smaps.readline() if self.is_alive() else ""
            self._nstats += 1
            for k in self._stats.keys():
                self._totstats[k] += self._stats[k]
                if self._stats[k] >= self._maxstats[k]:
                    self._maxstats[k] = self._stats[k]
            sleep(self._statsdelay)
        #self._proc_smaps.close()
        #self._proc_stat.close()
        #self._proc_uptime.close()

    def stat(self, stat_name):
        "Check whether there is no more content to expect."
        return self._stats[stat_name.lower()]

    def stats(self):
        "Check whether there is no more content to expect."
        return self._stats

    def max_stat(self, stat_name):
        "Check whether there is no more content to expect."
        return self._maxstats[stat_name.lower()]

    def max_stats(self):
        "Check whether there is no more content to expect."
        return self._maxstats

    def avg_stat(self, stat_name):
        "Check whether there is no more content to expect."
        return self._totstats[stat_name.lower()] / self._nstats if self._nstats > 0 else 0

    def avg_stats(self):
        "Check whether there is no more content to expect."
        return dict((k, self._totstats[k] / self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())
'''

def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

def timestamp2unit(timestamp, unit = "seconds"):
    if timestamp == "infinite":
        return timestamp
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

parser = ArgumentParser()

parser.add_argument('--mod', dest = 'modname', action = 'store', default = None, help = '')
parser.add_argument('--controller', dest = 'controllername', action = 'store', default = None, help = '')
parser.add_argument('--stepid', dest = 'stepid', action = 'store', default = "1", help = '')
parser.add_argument('--partition', dest = 'partition', action = 'store', default = None, help = '')
parser.add_argument('--nsteptasks', dest = 'nsteptasks', action = 'store', default = "1", help = '')
parser.add_argument('--nstepcpus', dest = 'nstepcpus', action = 'store', default = "1", help = '')
#parser.add_argument('--cpus-per-task', dest = 'cpuspertask', action = 'store', default = "1", help = '')
parser.add_argument('--stepmem', dest = 'stepmem', action = 'store', default = None, help = '')

#parser.add_argument('--nbatch', '-n', dest = 'nbatch', action = 'store', default = 1, help = '')
#parser.add_argument('--nworkers', '-N', dest = 'nworkers', action = 'store', default = 1, help = '')
parser.add_argument('--random-nbatch', dest = 'random_nbatch', action = 'store_true', default = False, help = '')

#parser.add_argument('--dbtype', dest = 'dbtype', action = 'store', default = None, help = '')
#parser.add_argument('--dbhost', dest = 'dbhost', action = 'store', default = None, help = '')
#parser.add_argument('--dbport', dest = 'dbport', action = 'store', default = None, help = '')
#parser.add_argument('--dbusername', dest = 'dbusername', action = 'store', default = None, help = '')
#parser.add_argument('--dbpassword', dest = 'dbpassword', action = 'store', default = None, help = '')
#parser.add_argument('--dbname', dest = 'dbname', action = 'store', default = None, help = '')

#parser.add_argument('--intermed-log', dest = 'intermedlog', action = 'store_true', default = False, help = '')
#parser.add_argument('--intermed-local', '-t', dest = 'intermedlocal', action = 'store_true', default = False, help = '')
#parser.add_argument('--out-log', dest = 'outlog', action = 'store_true', default = False, help = '')
#parser.add_argument('--out-local', '-w', dest = 'outlocal', action = 'store_true', default = False, help = '')
#parser.add_argument('--out-db', '-W', dest = 'outdb', action = 'store_true', default = False, help = '')
#parser.add_argument('--stats-local', '-s', dest = 'statslocal', action = 'store_true', default = False, help = '')
#parser.add_argument('--stats-db', '-S', dest = 'statsdb', action = 'store_true', default = False, help = '')
#parser.add_argument('--basecoll', dest = 'basecoll', action = 'store', default = None, help = '')
parser.add_argument('--dbindexes', dest = 'dbindexes', nargs = '+', default = None, help = '')
#parser.add_argument('--mark-done', dest = 'markdone', action = 'store', default = "MARK", help = '')

parser.add_argument('--delay', dest = 'delay', action = 'store', default = 0, help = '')
parser.add_argument('--stats', dest = 'stats_list', nargs = '+', action = 'store', default = [], help = '')
#parser.add_argument('--stats-delay', dest = 'stats_delay', action = 'store', default = 0, help = '')
parser.add_argument('--delimiter', '-d', dest = 'delimiter', action = 'store', default = '\n', help = '')
parser.add_argument('--input-args', '-i', dest = 'input_args', nargs = '+', action = 'store', default = [], help = '')
parser.add_argument('--input-file', '-f', dest = 'input_file', action = 'store', default = None, help = '')
#parser.add_argument('--cleanup-after', dest = 'cleanup', action = 'store', default = None, help = '')
parser.add_argument('--interactive', dest = 'interactive', action = 'store_true', default = False, help = '')
parser.add_argument('--time-limit', dest = 'time_limit', action = 'store', default = None, help = '')
parser.add_argument('--ignored-strings', dest = 'ignoredstrings', nargs = '+', default = [], help = '')
parser.add_argument('--module-language', dest = 'modlang', action = 'store', default = None, help = '')
parser.add_argument('--module', '-c', dest = 'modcommand', nargs = '+', required = True, help = '')
parser.add_argument('--args', '-a', dest = 'modargs', nargs = REMAINDER, default = [], help = '')

kwargs = vars(parser.parse_known_args()[0])

#print(kwargs['input_args'])
#sys.stdout.flush()

if kwargs['time_limit'] == None:
    start_time = None
else:
    start_time = time()
    kwargs['time_limit'] = timestamp2unit(kwargs['time_limit'])

kwargs['delay'] = float(kwargs['delay'])
#kwargs['stats_delay'] = float(kwargs['stats_delay'])
#controllerconfigdoc['options']['nbatch'] = int(controllerconfigdoc['options']['nbatch'])
#controllerconfigdoc['options']['nworkers'] = int(controllerconfigdoc['options']['nworkers'])
#kwargs['delimiter'] = kwargs['delimiter']#.decode("string_escape")
#if controllerconfigdoc['options']['cleanup'] == "":
#    controllerconfigdoc['options']['cleanup'] = None
#else:
#    controllerconfigdoc['options']['cleanup'] = eval(controllerconfigdoc['options']['cleanup'])

if kwargs['modname'] == None:
    modname = kwargs['modcommand'][-1].split("/")[-1].split(".")[0]
else:
    modname = kwargs['modname']

script = " ".join(kwargs['modcommand'] + kwargs['modargs'])

ignoredstrings = kwargs['ignoredstrings']

#if kwargs['controllername'] == None:
#    rootpath = os.getcwd()
#    controllerpath = rootpath
#    dbtype = kwargs['dbtype']
#    dbhost = kwargs['dbhost']
#    dbport = kwargs['dbport']
#    dbusername = kwargs['dbusername']
#    dbpassword = kwargs['dbpassword']
#    dbname = kwargs['dbname']
#    basecoll = kwargs['basecoll']
#
#    controllerconfigdoc = {
#                            "db": {
#                                     "type": dbtype,
#                                     "host": dbhost,
#                                     "port": dbport,
#                                     "username": dbusername,
#                                     "password": dbpassword,
#                                     "name": dbname,
#                                     "basecollection": basecoll
#                                  }
#                          }
#else:
rootpath = os.environ['CRUNCH_ROOT']
#softwarefile = rootpath + "/state/software"
#with open(softwarefile, "r") as softwarestream:
#    softwarestream.readline()
#    for line in softwarestream:
#        ext = line.split(',')[2]
#        if ext + " " in kwargs['modcommand']:
#            break
#modulesfile = rootpath + "/state/modules"
#with open(modulesfile, "r") as modulesstream:
#    modulesstream.readline()
#    for line in modulesstream:
#        modname = line.rstrip("\n")
#        if " " + modname + ext + " " in kwargs['modcommand'] or "/" + modname + ext + " " in kwargs['modcommand']:
#            break
#controllerpath = rootpath + "/modules/" + modname + "/" + kwargs['controllername']
with open(rootpath + "/crunch.config", "r") as crunchconfigstream:
    crunchconfigdoc = yaml.load(crunchconfigstream)

if (kwargs['modlang'] != None) and (kwargs['modlang'] in crunchconfigdoc["software"].keys()) and ("ignored-strings" in crunchconfigdoc["software"][kwargs['modlang']].keys()):
    ignoredstrings += crunchconfigdoc["software"][kwargs['modlang']]["ignored-strings"]

if crunchconfigdoc["workload-manager"] == "slurm":
    from crunch_slurm import *

controllerpath = "/".join(get_controllerpath(kwargs['stepid']).split("/")[:-1])

with open(controllerpath + "/" + kwargs["modname"] + "_" + kwargs["controllername"] + ".config", "r") as controllerconfigstream:
    controllerconfigdoc = yaml.load(controllerconfigstream)
if controllerconfigdoc["options"]["markdone"] == None:
    controllerconfigdoc["options"]["markdone"] = ""

#dbtype = controllerconfigdoc['db']['type']
#dbhost = str(controllerconfigdoc['db']['host'])
#dbport = str(controllerconfigdoc['db']['port'])
#dbusername = controllerconfigdoc['db']['username']
#dbpassword = controllerconfigdoc['db']['password']
#dbname = controllerconfigdoc['db']['name']
#basecoll = controllerconfigdoc['db']['basecollectionection']

#workpath = controllerpath + "/logs"
#controllerfile = controllerpath + "/crunch_" + modname + "_" + kwargs['controllername'] + "_controller.job"
#with open(controllerfile, "r") as controllerstream:
#    for controllerline in controllerstream:
#        if "dbtype=" in controllerline:
#            dbtype = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if dbtype == "":
#                dbtype = None
#        elif "dbhost=" in controllerline:
#            dbhost = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if dbhost == "":
#                dbhost = None
#        elif "dbport=" in controllerline:
#            dbport = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if dbport == "":
#                dbport = None
#        elif "dbusername=" in controllerline:
#            dbusername = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if dbusername == "":
#                dbusername = None
#        elif "dbpassword=" in controllerline:
#            dbpassword = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if dbpassword == "":
#                dbpassword = None
#        elif "dbname=" in controllerline:
#            dbname = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if dbname == "":
#                dbname = None
#        elif "basecollection=" in controllerline:
#            basecoll = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
#            if basecoll == "":
#                basecoll = None

if not os.path.isdir(controllerpath + "/jobs"):
    os.mkdir(controllerpath + "/jobs")
if not os.path.isdir(controllerpath + "/docs"):
    os.mkdir(controllerpath + "/docs")
if not os.path.isdir(controllerpath + "/logs"):
    os.mkdir(controllerpath + "/logs")
if not os.path.isdir(controllerpath + "/locks"):
    os.mkdir(controllerpath + "/locks")
if not os.path.isdir(controllerpath + "/bkps") and (controllerconfigdoc['options']['intermedlocal'] or controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']):
    os.mkdir(controllerpath + "/bkps")

stdin_args = iter(kwargs['input_args'])  

#if kwargs['input_file'] != None:
if "/" in kwargs['input_file']:
    kwargs['input_file'] = kwargs['input_file'].split('/')[-1]
stdin_file_path = controllerpath + "/docs/" + kwargs['input_file']
try:
    input_size = os.path.getsize(stdin_file_path)
except OSError:
    input_size = 0
while input_size == 0:
    try:
        input_size = os.path.getsize(stdin_file_path)
    except OSError:
        input_size = 0
    sleep(kwargs['delay'])
#stdin_file = open(stdin_file_path, "r+")
jobstepname = kwargs['input_file'].split('.')[0]
#filename = ".".join(kwargs['input_file'].split('.')[:-1])
#stepsplit = jobstepname.split("_step_")
#job = stepsplit[0].split("_job_")[1]
#step = stepsplit[1]
#else:
#    stdin_file = None
#    #filename = workpath + "/" + kwargs['stepid']
#    jobstepname = kwargs['stepid']
#    stepsplit = jobstepname.split('.')
#    job = stepsplit[0]
#    if len(stepsplit) > 1:
#        step = stepsplit[1]
#    else:
#        step = "1"

while True:
    try:
        flock(sys.stdout, LOCK_EX | LOCK_NB)
        break
    except IOError as e:
        if e.errno != EAGAIN:
            raise
        else:
            sleep(0.1)
print(get_timestamp() + " " + jobstepname + " START")
sys.stdout.flush()
flock(sys.stdout, LOCK_UN)

#if controllerconfigdoc['options']['intermedlog']:
#    intermedlogstream = open(controllerpath + "/logs/" + jobstepname + ".log.intermed", "w")

#if controllerconfigdoc['options']['outlog']:
#    outlogstream = open(controllerpath + "/logs/" + jobstepname + ".log", "w")
#else:
#    outlogstream = None
    #outloglist = [""]

#if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
    #outiolist = [{}]
    #outiostream = open(controllerpath + "/bkps/" + jobstepname + ".out", "w")
    #if controllerconfigdoc['options']['intermedlocal']:
    #    intermediostream = open(controllerpath + "/bkps/" + jobstepname + ".intermed", "w")
#    if (kwargs['dbindexes'] == None) or ((kwargs['controllername'] == None) and (basecoll == None)):
#        parser.error("Both --write-local and --stats-local require either the options:\n--controllername --dbindexes, \nor the options: --basecoll --dbindexes")

if any([controllerconfigdoc['options'][x] for x in ['outdb', 'statsdb']]):
    #if (kwargs['dbindexes'] == None) or ((kwargs['controllername'] == None) and any([x == None for x in [dbtype, controllerconfigdoc['db']['host'], controllerconfigdoc['db']['port'], controllerconfigdoc['db']['username'], controllerconfigdoc['db']['password'], controllerconfigdoc['db']['name'], controllerconfigdoc['db']['basecollection']]])):
    #    parser.error("Both --outdb and --statsdb require either the options:\n--controllername --dbindexes, \nor the options:\n--dbtype --dbhost --dbport --dbusername --dbpassword --dbname --basecoll --dbindexes")
    #else:
    if controllerconfigdoc['db']['type'] == "mongodb":
        from mongojoin import get_bsonsize
        from pymongo import MongoClient, UpdateOne, WriteConcern
        from pymongo.errors import BulkWriteError
    write_queue = Queue()
    dbhandler = AsyncBulkWriteStream(kwargs['modname'], kwargs['controllername'], controllerpath, jobstepname, write_queue, controllerconfigdoc,
                                     stepid = kwargs['stepid'])
    dbhandler.start()

process = Popen(script, shell = True, stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize = 1, preexec_fn = default_sigpipe)
#print("a")
#sys.stdout.flush()

stdin_queue = Queue()
if not kwargs['interactive']:
    stdin_queue.put("")

intermed_queue = Queue()

stdout_queue = Queue()

if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
    stats_queue = Queue()

#stdout_reader = AsynchronousThreadStreamReaderWriter(process.stdin, process.stdout, stdin_args, stdin_file, stdin_queue, intermed_queue, stdout_queue, delimiter = kwargs['delimiter'], cleanup = controllerconfigdoc['options']['cleanup'], time_limit = kwargs['time_limit'], start_time = start_time)
#stdout_reader.start()

#stderr_queue = Queue()
#stderr_reader = AsynchronousThreadStreamReader(process.stderr, stderr_queue)
#stderr_reader.start()

#if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
#    stats_reader = AsynchronousThreadStatsReader(process.pid, kwargs['stats_list'], stats_delay = kwargs['stats_delay'])
#    stats_reader.start()

stats_list = [x.lower() for x in kwargs['stats_list']]
if not any([x.lower() == "totalcputime" for x in kwargs['stats_list']]):
    stats_list += ["totalcputime"]
if not any([x.lower() == "rss" for x in kwargs['stats_list']]):
    stats_list += ["rss"]
if not any([x.lower() == "size" for x in kwargs['stats_list']]):
    stats_list += ["size"]
if not any([x.lower() == "elapsedtime" for x in kwargs['stats_list']]):
    stats_list += ["elapsedtime"]
#if not any([x.lower() == "timestamp" for x in kwargs['stats_list']]):
#    stats_list += ["timestamp"]

handler = AsyncIOStatsStream(kwargs['modname'], kwargs['controllername'], controllerpath, jobstepname, process.pid, process.stdin, process.stdout, process.stderr, stdin_args, stdin_queue, intermed_queue, stdout_queue,
                             nrefill = controllerconfigdoc['options']['nrefill'],
                             partition = kwargs['partition'],
                             nsteptasks = kwargs['nsteptasks'],
                             #cpuspertask = kwargs['cpuspertask'],
                             nstepcpus = kwargs['nstepcpus'],
                             stepmem = kwargs['stepmem'],
                             stepid = kwargs['stepid'],
                             ignoredstrings = kwargs['ignoredstrings'],
                             stats = stats_list if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb'] else None,
                             stats_queue = stats_queue if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb'] else None,
                             delimiter = kwargs['delimiter'],
                             cleanup = controllerconfigdoc['options']['cleanup'],
                             time_limit = kwargs['time_limit'],
                             start_time = start_time)
handler.start()
#print("b")
#sys.stdout.flush()

#bulkcolls = {}
#bulkrequestslist = [{}]
#countallbatches = [0]
batch_dict = {'requests': {}, 'count': 0}
if controllerconfigdoc['options']['outlog']:
    batch_dict['log'] = ""
if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
    batch_dict['io'] = {}
#intermediostream = open(workpath + "/" + stepname + ".temp", "w")
bsonsize = 0
countthisbatch = 0
nbatch = randint(1, controllerconfigdoc['options']['nbatch']) if kwargs['random_nbatch'] else controllerconfigdoc['options']['nbatch']
#countioouts = {}
#countlogout = 0
#countbatchesout = 0
#while process.poll() == None and stats_reader.is_inprog() and not (stdout_reader.eof() or stderr_reader.eof()):
while process.poll() == None and handler.is_inprog() and not handler.eof():
    #if handler.waiting():
    #    stdin_line = sys.stdin.readline().rstrip("\n")
    #    stdin_queue.put(stdin_line)
    while not stdout_queue.empty():
        #while (not stdout_queue.empty()) and ((len(bulkrequestslist) <= 1 and countthisbatch < nbatch) or (handler.nlocks() >= controllerconfigdoc['options']['nworkers'])):
        line = stdout_queue.get().rstrip("\n")
        #with open(controllerpath + "/logs/" + jobstepname + ".test", "a") as teststream:
        #    teststream.write(get_timestamp() + " UTC: Print: " + line + "\n")
        #    teststream.flush()
        if line not in ignoredstrings:
            line_split = line.split()
            if len(line_split) > 0:
                line_action = line_split[0]
            if line == "":
                with open(controllerpath + "/" + kwargs["modname"] + "_" + kwargs["controllername"] + ".config", "r") as controllerconfigstream:
                    controllerconfigdoc = yaml.load(controllerconfigstream)
                if controllerconfigdoc["options"]["markdone"] == None:
                    controllerconfigdoc["options"]["markdone"] = ""
                if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
                    #cputime = eval("%.2f" % handler.stat("TotalCPUTime"))
                    #maxrss = handler.max_stat("Rss")
                    #maxvmsize = handler.max_stat("Size")
                    stats = stats_queue.get()
                    cputime = eval("%.4f" % stats["stats"]["totalcputime"])
                    maxrss = stats["max"]["rss"]
                    maxvmsize = stats["max"]["size"]
                #    stats = getstats("sstat", ["MaxRSS", "MaxVMSize"], kwargs['stepid'])
                #    if (len(stats) == 1) and (stats[0] == ""):
                #        newtotcputime, maxrss, maxvmsize = [eval(x) for x in getstats("sacct", ["CPUTimeRAW", "MaxRSS", "MaxVMSize"], kwargs['stepid'])]
                #    else:
                #        newtotcputime = eval(getstats("sacct", ["CPUTimeRAW"], kwargs['stepid'])[0])
                #        maxrss, maxvmsize = stats
                #    cputime = newtotcputime-totcputime
                #    totcputime = newtotcputime
                #newcollection, strindexdoc = linehead[1:].split("<")[0].split(".")
                #newindexdoc = json.loads(strindexdoc)
                newcollection = controllerconfigdoc['db']['basecollection']
                doc = json.loads(intermed_queue.get())
                newindexdoc = dict([(x, doc[x]) for x in kwargs['dbindexes']]);
                outext = newcollection + ".set"
                #runtime = "%.2f" % handler.stat("ElapsedTime")
                #runtime = "%.2f" % stats["stats"]["elapsedtime"]
                in_intermedtime = stats["in_timestamp"]
                out_intermedtime = stats["out_timestamp"]
                if controllerconfigdoc['options']['intermedlog'] or controllerconfigdoc['options']['outlog']:
                    if controllerconfigdoc['options']['intermedlog']:
                        with open(controllerpath + "/logs/" + jobstepname + ".log.intermed", "a") as intermedlogstream:
                            intermedlogstream.write(out_intermedtime + " " + in_intermedtime + " " + str(dir_size(controllerpath)) + " " + ("%.2f" % cputime) + " " + str(maxrss) + " " + str(maxvmsize) + " " + str(bsonsize) + " " + json.dumps(newindexdoc, separators = (',', ':')) + "\n")
                            intermedlogstream.flush()
                    if controllerconfigdoc['options']['outlog']:
                        batch_dict['log'] += out_intermedtime + " " + in_intermedtime + " " + str(dir_size(controllerpath)) + " " + ("%.2f" % cputime) + " " + str(maxrss) + " " + str(maxvmsize) + " " + str(bsonsize) + " " + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                statsmark = {}
                if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
                    statsmark.update({modname + "STATS": {"CPUTIME": cputime, "MAXRSS": maxrss, "MAXVMSIZE": maxvmsize, "BSONSIZE": bsonsize}})
                if controllerconfigdoc['options']['markdone'] != "":
                    statsmark.update({modname + controllerconfigdoc['options']['markdone']: True})
                # Testing
                #statsmark.update({"HOST": os.environ['HOSTNAME'], "STEP": "job_" + kwargs['input_file'].split("_job_")[1], "NBATCH": nbatch, "TIME": get_timestamp() + " UTC"})
                # Done testing
                if len(statsmark) > 0:
                    #mergeddoc = merge_two_dicts(newindexdoc, statsmark)
                    #lineout = json.dumps(mergeddoc, separators = (',',':'))
                    lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(statsmark, separators = (',',':'))
                    if controllerconfigdoc['options']['intermedlocal']:
                        with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                            intermediostream.write(lineout + "\n")
                            intermediostream.flush()
                    if controllerconfigdoc['options']['statslocal']:
                        #outiolist[-1] += "CPUTime: " + str(cputime) + " seconds\n"
                        #outiolist[-1] += "MaxRSS: " + str(maxrss) + " bytes\n"
                        #outiolist[-1] += "MaxVMSize: " + str(maxvmsize) + " bytes\n"
                        #outiolist[-1] += "BSONSize: " + str(bsonsize) + " bytes\n"
                        if outext not in batch_dict['io'].keys():
                            batch_dict['io'][outext] = ""
                        batch_dict['io'][outext] += lineout + "\n"
                    #if controllerconfigdoc['options']['outlocal']:
                    #    outiolist[-1] += newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                    #if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                    #    if controllerconfigdoc['db']['type'] == "mongodb":
                    #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in batch_dict['requests'].keys():
                        batch_dict['requests'][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": statsmark})
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$set": statsmark}, upsert = True)]
                bsonsize = 0
                countthisbatch += 1
                batch_dict['count'] += 1
                if countthisbatch == nbatch:
                    write_queue.put(batch_dict)
                    #bulkrequestslist += [{}]
                    #if controllerconfigdoc['options']['outlog']:
                    #    outloglist += [""]
                    #if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
                    #    outiolist += [{}]
                    batch_dict = {'requests': {}, 'count': 0}
                    if controllerconfigdoc['options']['outlog']:
                        batch_dict['log'] = ""
                    if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
                        batch_dict['io'] = {}
                    countthisbatch = 0
                    nbatch = randint(1, controllerconfigdoc['options']['nbatch']) if kwargs['random_nbatch'] else controllerconfigdoc['options']['nbatch']
                    #countallbatches += [0]
            elif line[0] == "#":
                if controllerconfigdoc['options']['intermedlog'] or controllerconfigdoc['options']['outlog']:
                    if controllerconfigdoc['options']['intermedlog']:
                        with open(controllerpath + "/logs/" + jobstepname + ".log.intermed", "a") as intermedlogstream:
                            intermedlogstream.write("# " + line + "\n")
                            intermedlogstream.flush()
                    if controllerconfigdoc['options']['outlog']:
                        with open(controllerpath + "/logs/" + jobstepname + ".log", "a") as outlogstream:
                            outlogstream.write("# " + line + "\n")
                            outlogstream.flush()
            elif len(line_split) == 4:
                #linehead = re.sub("^([-+&@#].*?>|None).*", r"\1", line)
                #linemarker = linehead[0]
                if line_action == "unset":
                    newcollection = line_split[1]
                    newindexdoc = json.loads(line_split[2])
                    #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                    doc = json.loads(line_split[3])
                    #mergeddoc = merge_two_dicts(newindexdoc, doc)
                    #lineout = json.dumps(mergeddoc, separators = (',',':'))
                    lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(doc, separators = (',',':'))
                    outext = newcollection + "." + line_action
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        bsonsize -= get_bsonsize(doc)
                    if controllerconfigdoc['options']['intermedlocal']:
                        with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                            intermediostream.write(lineout + "\n")
                            intermediostream.flush()
                    if controllerconfigdoc['options']['outlocal']:
                        if outext not in batch_dict['io'].keys():
                            batch_dict['io'][outext] = ""
                        batch_dict['io'][outext] += lineout + "\n"
                    if controllerconfigdoc['options']['outdb']:
                        #if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        #    if controllerconfigdoc['db']['type'] == "mongodb":
                        #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in batch_dict['requests'].keys():
                            batch_dict['requests'][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).update({"$unset": doc})
                        if controllerconfigdoc['db']['type'] == "mongodb":
                            batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$unset": doc})]
                elif line_action == "set":
                    newcollection = line_split[1]
                    newindexdoc = json.loads(line_split[2])
                    #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                    doc = json.loads(line_split[3])
                    #mergeddoc = merge_two_dicts(newindexdoc, doc)
                    #lineout = json.dumps(mergeddoc, separators = (',',':'))
                    lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(doc, separators = (',',':'))
                    outext = newcollection + "." + line_action
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        bsonsize += get_bsonsize(doc)
                    if controllerconfigdoc['options']['intermedlocal']:
                        with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                            intermediostream.write(lineout + "\n")
                            intermediostream.flush()
                    if controllerconfigdoc['options']['outlocal']:
                        if outext not in batch_dict['io'].keys():
                            batch_dict['io'][outext] = ""
                        batch_dict['io'][outext] += lineout + "\n"
                    if controllerconfigdoc['options']['outdb']:
                        #if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        #    if controllerconfigdoc['db']['type'] == "mongodb":
                        #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in batch_dict['requests'].keys():
                            batch_dict['requests'][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": doc})
                        if controllerconfigdoc['db']['type'] == "mongodb":
                            batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$set": doc}, upsert = True)]
                elif line_action == "addToSet":
                    newcollection = line_split[1]
                    newindexdoc = json.loads(line_split[2])
                    #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                    doc = json.loads(line_split[3])
                    #mergeddoc = merge_two_dicts(newindexdoc, doc)
                    #lineout = json.dumps(mergeddoc, separators = (',',':'))
                    lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(doc, separators = (',',':'))
                    outext = newcollection + "." + line_action
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        bsonsize += get_bsonsize(doc)
                    if controllerconfigdoc['options']['intermedlocal']:
                        with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                            intermediostream.write(lineout + "\n")
                            intermediostream.flush()
                    if controllerconfigdoc['options']['outlocal']:
                        if outext not in batch_dict['io'].keys():
                            batch_dict['io'][outext] = ""
                        batch_dict['io'][outext] += lineout + "\n"
                    if controllerconfigdoc['options']['outdb']:
                        #if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        #    if controllerconfigdoc['db']['type'] == "mongodb":
                        #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in batch_dict['requests'].keys():
                            batch_dict['requests'][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
                        if controllerconfigdoc['db']['type'] == "mongodb":
                            batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$addToSet": doc}, upsert = True)]
                elif line_action == "insert":
                    newcollection = line_split[1]
                    newindexdoc = json.loads(line_split[2])
                    #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                    doc = json.loads(line_split[3])
                    mergeddoc = merge_two_dicts(newindexdoc, doc)
                    lineout = json.dumps(mergeddoc, separators = (',',':'))
                    outext = newcollection + "." + line_action
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        bsonsize += get_bsonsize(doc)
                    if controllerconfigdoc['options']['intermedlocal']:
                        with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                            intermediostream.write(lineout + "\n")
                            intermediostream.flush()
                    if controllerconfigdoc['options']['outlocal']:
                        if outext not in batch_dict['io'].keys():
                            batch_dict['io'][outext] = ""
                        batch_dict['io'][outext] += lineout + "\n"
                    if controllerconfigdoc['options']['outdb']:
                        #if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        #    if controllerconfigdoc['db']['type'] == "mongodb":
                        #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in batch_dict['requests'].keys():
                            batch_dict['requests'][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
                        if controllerconfigdoc['db']['type'] == "mongodb":
                            batch_dict['requests'][newcollection] += [InsertOne(mergeddoc)]
            else:
                #if controllerconfigdoc['options']['intermedlocal']:
                #    intermediostream.write(line + "\n")
                #    intermediostream.flush()
                #if controllerconfigdoc['options']['outlocal']:
                #    outiolist[-1] += line + "\n"
                try:
                    raise IndexError("Modules should only output commented lines, blank lines separating processed input documents, or line with 4 columns representing: collection name, update action, index document, output document.")
                except IndexError as e:
                    with open(controllerpath + "/logs/" + jobstepname + ".err", "a") as errfilestream:
                        traceback.print_exc(file = errfilestream)
                        errfilestream.flush()
                    raise
                #if handler.nlocks() < controllerconfigdoc['options']['nworkers']:
                #    lockfile = controllerpath + "/locks/" + kwargs['stepid'] + ".lock"
                #    with open(lockfile, 'w') as lockstream:
                #        lockstream.write(str(countallbatches))
                #        lockstream.flush()
                #    for bulkcoll in bulkdict.keys():
                #        try:
                #            bulkdict[bulkcoll].execute()
                #        except BulkWriteError as bwe:
                #            pprint(bwe.details)
                #    while True:
                #        try:
                #            fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
                #            break
                #        except IOError:
                #            sleep(0.01)
                #    sys.stdout.write(intermediostream.getvalue())
                #    sys.stdout.flush()
                #    fcntl.flock(sys.stdout, fcntl.LOCK_UN)
                #    bulkdict = {}
                #    intermediostream = cStringIO.StringIO()
                #    countallbatches = 0
                #    os.remove(lockfile)

        #while not stderr_queue.empty():
        #    stderr_line = stderr_queue.get().rstrip("\n")
        #    if stderr_line not in ignoredstrings:
        #        if kwargs['controllername'] != None:
        #            exitcode = get_exitcode(kwargs['stepid'])
        #        with open(controllerpath + "/logs/" + jobstepname + ".err", "a") as errstream:
        #            if kwargs['controllername'] != None:
        #                errstream.write("ExitCode: " + exitcode + "\n")
        #            errstream.write(stderr_line + "\n")
        #            errstream.flush()
        #        #while True:
        #        #    try:
        #        #        fcntl.flock(sys.stderr, fcntl.LOCK_EX | fcntl.LOCK_NB)
        #        #        break
        #        #    except IOError:
        #        #        sleep(0.01)
        #        sys.stderr.write(stderr_line + "\n")
        #        sys.stderr.flush()
        #        #fcntl.flock(sys.stderr, fcntl.LOCK_UN)

        #if (len(bulkrequestslist) > 1) and (handler.nlocks() < controllerconfigdoc['options']['nworkers']):
            #with open(controllerpath + "/logs/" + jobstepname + ".test", "a") as teststream:
            #    teststream.write(get_timestamp() + " UTC: Work\n")
            #    teststream.flush()
            #if handler.nlocks() >= controllerconfigdoc['options']['nworkers']:
            #    overlocked = True
            #    os.kill(process.pid, signal.SIGSTOP)
            #    while handler.nlocks() >= controllerconfigdoc['options']['nworkers']:
            #        sleep(0.01)
            #else:
            #    overlocked = False
        #    lockfile = controllerpath + "/locks/" + jobstepname + ".lock"
        #    with open(lockfile, 'w') as lockstream:
        #        lockstream.write(kwargs['stepid'] + ": Writing " + str(countallbatches[0]) + " items.")
        #        lockstream.flush()
        #    countbatchesout += countallbatches[0]
        #    del countallbatches[0]
            #print(bulkdict)
            #sys.stdout.flush()

        #    if controllerconfigdoc['options']['outlog']:
        #        in_writetime = time()

        #    if controllerconfigdoc['db']['type'] == "mongodb":
        #        for coll, requests in bulkrequestslist[0].items():
        #            try:
                        #bulkdict[bulkcoll].execute()
        #                bulkcolls[coll].bulk_write(requests, ordered = False)
        #            except BulkWriteError as bwe:
        #                pprint(bwe.details)
        #        del bulkrequestslist[0]
            #while True:
            #    try:
            #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
            #        break
            #    except IOError:
            #        sleep(0.01)
            #intermediostream.close()
            #with open(workpath + "/" + stepname + ".temp", "r") as intermediostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
            #    for line in intermediostream:
            #        iostream.write(line)
            #        iostream.flush()
            #    os.remove(intermediostream.name)
            #sys.stdout.write(intermediostream.getvalue())
            #sys.stdout.flush()
        #    if controllerconfigdoc['options']['outlog']:
                #print(len(outloglist[0].rstrip("\n").split("\n")))
                #sys.stdout.flush()
        #        writetime = time() - in_writetime
        #        outlogiosplit = outloglist[0].rstrip("\n").split("\n")
        #        avgwritetime = "%.2f" % (writetime / len(outlogiosplit))
                #outlogiotime = ""
        #        for outlogioline in outlogiosplit:
        #            if outlogioline != "":
        #                outlogstream.write(get_timestamp() + " " + str(avgwritetime) + "s " + outlogioline + "\n")
        #                outlogstream.flush()
        #                countlogout += 1
        #        del outloglist[0]
        #        if controllerconfigdoc['options']['intermedlog'] and countbatchesout >= kwargs["cleanup"]:
        #            cleanup_intermedlog(controllerpath, jobstepname, countlogout)
        #            countlogout = 0
            #if controllerconfigdoc['options']['intermedlocal']:
                #name = intermediostream.name
                #intermediostream.close()
                #os.remove(name)
                #intermediostream = open(name, "w")
        #    if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
        #        for outext, outio in outiolist[0].items():
        #            with open(controllerpath + "/bkps/" + jobstepname + "." + outext, "a") as outiostream:
        #                outiosplit = outio.rstrip("\n").split("\n")
        #                for outioline in outlogiosplit:
        #                    if outioline != "":
        #                        outiostream.write(outioline + "\n")
        #                        outiostream.flush()
        #                        if outext in countioouts.keys():
        #                            countioouts[outext] += 1
        #                        else:
        #                            countioouts[outext] = 1
        #        del outiolist[0]
        #        if controllerconfigdoc['options']['intermedlocal'] and countbatchesout >= kwargs["cleanup"]:
        #            cleanup_intermedio(controllerpath, jobstepname, countioouts)
        #            countioouts = {}

        #    if countbatchesout >= kwargs["cleanup"]:
        #        countbatchesout = 0
            #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
            #bulkdict = {}
            #intermediostream = open(workpath + "/" + stepname + ".temp", "w")
            #countallbatches = 0
        #    os.remove(lockfile)
            #if overlocked:
            #    os.kill(process.pid, signal.SIGCONT)

    #sleep(kwargs['delay'])

while not stdout_queue.empty():
    #while (not stdout_queue.empty()) and ((len(bulkrequestslist) <= 1 and countthisbatch < nbatch) or (handler.nlocks() >= controllerconfigdoc['options']['nworkers'])):
    line = stdout_queue.get().rstrip("\n")
    #with open(controllerpath + "/logs/" + jobstepname + ".test", "a") as teststream:
    #    teststream.write(get_timestamp() + " UTC: Print: " + line + "\n")
    #    teststream.flush()
    if line not in ignoredstrings:
        line_split = line.split()
        if len(line_split) > 0:
            line_action = line_split[0]
        if line == "":
            with open(controllerpath + "/" + kwargs["modname"] + "_" + kwargs["controllername"] + ".config", "r") as controllerconfigstream:
                controllerconfigdoc = yaml.load(controllerconfigstream)
            if controllerconfigdoc["options"]["markdone"] == None:
                controllerconfigdoc["options"]["markdone"] = ""
            if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
                #cputime = eval("%.2f" % handler.stat("TotalCPUTime"))
                #maxrss = handler.max_stat("Rss")
                #maxvmsize = handler.max_stat("Size")
                stats = stats_queue.get()
                cputime = eval("%.4f" % stats["stats"]["totalcputime"])
                maxrss = stats["max"]["rss"]
                maxvmsize = stats["max"]["size"]
            #    stats = getstats("sstat", ["MaxRSS", "MaxVMSize"], kwargs['stepid'])
            #    if (len(stats) == 1) and (stats[0] == ""):
            #        newtotcputime, maxrss, maxvmsize = [eval(x) for x in getstats("sacct", ["CPUTimeRAW", "MaxRSS", "MaxVMSize"], kwargs['stepid'])]
            #    else:
            #        newtotcputime = eval(getstats("sacct", ["CPUTimeRAW"], kwargs['stepid'])[0])
            #        maxrss, maxvmsize = stats
            #    cputime = newtotcputime-totcputime
            #    totcputime = newtotcputime
            #newcollection, strindexdoc = linehead[1:].split("<")[0].split(".")
            #newindexdoc = json.loads(strindexdoc)
            newcollection = controllerconfigdoc['db']['basecollection']
            doc = json.loads(intermed_queue.get())
            newindexdoc = dict([(x, doc[x]) for x in kwargs['dbindexes']]);
            outext = newcollection + ".set"
            #runtime = "%.2f" % handler.stat("ElapsedTime")
            #runtime = "%.2f" % stats["stats"]["elapsedtime"]
            in_intermedtime = stats["in_timestamp"]
            out_intermedtime = stats["out_timestamp"]
            if controllerconfigdoc['options']['intermedlog'] or controllerconfigdoc['options']['outlog']:
                if controllerconfigdoc['options']['intermedlog']:
                    with open(controllerpath + "/logs/" + jobstepname + ".log.intermed", "a") as intermedlogstream:
                        intermedlogstream.write(out_intermedtime + " " + in_intermedtime + " " + str(dir_size(controllerpath)) + " " + ("%.2f" % cputime) + " " + str(maxrss) + " " + str(maxvmsize) + " " + str(bsonsize) + " " + json.dumps(newindexdoc, separators = (',', ':')) + "\n")
                        intermedlogstream.flush()
                if controllerconfigdoc['options']['outlog']:
                    batch_dict['log'] += out_intermedtime + " " + in_intermedtime + " " + str(dir_size(controllerpath)) + " " + ("%.2f" % cputime) + " " + str(maxrss) + " " + str(maxvmsize) + " " + str(bsonsize) + " " + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
            statsmark = {}
            if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
                statsmark.update({modname + "STATS": {"CPUTIME": cputime, "MAXRSS": maxrss, "MAXVMSIZE": maxvmsize, "BSONSIZE": bsonsize}})
            if controllerconfigdoc['options']['markdone'] != "":
                statsmark.update({modname + controllerconfigdoc['options']['markdone']: True})
            # Testing
            #statsmark.update({"HOST": os.environ['HOSTNAME'], "STEP": "job_" + kwargs['input_file'].split("_job_")[1], "NBATCH": nbatch, "TIME": get_timestamp() + " UTC"})
            # Done testing
            if len(statsmark) > 0:
                #mergeddoc = merge_two_dicts(newindexdoc, statsmark)
                #lineout = json.dumps(mergeddoc, separators = (',',':'))
                lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(statsmark, separators = (',',':'))
                if controllerconfigdoc['options']['intermedlocal']:
                    with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                        intermediostream.write(lineout + "\n")
                        intermediostream.flush()
                if controllerconfigdoc['options']['statslocal']:
                    #outiolist[-1] += "CPUTime: " + str(cputime) + " seconds\n"
                    #outiolist[-1] += "MaxRSS: " + str(maxrss) + " bytes\n"
                    #outiolist[-1] += "MaxVMSize: " + str(maxvmsize) + " bytes\n"
                    #outiolist[-1] += "BSONSize: " + str(bsonsize) + " bytes\n"
                    if outext not in batch_dict['io'].keys():
                        batch_dict['io'][outext] = ""
                    batch_dict['io'][outext] += lineout + "\n"
                #if controllerconfigdoc['options']['outlocal']:
                #    outiolist[-1] += newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                #if newcollection not in bulkcolls.keys():
                    #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                #    if controllerconfigdoc['db']['type'] == "mongodb":
                #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                if newcollection not in batch_dict['requests'].keys():
                    batch_dict['requests'][newcollection] = []
                #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": statsmark})
                if controllerconfigdoc['db']['type'] == "mongodb":
                    batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$set": statsmark}, upsert = True)]
            bsonsize = 0
            countthisbatch += 1
            batch_dict['count'] += 1
            if countthisbatch == nbatch:
                write_queue.put(batch_dict)
                #bulkrequestslist += [{}]
                #if controllerconfigdoc['options']['outlog']:
                #    outloglist += [""]
                #if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
                #    outiolist += [{}]
                batch_dict = {'requests': {}, 'count': 0}
                if controllerconfigdoc['options']['outlog']:
                    batch_dict['log'] = ""
                if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
                    batch_dict['io'] = {}
                countthisbatch = 0
                nbatch = randint(1, controllerconfigdoc['options']['nbatch']) if kwargs['random_nbatch'] else controllerconfigdoc['options']['nbatch']
                #countallbatches += [0]
        elif line[0] == "#":
            if controllerconfigdoc['options']['intermedlog'] or controllerconfigdoc['options']['outlog']:
                if controllerconfigdoc['options']['intermedlog']:
                    with open(controllerpath + "/logs/" + jobstepname + ".log.intermed", "a") as intermedlogstream:
                        intermedlogstream.write("# " + line + "\n")
                        intermedlogstream.flush()
                if controllerconfigdoc['options']['outlog']:
                    with open(controllerpath + "/logs/" + jobstepname + ".log", "a") as outlogstream:
                        outlogstream.write("# " + line + "\n")
                        outlogstream.flush()
        elif len(line_split) == 4:
            #linehead = re.sub("^([-+&@#].*?>|None).*", r"\1", line)
            #linemarker = linehead[0]
            if line_action == "unset":
                newcollection = line_split[1]
                newindexdoc = json.loads(line_split[2])
                #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                doc = json.loads(line_split[3])
                #mergeddoc = merge_two_dicts(newindexdoc, doc)
                #lineout = json.dumps(mergeddoc, separators = (',',':'))
                lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(doc, separators = (',',':'))
                outext = newcollection + "." + line_action
                if controllerconfigdoc['db']['type'] == "mongodb":
                    bsonsize -= get_bsonsize(doc)
                if controllerconfigdoc['options']['intermedlocal']:
                    with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                        intermediostream.write(lineout + "\n")
                        intermediostream.flush()
                if controllerconfigdoc['options']['outlocal']:
                    if outext not in batch_dict['io'].keys():
                        batch_dict['io'][outext] = ""
                    batch_dict['io'][outext] += lineout + "\n"
                if controllerconfigdoc['options']['outdb']:
                    #if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                    #    if controllerconfigdoc['db']['type'] == "mongodb":
                    #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in batch_dict['requests'].keys():
                        batch_dict['requests'][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).update({"$unset": doc})
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$unset": doc})]
            elif line_action == "set":
                newcollection = line_split[1]
                newindexdoc = json.loads(line_split[2])
                #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                doc = json.loads(line_split[3])
                #mergeddoc = merge_two_dicts(newindexdoc, doc)
                #lineout = json.dumps(mergeddoc, separators = (',',':'))
                lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(doc, separators = (',',':'))
                outext = newcollection + "." + line_action
                if controllerconfigdoc['db']['type'] == "mongodb":
                    bsonsize += get_bsonsize(doc)
                if controllerconfigdoc['options']['intermedlocal']:
                    with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                        intermediostream.write(lineout + "\n")
                        intermediostream.flush()
                if controllerconfigdoc['options']['outlocal']:
                    if outext not in batch_dict['io'].keys():
                        batch_dict['io'][outext] = ""
                    batch_dict['io'][outext] += lineout + "\n"
                if controllerconfigdoc['options']['outdb']:
                    #if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                    #    if controllerconfigdoc['db']['type'] == "mongodb":
                    #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in batch_dict['requests'].keys():
                        batch_dict['requests'][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": doc})
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$set": doc}, upsert = True)]
            elif line_action == "addToSet":
                newcollection = line_split[1]
                newindexdoc = json.loads(line_split[2])
                #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                doc = json.loads(line_split[3])
                #mergeddoc = merge_two_dicts(newindexdoc, doc)
                #lineout = json.dumps(mergeddoc, separators = (',',':'))
                lineout = json.dumps(newindexdoc, separators = (',',':')) + " " + json.dumps(doc, separators = (',',':'))
                outext = newcollection + "." + line_action
                if controllerconfigdoc['db']['type'] == "mongodb":
                    bsonsize += get_bsonsize(doc)
                if controllerconfigdoc['options']['intermedlocal']:
                    with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                        intermediostream.write(lineout + "\n")
                        intermediostream.flush()
                if controllerconfigdoc['options']['outlocal']:
                    if outext not in batch_dict['io'].keys():
                        batch_dict['io'][outext] = ""
                    batch_dict['io'][outext] += lineout + "\n"
                if controllerconfigdoc['options']['outdb']:
                    #if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                    #    if controllerconfigdoc['db']['type'] == "mongodb":
                    #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in batch_dict['requests'].keys():
                        batch_dict['requests'][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        batch_dict['requests'][newcollection] += [UpdateOne(newindexdoc, {"$addToSet": doc}, upsert = True)]
            elif line_action == "insert":
                newcollection = line_split[1]
                newindexdoc = json.loads(line_split[2])
                #linedoc = re.sub("^[-+&@#].*?>", "", line).rstrip("\n")
                doc = json.loads(line_split[3])
                mergeddoc = merge_two_dicts(newindexdoc, doc)
                lineout = json.dumps(mergeddoc, separators = (',',':'))
                outext = newcollection + "." + line_action
                if controllerconfigdoc['db']['type'] == "mongodb":
                    bsonsize += get_bsonsize(doc)
                if controllerconfigdoc['options']['intermedlocal']:
                    with open(controllerpath + "/bkps/" + jobstepname + "." + outext + ".intermed", "a") as intermediostream:
                        intermediostream.write(lineout + "\n")
                        intermediostream.flush()
                if controllerconfigdoc['options']['outlocal']:
                    if outext not in batch_dict['io'].keys():
                        batch_dict['io'][outext] = ""
                    batch_dict['io'][outext] += lineout + "\n"
                if controllerconfigdoc['options']['outdb']:
                    #if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                    #    if controllerconfigdoc['db']['type'] == "mongodb":
                    #        bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in batch_dict['requests'].keys():
                        batch_dict['requests'][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
                    if controllerconfigdoc['db']['type'] == "mongodb":
                        batch_dict['requests'][newcollection] += [InsertOne(mergeddoc)]
        else:
            #if controllerconfigdoc['options']['intermedlocal']:
            #    intermediostream.write(line + "\n")
            #    intermediostream.flush()
            #if controllerconfigdoc['options']['outlocal']:
            #    outiolist[-1] += line + "\n"
            try:
                raise IndexError("Modules should only output commented lines, blank lines separating processed input documents, or line with 4 columns representing: collection name, update action, index document, output document.")
            except IndexError as e:
                with open(controllerpath + "/logs/" + jobstepname + ".err", "a") as errfilestream:
                    traceback.print_exc(file = errfilestream)
                    errfilestream.flush()
                raise
            #if handler.nlocks() < controllerconfigdoc['options']['nworkers']:
            #    lockfile = controllerpath + "/locks/" + kwargs['stepid'] + ".lock"
            #    with open(lockfile, 'w') as lockstream:
            #        lockstream.write(str(countallbatches))
            #        lockstream.flush()
            #    for bulkcoll in bulkdict.keys():
            #        try:
            #            bulkdict[bulkcoll].execute()
            #        except BulkWriteError as bwe:
            #            pprint(bwe.details)
            #    while True:
            #        try:
            #            fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
            #            break
            #        except IOError:
            #            sleep(0.01)
            #    sys.stdout.write(intermediostream.getvalue())
            #    sys.stdout.flush()
            #    fcntl.flock(sys.stdout, fcntl.LOCK_UN)
            #    bulkdict = {}
            #    intermediostream = cStringIO.StringIO()
            #    countallbatches = 0
            #    os.remove(lockfile)

    #while not stderr_queue.empty():
    #    stderr_line = stderr_queue.get().rstrip("\n")
    #    if stderr_line not in ignoredstrings:
    #        if kwargs['controllername'] != None:
    #            exitcode = get_exitcode(kwargs['stepid'])
    #        with open(controllerpath + "/logs/" + jobstepname + ".err", "a") as errstream:
    #            if kwargs['controllername'] != None:
    #                errstream.write("ExitCode: " + exitcode + "\n")
    #            errstream.write(stderr_line + "\n")
    #            errstream.flush()
    #        #while True:
    #        #    try:
    #        #        fcntl.flock(sys.stderr, fcntl.LOCK_EX | fcntl.LOCK_NB)
    #        #        break
    #        #    except IOError:
    #        #        sleep(0.01)
    #        sys.stderr.write(stderr_line + "\n")
    #        sys.stderr.flush()
    #        #fcntl.flock(sys.stderr, fcntl.LOCK_UN)

    #if (len(bulkrequestslist) > 1) and (handler.nlocks() < controllerconfigdoc['options']['nworkers']):
        #with open(controllerpath + "/logs/" + jobstepname + ".test", "a") as teststream:
        #    teststream.write(get_timestamp() + " UTC: Work\n")
        #    teststream.flush()
        #if handler.nlocks() >= controllerconfigdoc['options']['nworkers']:
        #    overlocked = True
        #    os.kill(process.pid, signal.SIGSTOP)
        #    while handler.nlocks() >= controllerconfigdoc['options']['nworkers']:
        #        sleep(0.01)
        #else:
        #    overlocked = False
    #    lockfile = controllerpath + "/locks/" + jobstepname + ".lock"
    #    with open(lockfile, 'w') as lockstream:
    #        lockstream.write(kwargs['stepid'] + ": Writing " + str(countallbatches[0]) + " items.")
    #        lockstream.flush()
    #    countbatchesout += countallbatches[0]
    #    del countallbatches[0]
        #print(bulkdict)
        #sys.stdout.flush()

    #    if controllerconfigdoc['options']['outlog']:
    #        in_writetime = time()

    #    if controllerconfigdoc['db']['type'] == "mongodb":
    #        for coll, requests in bulkrequestslist[0].items():
    #            try:
                    #bulkdict[bulkcoll].execute()
    #                bulkcolls[coll].bulk_write(requests, ordered = False)
    #            except BulkWriteError as bwe:
    #                pprint(bwe.details)
    #        del bulkrequestslist[0]
        #while True:
        #    try:
        #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
        #        break
        #    except IOError:
        #        sleep(0.01)
        #intermediostream.close()
        #with open(workpath + "/" + stepname + ".temp", "r") as intermediostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
        #    for line in intermediostream:
        #        iostream.write(line)
        #        iostream.flush()
        #    os.remove(intermediostream.name)
        #sys.stdout.write(intermediostream.getvalue())
        #sys.stdout.flush()
    #    if controllerconfigdoc['options']['outlog']:
            #print(len(outloglist[0].rstrip("\n").split("\n")))
            #sys.stdout.flush()
    #        writetime = time() - in_writetime
    #        outlogiosplit = outloglist[0].rstrip("\n").split("\n")
    #        avgwritetime = "%.2f" % (writetime / len(outlogiosplit))
            #outlogiotime = ""
    #        for outlogioline in outlogiosplit:
    #            if outlogioline != "":
    #                outlogstream.write(get_timestamp() + " " + str(avgwritetime) + "s " + outlogioline + "\n")
    #                outlogstream.flush()
    #                countlogout += 1
    #        del outloglist[0]
    #        if controllerconfigdoc['options']['intermedlog'] and countbatchesout >= kwargs["cleanup"]:
    #            cleanup_intermedlog(controllerpath, jobstepname, countlogout)
    #            countlogout = 0
        #if controllerconfigdoc['options']['intermedlocal']:
            #name = intermediostream.name
            #intermediostream.close()
            #os.remove(name)
            #intermediostream = open(name, "w")
    #    if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
    #        for outext, outio in outiolist[0].items():
    #            with open(controllerpath + "/bkps/" + jobstepname + "." + outext, "a") as outiostream:
    #                outiosplit = outio.rstrip("\n").split("\n")
    #                for outioline in outlogiosplit:
    #                    if outioline != "":
    #                        outiostream.write(outioline + "\n")
    #                        outiostream.flush()
    #                        if outext in countioouts.keys():
    #                            countioouts[outext] += 1
    #                        else:
    #                            countioouts[outext] = 1
    #        del outiolist[0]
    #        if controllerconfigdoc['options']['intermedlocal'] and countbatchesout >= kwargs["cleanup"]:
    #            cleanup_intermedio(controllerpath, jobstepname, countioouts)
    #            countioouts = {}

    #    if countbatchesout >= kwargs["cleanup"]:
    #        countbatchesout = 0
        #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
        #bulkdict = {}
        #intermediostream = open(workpath + "/" + stepname + ".temp", "w")
        #countallbatches = 0
    #    os.remove(lockfile)
        #if overlocked:
        #    os.kill(process.pid, signal.SIGCONT)

#while len(bulkrequestslist) > 0:
#    while handler.nlocks() >= controllerconfigdoc['options']['nworkers']:
#        sleep(kwargs['delay'])

#    if (len(bulkrequestslist) > 0) and (handler.nlocks() < controllerconfigdoc['options']['nworkers']):
        #with open(controllerpath + "/logs/" + jobstepname + ".test", "a") as teststream:
        #    teststream.write(get_timestamp() + " UTC: Work\n")
        #    teststream.flush()
#        lockfile = controllerpath + "/locks/" + jobstepname + ".lock"
#        with open(lockfile, 'w') as lockstream:
#            lockstream.write(kwargs['stepid'] + ": Writing " + str(countallbatches[0]) + " items.")
#            lockstream.flush()
#        countbatchesout += countallbatches[0]
#        del countallbatches[0]
        #print(bulkdict)
        #sys.stdout.flush()

#        if controllerconfigdoc['options']['outlog']:
#            in_writetime = time()

#        if controllerconfigdoc['db']['type'] == "mongodb":
#            for coll, requests in bulkrequestslist[0].items():
#                try:
                    #bulkdict[bulkcoll].execute()
#                    bulkcolls[coll].bulk_write(requests, ordered = False)
#                except BulkWriteError as bwe:
#                    pprint(bwe.details)
#            del bulkrequestslist[0]
        #while True:
        #    try:
        #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
        #        break
        #    except IOError:
        #        sleep(0.01)
        #intermediostream.close()
        #with open(workpath + "/" + stepname + ".temp", "r") as intermediostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
        #    for line in intermediostream:
        #        iostream.write(line)
        #        iostream.flush()
        #    os.remove(intermediostream.name)
        #sys.stdout.write(intermediostream.getvalue())
        #sys.stdout.flush()
#        if controllerconfigdoc['options']['outlog']:
            #print(len(outloglist[0].rstrip("\n").split("\n")))
            #sys.stdout.flush()
#            writetime = time() - in_writetime
#            outlogiosplit = outloglist[0].rstrip("\n").split("\n")
#            avgwritetime = "%.2f" % (writetime / len(outlogiosplit))
            #outlogiotime = ""
#            for outlogioline in outlogiosplit:
#                if outlogioline != "":
#                    outlogstream.write(get_timestamp() + " " + str(avgwritetime) + "s " + outlogioline + "\n")
#                    outlogstream.flush()
#                    countlogout += 1
#            del outloglist[0]
#            if controllerconfigdoc['options']['intermedlog'] and countbatchesout >= kwargs["cleanup"]:
#                cleanup_intermedlog(controllerpath, jobstepname, countlogout)
#                countlogout = 0
        #if controllerconfigdoc['options']['intermedlocal']:
            #name = intermediostream.name
            #intermediostream.close()
            #os.remove(name)
            #intermediostream = open(name, "w")
#        if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
#            for outext, outio in outiolist[0].items():
#                with open(controllerpath + "/bkps/" + jobstepname + "." + outext, "a") as outiostream:
#                    outiosplit = outio.rstrip("\n").split("\n")
#                    for outioline in outlogiosplit:
#                        if outioline != "":
#                            outiostream.write(outioline + "\n")
#                            outiostream.flush()
#                            if outext in countioouts.keys():
#                                countioouts[outext] += 1
#                            else:
#                                countioouts[outext] = 1
#            del outiolist[0]
#            if controllerconfigdoc['options']['intermedlocal'] and countbatchesout >= kwargs["cleanup"]:
#                cleanup_intermedio(controllerpath, jobstepname, countioouts)
#                countioouts = {}

#        if countbatchesout >= kwargs["cleanup"]:
#            countbatchesout = 0
        #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
        #bulkdict = {}
        #intermediostream = open(workpath + "/" + stepname + ".temp", "w")
        #countallbatches = 0
#        os.remove(lockfile)
        #if overlocked:
        #    os.kill(process.pid, signal.SIGCONT)

if batch_dict['count'] > 0:
    write_queue.put(batch_dict)

#handler.signal()
handler.join()
dbhandler.signal()
dbhandler.join()

#if kwargs['input_file'] != None:
#    stdin_file.close()

if controllerconfigdoc['options']['intermedlog']:
    #intermedlogstream.close()
    os.remove(controllerpath + "/logs/" + jobstepname + ".log.intermed")

#if controllerconfigdoc['options']['outlog']:
#    outlogstream.close()

if controllerconfigdoc['options']['intermedlocal']:
    #if not intermediostream.closed:
    #    intermediostream.close()
    #if os.path.exists(intermediostream.name):
    #    os.remove(intermediostream.name)
    for intermediofilename in iglob(controllerpath + "/bkps/" + jobstepname + ".*.intermed"):
        os.remove(intermediofilename)
#if controllerconfigdoc['options']['outlocal'] or controllerconfigdoc['options']['statslocal']:
#    outiostream.close()
#stderr_reader.join()
#if controllerconfigdoc['options']['statslocal'] or controllerconfigdoc['options']['statsdb']:
#    stats_reader.join()

while True:
    try:
        flock(sys.stdout, LOCK_EX | LOCK_NB)
        break
    except IOError as e:
        if e.errno != EAGAIN:
            raise
        else:
            sleep(0.1)
print(get_timestamp() + " " + jobstepname + " END")
sys.stdout.flush()
flock(sys.stdout, LOCK_UN)

process.stdin.close()
process.stdout.close()
process.stderr.close()

#if controllerconfigdoc['db']['type'] == "mongodb":
#    dbclient.close()