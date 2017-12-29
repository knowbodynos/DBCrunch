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

import sys, os, glob, json, yaml, re, tempfile, datetime #, linecache, traceback, fcntl
from pprint import pprint
from time import time, sleep
from random import randint
from subprocess import Popen, PIPE
from threading import Thread
from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read
from locale import getpreferredencoding
from argparse import ArgumentParser, REMAINDER
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

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

class AsynchronousThreadStreamReader(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, stream, queue):
        assert isinstance(queue, Queue)
        assert callable(stream.readline)
        Thread.__init__(self)
        self._stream = stream
        self._queue = queue
        self.daemon = True

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        for line in iter(self._stream.readline, ''):
            self._queue.put(line)

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return (not self.is_alive()) and self._queue.empty()

class AsynchronousThreadStatsStreamReaderWriter(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, workpath, filename, pid, in_stream, out_stream, err_stream, in_iter_arg, in_iter_file, in_queue, temp_queue, out_queue, stepid = None, ignoredstrings = [], stats = None, delimiter = '', cleanup = None, time_limit = None, start_time = None):
        assert hasattr(in_iter_arg, '__iter__')
        assert isinstance(in_iter_file, file) or in_iter_file == None
        assert isinstance(in_queue, Queue)
        assert isinstance(out_queue, Queue)
        #assert isinstance(err_queue, Queue)
        assert callable(in_stream.write)
        #assert callable(out_stream.readline)
        #assert callable(err_stream.readline)
        #assert callable(err_stream.write)
        Thread.__init__(self)
        self._workpath = workpath
        self._filename = filename
        self._pid = str(pid)
        self._instream = in_stream
        #self._outstream = out_stream
        self._outgen = nonblocking_readlines(out_stream)
        self._errgen = nonblocking_readlines(err_stream)
        self._initerarg = in_iter_arg
        self._initerfile = in_iter_file
        self._initerargflag = False
        #self._initerfileflag = False
        self._inqueue = in_queue
        self._tempqueue = temp_queue
        self._outqueue = out_queue
        #self._errqueue = err_queue
        self._delimiter = delimiter
        self._cleanup = cleanup
        self._cleanup_counter = 0
        self._timelimit = time_limit
        self._starttime = start_time
        self._stepid = stepid
        self._nlocks = 0
        self._ignoredstrings = ignoredstrings
        if stats == None:
            self._stats = stats
        else:
            self._stats = dict((s, 0) for s in stats)
            self._proc_smaps = open("/proc/" + str(pid) + "/smaps", "r")
            self._proc_stat = open("/proc/" + str(pid) + "/stat", "r")
            self._proc_uptime = open("/proc/uptime", "r")
            self._maxstats = dict((s, 0) for s in stats)
            self._totstats = dict((s, 0) for s in stats)
            self._nstats = 0
            self._lower_keys = [k.lower() for k in self._stats.keys()]
            self._time_keys = ["elapsedtime", "totalcputime", "parentcputime", "childcputime", "parentcpuusage", "childcpuusage", "totalcpuusage"]
            self._hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

        self.daemon = True

    def is_inprog(self):
        '''Check whether there is no more content to expect.'''
        return self.is_alive() and os.path.exists("/proc/" + self._pid + "/smaps")

    def write_stdin(self):
        try:
            in_line = self._initerarg.next()
        except StopIteration:
            self._initerargflag = True
            if self._initerfile == None or self._initerfile.closed:
                #in_line = self._inqueue.get()
                in_line = sys.stdin.readline().rstrip("\n")
                if in_line == "":
                    self._instream.close()
                    #print("finally!")
                    #sys.stdout.flush()
                else:
                    self._tempqueue.put(in_line)
                    self._instream.write(in_line + self._delimiter)
                    #print("a: " + in_line + self._delimiter)
                    #sys.stdout.flush()
                    self._instream.flush()
            else:
                in_line = self._initerfile.readline().rstrip("\n")
                if in_line == "":
                    name = self._initerfile.name
                    self._initerfile.close()
                    os.remove(name)
                    #self._initerfileflag = True
                    #in_line = self._inqueue.get()
                    in_line = sys.stdin.readline().rstrip("\n")
                    if in_line == "":
                        self._instream.close()
                        #print("finally!")
                        #sys.stdout.flush()
                    else:
                        self._tempqueue.put(in_line)
                        self._instream.write(in_line + self._delimiter)
                        #print("a: " + in_line + self._delimiter)
                        #sys.stdout.flush()
                        self._instream.flush()
                else:
                    self._tempqueue.put(in_line)
                    self._instream.write(in_line + self._delimiter)
                    #print("a: " + in_line + self._delimiter)
                    #sys.stdout.flush()
                    self._instream.flush()
                    if (self._cleanup != None and self._cleanup_counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time() - self._starttime >= self._timelimit):
                        with tempfile.NamedTemporaryFile(dir = "/".join(self._initerfile.name.split("/")[:-1]), delete = False) as tempstream:
                            in_line = self._initerfile.readline()
                            while in_line != "":
                                tempstream.write(in_line)
                                tempstream.flush()
                                in_line = self._initerfile.readline()
                            name = self._initerfile.name
                            self._initerfile.close()
                            os.rename(tempstream.name, name)
                            self._initerfile = open(name, 'r')
                        self._cleanup_counter = 0
        else:
            self._tempqueue.put(in_line)
            self._instream.write(in_line + self._delimiter)
            #print("a: " + in_line + self._delimiter)
            #sys.stdout.flush()
            self._instream.flush()
            #self._instream.close()
            #print(in_line)
            #sys.stdout.flush()

    def get_stats(self):
        if any([k in self._lower_keys for k in self._time_keys]):
            self._proc_stat.seek(0)
            stat_line = self._proc_stat.read() if self.is_inprog() else ""
            #print(stat_line)
            #sys.stdout.flush()
            if stat_line != "":
                stat_line_split = stat_line.split()
                utime, stime, cutime, cstime = [(float(f) / self._hz) for f in stat_line_split[13:17]]
                starttime = float(stat_line_split[21]) / self._hz
                parent_cputime = utime + stime
                child_cputime = cutime + cstime
                total_cputime = parent_cputime + child_cputime
                self._proc_uptime.seek(0)
                uptime_line = self._proc_uptime.read()
                uptime = float(uptime_line.split()[0])
                elapsedtime = uptime - self._starttime
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
                if stat_name.lower() in self._lower_keys:
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
        #sleep(self._statsdelay)

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        self.write_stdin()
        while self.is_inprog():
            try:
                err_line = self._errgen.next()
            except StopIteration:
                err_line = ""
                pass
            while err_line != "":
                err_line = err_line.rstrip("\n")
                if err_line not in self._ignoredstrings:
                    with open(self._filename + ".err", "a") as errfilestream:
                        if self._stepid != None:
                            exitcode = get_exitcode(self._stepid)
                        if self._stepid != None:
                            errfilestream.write("ExitCode: " + exitcode + "\n")
                        errfilestream.write(err_line + "\n")
                        errfilestream.flush()
                    sys.stderr.write(err_line + "\n")
                    sys.stderr.flush()
                try:
                    err_line = self._errgen.next()
                except StopIteration:
                    break
                        
            try:
                out_line = self._outgen.next()
            except StopIteration:
                out_line = ""
                pass
            while out_line != "":
                #for out_line in iter(self._outstream.readline, ''):
                out_line = out_line.rstrip("\n")
                #if out_line == "\n".decode('string_escape'):
                #sys.stdout.write(self._filename+" out_line: \""+out_line+"\"\n")
                #sys.stdout.flush()
                if out_line == "@":
                    if self._stats != None:
                        self.get_stats()
                        self._nlocks = len(glob.glob(self._workpath + "/*.lock"))
                    self.write_stdin()
                self._outqueue.put(out_line.rstrip("\n"))
                try:
                    out_line = self._outgen.next()
                except StopIteration:
                    break 

    #def waiting(self):
    #    return self.is_alive() and self._initerargflag and self._initerfile.closed and self._inqueue.empty() and self._outqueue.empty()

    def nlocks(self):
        return self._nlocks

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return (not self.is_alive()) and self._initerargflag and self._initerfile.closed and self._inqueue.empty() and self._outqueue.empty()

    def stat(self, stat_name):
        '''Check whether there is no more content to expect.'''
        return self._stats[stat_name]

    def stats(self):
        '''Check whether there is no more content to expect.'''
        return self._stats

    def max_stat(self, stat_name):
        '''Check whether there is no more content to expect.'''
        return self._maxstats[stat_name]

    def max_stats(self):
        '''Check whether there is no more content to expect.'''
        return self._maxstats

    def avg_stat(self, stat_name):
        '''Check whether there is no more content to expect.'''
        return self._totstats[stat_name] / self._nstats if self._nstats > 0 else 0

    def avg_stats(self):
        '''Check whether there is no more content to expect.'''
        return dict((k, self._totstats[k] / self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())

class AsynchronousThreadStatsReader(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
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
        '''Check whether there is no more content to expect.'''
        return self.is_alive() and os.path.exists("/proc/" + self._pid + "/smaps")

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        lower_keys = [k.lower() for k in self._stats.keys()]
        time_keys = ["elapsedtime", "totalcputime", "parentcputime", "childcputime", "parentcpuusage", "childcpuusage", "totalcpuusage"]
        if any([k in lower_keys for k in time_keys]):
            hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        while self.is_inprog():
            #for stat_name in self._stats.keys():
            #    self._stats[stat_name] = 0
            if any([k in self._lower_keys for k in self._time_keys]):
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
                    if stat_name.lower() in self._lower_keys:
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
        '''Check whether there is no more content to expect.'''
        return self._stats[stat_name]

    def stats(self):
        '''Check whether there is no more content to expect.'''
        return self._stats

    def max_stat(self, stat_name):
        '''Check whether there is no more content to expect.'''
        return self._maxstats[stat_name]

    def max_stats(self):
        '''Check whether there is no more content to expect.'''
        return self._maxstats

    def avg_stat(self, stat_name):
        '''Check whether there is no more content to expect.'''
        return self._totstats[stat_name] / self._nstats if self._nstats > 0 else 0

    def avg_stats(self):
        '''Check whether there is no more content to expect.'''
        return dict((k, self._totstats[k] / self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())

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

parser.add_argument('--nbatch', '-n', dest = 'nbatch', action = 'store', default = 1, help = '')
parser.add_argument('--nworkers', '-N', dest = 'nworkers', action = 'store', default = 1, help = '')
parser.add_argument('--random-nbatch', dest = 'random_nbatch', action = 'store_true', default = False, help = '')

parser.add_argument('--dbtype', dest = 'dbtype', action = 'store', default = None, help = '')
parser.add_argument('--dbhost', dest = 'dbhost', action = 'store', default = None, help = '')
parser.add_argument('--dbport', dest = 'dbport', action = 'store', default = None, help = '')
parser.add_argument('--dbusername', dest = 'dbusername', action = 'store', default = None, help = '')
parser.add_argument('--dbpassword', dest = 'dbpassword', action = 'store', default = None, help = '')
parser.add_argument('--dbname', dest = 'dbname', action = 'store', default = None, help = '')

parser.add_argument('--logging', dest = 'logging', action = 'store_true', default = False, help = '')
parser.add_argument('--temp-local', '-t', dest = 'templocal', action = 'store_true', default = False, help = '')
parser.add_argument('--write-local', '-w', dest = 'writelocal', action = 'store_true', default = False, help = '')
parser.add_argument('--write-db', '-W', dest = 'writedb', action = 'store_true', default = False, help = '')
parser.add_argument('--stats-local', '-s', dest = 'statslocal', action = 'store_true', default = False, help = '')
parser.add_argument('--stats-db', '-S', dest = 'statsdb', action = 'store_true', default = False, help = '')
parser.add_argument('--basecoll', dest = 'basecoll', action = 'store', default = None, help = '')
parser.add_argument('--dbindexes', dest = 'dbindexes', nargs = '+', default = None, help = '')
parser.add_argument('--markdone', dest = 'markdone', action = 'store', default = "MARK", help = '')

parser.add_argument('--delay', dest = 'delay', action = 'store', default = 0, help = '')
parser.add_argument('--stats', dest = 'stats_list', nargs = '+', action = 'store', default = [], help = '')
#parser.add_argument('--stats-delay', dest = 'stats_delay', action = 'store', default = 0, help = '')
parser.add_argument('--delimiter', '-d', dest = 'delimiter', action = 'store', default = '\n', help = '')
parser.add_argument('--input', '-i', dest = 'input_list', nargs = '+', action = 'store', default = [], help = '')
parser.add_argument('--file', '-f', dest = 'input_file', action = 'store', default = None, help = '')
parser.add_argument('--cleanup-after', dest = 'cleanup', action = 'store', default = None, help = '')
parser.add_argument('--interactive', dest = 'interactive', action = 'store_true', default = False, help = '')
parser.add_argument('--time-limit', dest = 'time_limit', action = 'store', default = None, help = '')
parser.add_argument('--ignored-strings', dest = 'ignoredstrings', nargs = '+', default = [], help = '')
parser.add_argument('--script-language', dest = 'scriptlanguage', action = 'store', default = None, help = '')
parser.add_argument('--script', '-c', dest = 'scriptcommand', nargs = '+', required = True, help = '')
parser.add_argument('--args', '-a', dest = 'scriptargs', nargs = REMAINDER, default = [], help = '')

kwargs = vars(parser.parse_known_args()[0])

#print(kwargs['input_list'])
#sys.stdout.flush()

if kwargs['time_limit'] == None:
    start_time = None
else:
    start_time = time()
    kwargs['time_limit'] = timestamp2unit(kwargs['time_limit'])

kwargs['delay'] = float(kwargs['delay'])
#kwargs['stats_delay'] = float(kwargs['stats_delay'])
kwargs['nbatch'] = int(kwargs['nbatch'])
kwargs['nworkers'] = int(kwargs['nworkers'])
#kwargs['delimiter'] = kwargs['delimiter']#.decode("string_escape")
if kwargs['cleanup'] == "":
    kwargs['cleanup'] = None
else:
    kwargs['cleanup'] = eval(kwargs['cleanup'])

if kwargs['modname'] == None:
    modname = kwargs['scriptcommand'][-1].split("/")[-1].split(".")[0]
else:
    modname = kwargs['modname']

script = " ".join(kwargs['scriptcommand'] + kwargs['scriptargs'])

ignoredstrings = kwargs['ignoredstrings']

if kwargs['controllername'] == None:
    rootpath = os.getcwd()
    workpath = rootpath
    dbtype = kwargs['dbtype']
    dbhost = kwargs['dbhost']
    dbport = kwargs['dbport']
    dbusername = kwargs['dbusername']
    dbpassword = kwargs['dbpassword']
    dbname = kwargs['dbname']
    basecoll = kwargs['basecoll']
else:
    rootpath = os.environ['CRUNCH_ROOT']
    #softwarefile = rootpath + "/state/software"
    #with open(softwarefile, "r") as softwarestream:
    #    softwarestream.readline()
    #    for line in softwarestream:
    #        ext = line.split(',')[2]
    #        if ext + " " in kwargs['scriptcommand']:
    #            break
    #modulesfile = rootpath + "/state/modules"
    #with open(modulesfile, "r") as modulesstream:
    #    modulesstream.readline()
    #    for line in modulesstream:
    #        modname = line.rstrip("\n")
    #        if " " + modname + ext + " " in kwargs['scriptcommand'] or "/" + modname + ext + " " in kwargs['scriptcommand']:
    #            break
    #controllerpath = rootpath + "/modules/" + modname + "/" + kwargs['controllername']
    with open(rootpath + "/config.yaml", "r") as configstream:
        configdoc = yaml.load(configstream)

    if (kwargs['scriptlanguage'] != None) and (kwargs['scriptlanguage'] in configdoc["software"].keys()) and ("ignored-strings" in configdoc["software"][kwargs['scriptlanguage']].keys()):
        ignoredstrings += configdoc["software"][kwargs['scriptlanguage']]["ignored-strings"]

    if configdoc["workload-manager"] == "slurm":
        from crunch_slurm import *

    controllerpath = "/".join(get_controllerpath(kwargs['stepid']).split("/")[:-1])
    workpath = controllerpath + "/jobs"
    if not os.path.isdir(workpath):
        os.mkdir(workpath)
    controllerfile = controllerpath + "/crunch_" + modname + "_" + kwargs['controllername'] + "_controller.job"
    with open(controllerfile, "r") as controllerstream:
        for controllerline in controllerstream:
            if "dbtype=" in controllerline:
                dbtype = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if dbtype == "":
                    dbtype = None
            elif "dbhost=" in controllerline:
                dbhost = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if dbhost == "":
                    dbhost = None
            elif "dbport=" in controllerline:
                dbport = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if dbport == "":
                    dbport = None
            elif "dbusername=" in controllerline:
                dbusername = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if dbusername == "":
                    dbusername = None
            elif "dbpassword=" in controllerline:
                dbpassword = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if dbpassword == "":
                    dbpassword = None
            elif "dbname=" in controllerline:
                dbname = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if dbname == "":
                    dbname = None
            elif "basecollection=" in controllerline:
                basecoll = controllerline.split("=")[1].lstrip("\"").rstrip("\"\n")
                if basecoll == "":
                    basecoll = None

if len(kwargs['input_list']) > 0:
    stdin_iter_arg = iter(kwargs['input_list'])
else:
    stdin_iter_arg = iter([])

if kwargs['input_file'] != None:
    if "/" not in kwargs['input_file']:
        kwargs['input_file'] = workpath + "/" + kwargs['input_file']
    stdin_iter_file = open(kwargs['input_file'], "r")
else:
    stdin_iter_file = None

if kwargs['input_file'] != None:
    filename = ".".join(kwargs['input_file'].split('.')[:-1])
    step = kwargs['input_file'].split('/')[-1].split('.')[0].split("_step_")[1]
else:
    filename = workpath + "/" + kwargs['stepid']
    step = kwargs['stepid'].split('.')[-1:]

if kwargs['logging']:
    logiolist = [""]

if kwargs['writelocal'] or kwargs['statslocal']:
    outiolist = [""]
    outiostream = open(filename + ".out", "w")
    if kwargs['templocal']:
        tempiostream = open(filename + ".temp", "w")
    if (kwargs['dbindexes'] == None) or ((kwargs['controllername'] == None) and (basecoll == None)):
        parser.error("Both --write-local and --stats-local require either the options:\n--controllername --dbindexes, \nor the options: --basecoll --dbindexes")

if any([kwargs[x] for x in ['writedb', 'statsdb']]):
    if (kwargs['dbindexes'] == None) or ((kwargs['controllername'] == None) and any([x == None for x in [dbtype, dbhost, dbport, dbusername, dbpassword, dbname, basecoll]])):
        parser.error("Both --writedb and --statsdb require either the options:\n--controllername --dbindexes, \nor the options:\n--dbtype --dbhost --dbport --dbusername --dbpassword --dbname --basecoll --dbindexes")
    else:
        if dbtype == "mongodb":
            from mongojoin import get_bsonsize
            from pymongo import MongoClient, UpdateOne, WriteConcern
            from pymongo.errors import BulkWriteError
            if dbusername == None:
                dbclient = MongoClient("mongodb://" + dbhost + ":" + dbport + "/" + dbname)
            else:
                dbclient = MongoClient("mongodb://" + dbusername + ":" + dbpassword + "@" + dbhost + ":" + dbport + "/" + dbname + "?authMechanism=SCRAM-SHA-1")

            db = dbclient[dbname]
        else:
            raise Exception("Only \"mongodb\" is currently supported.")

process = Popen(script, shell = True, stdin = PIPE, stdout = PIPE, stderr = PIPE, bufsize = 1)

stdin_queue = Queue()
if not kwargs['interactive']:
    stdin_queue.put("")

temp_queue = Queue()

stdout_queue = Queue()
#stdout_reader = AsynchronousThreadStreamReaderWriter(process.stdin, process.stdout, stdin_iter_arg, stdin_iter_file, stdin_queue, temp_queue, stdout_queue, delimiter = kwargs['delimiter'], cleanup = kwargs['cleanup'], time_limit = kwargs['time_limit'], start_time = start_time)
#stdout_reader.start()

#stderr_queue = Queue()
#stderr_reader = AsynchronousThreadStreamReader(process.stderr, stderr_queue)
#stderr_reader.start()

#if kwargs['statslocal'] or kwargs['statsdb']:
#    stats_reader = AsynchronousThreadStatsReader(process.pid, kwargs['stats_list'], stats_delay = kwargs['stats_delay'])
#    stats_reader.start()

handler = AsynchronousThreadStatsStreamReaderWriter(workpath, filename, process.pid, process.stdin, process.stdout, process.stderr, stdin_iter_arg, stdin_iter_file, stdin_queue, temp_queue, stdout_queue, stepid = kwargs['stepid'], ignoredstrings = kwargs['ignoredstrings'], stats = kwargs['stats_list'] if kwargs['statslocal'] or kwargs['statsdb'] else None, delimiter = kwargs['delimiter'], cleanup = kwargs['cleanup'], time_limit = kwargs['time_limit'], start_time = start_time)
handler.start()

bulkcolls = {}
bulkrequestslist = [{}]
countallbatches = [0]
#tempiostream = open(workpath + "/" + stepname + ".temp", "w")
bsonsize = 0
countthisbatch = 0
nbatch = randint(1, kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch']
#while process.poll() == None and stats_reader.is_inprog() and not (stdout_reader.eof() or stderr_reader.eof()):
while process.poll() == None and handler.is_inprog() and not handler.eof():
    #if handler.waiting():
    #    stdin_line = sys.stdin.readline().rstrip("\n")
    #    stdin_queue.put(stdin_line)

    while not stdout_queue.empty():
        while (not stdout_queue.empty()) and ((len(bulkrequestslist) <= 1 and countthisbatch < nbatch) or (handler.nlocks() >= kwargs['nworkers'])):
            line = stdout_queue.get().rstrip("\n")
            if line not in ignoredstrings:
                linehead = re.sub("^([-+&@].*?>|None).*", r"\1", line)
                linemarker = linehead[0]
                if linemarker == "-":
                    newcollection, strindexdoc = linehead[1:-1].split(".")
                    newindexdoc = json.loads(strindexdoc)
                    linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
                    doc = json.loads(linedoc)
                    if dbtype == "mongodb":
                        bsonsize -= get_bsonsize(doc)
                    if kwargs['templocal']:
                        tempiostream.write(line + "\n")
                        tempiostream.flush()
                    if kwargs['writelocal']:
                        outiolist[-1] += line + "\n"
                    if kwargs['writedb']:
                        if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                            if dbtype == "mongodb":
                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in bulkrequestslist[-1].keys():
                            bulkrequestslist[-1][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).update({"$unset": doc})
                        if dbtype == "mongodb":
                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$unset": doc})]
                elif linemarker == "+":
                    #tempiostream.write(linehead[1:-1].split(".") + "\n")
                    #sys.stdout.flush()
                    #print(line)
                    #print(linehead)
                    #sys.stdout.flush()
                    newcollection, strindexdoc = linehead[1:-1].split(".")
                    newindexdoc = json.loads(strindexdoc)
                    linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
                    doc = json.loads(linedoc)
                    if dbtype == "mongodb":
                        bsonsize += get_bsonsize(doc)
                    if kwargs['templocal']:
                        tempiostream.write(line + "\n")
                        tempiostream.flush()
                    if kwargs['writelocal']:
                        outiolist[-1] += line + "\n"
                    if kwargs['writedb']:
                        if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                            if dbtype == "mongodb":
                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in bulkrequestslist[-1].keys():
                            bulkrequestslist[-1][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": doc})
                        if dbtype == "mongodb":
                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": doc}, upsert = True)]
                elif linemarker == "&":
                    newcollection, strindexdoc = linehead[1:-1].split(".")
                    newindexdoc = json.loads(strindexdoc)
                    linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
                    doc = json.loads(linedoc)
                    if dbtype == "mongodb":
                        bsonsize += get_bsonsize(doc)
                    if kwargs['templocal']:
                        tempiostream.write(line + "\n")
                        tempiostream.flush()
                    if kwargs['writelocal']:
                        outiolist[-1] += line + "\n"
                    if kwargs['writedb']:
                        if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                            if dbtype == "mongodb":
                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in bulkrequestslist[-1].keys():
                            bulkrequestslist[-1][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
                        if dbtype == "mongodb":
                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$addToSet": doc}, upsert = True)]
                elif linemarker == "@":
                    if kwargs['statslocal'] or kwargs['statsdb']:
                        cputime = eval("%.2f" % handler.stat("TotalCPUTime"))
                        maxrss = handler.max_stat("Rss")
                        maxvmsize = handler.max_stat("Size")
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
                    newcollection = basecoll
                    doc = json.loads(temp_queue.get())
                    newindexdoc = dict([(x, doc[x]) for x in kwargs['dbindexes']]);               
                    if kwargs['logging']:
                        logiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + ">OUT\n"
                        sys.stdout.write("Step " + step + ": " + datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + ">TEMP\n")
                        sys.stdout.flush()
                    if kwargs['templocal']:
                        tempiostream.write(line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n")
                        tempiostream.write("CPUTime: " + str(cputime) + " seconds\n")
                        tempiostream.write("MaxRSS: " + str(maxrss) + " bytes\n")
                        tempiostream.write("MaxVMSize: " + str(maxvmsize) + " bytes\n")
                        tempiostream.write("BSONSize: " + str(bsonsize) + " bytes\n")
                        tempiostream.flush()
                    if kwargs['writelocal']:
                        outiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                    if kwargs['statslocal']:
                        outiolist[-1] += "CPUTime: " + str(cputime) + " seconds\n"
                        outiolist[-1] += "MaxRSS: " + str(maxrss) + " bytes\n"
                        outiolist[-1] += "MaxVMSize: " + str(maxvmsize) + " bytes\n"
                        outiolist[-1] += "BSONSize: " + str(bsonsize) + " bytes\n"
                    statsmark = {}
                    if kwargs['statsdb']:
                        statsmark.update({modname + "STATS": {"CPUTIME": cputime, "MAXRSS": maxrss, "MAXVMSIZE": maxvmsize, "BSONSIZE": bsonsize}})
                    if kwargs['markdone'] != "":
                        statsmark.update({modname + kwargs['markdone']: True})
                    # Testing
                    #statsmark.update({"HOST": os.environ['HOSTNAME'], "STEP": "job_" + filename.split("_job_")[1], "NBATCH": nbatch, "TIME": datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC"})
                    # Done testing
                    if len(statsmark) > 0:
                        if newcollection not in bulkcolls.keys():
                            #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                            if dbtype == "mongodb":
                                bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                        if newcollection not in bulkrequestslist[-1].keys():
                            bulkrequestslist[-1][newcollection] = []
                        #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": statsmark})
                        if dbtype == "mongodb":
                            bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": statsmark}, upsert = True)]
                    bsonsize = 0
                    countthisbatch += 1
                    countallbatches[-1] += 1
                    if countthisbatch == nbatch:
                        bulkrequestslist += [{}]
                        if kwargs['logging']:
                            logiolist += [""]
                        if kwargs['writelocal'] or kwargs['statslocal']:
                            outiolist += [""]
                        countthisbatch = 0
                        nbatch = randint(1, kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch']
                        countallbatches += [0]
                else:
                    if kwargs['templocal']:
                        tempiostream.write(line + "\n")
                        tempiostream.flush()
                    if kwargs['writelocal']:
                        outiolist[-1] += line + "\n"
                #if handler.nlocks() < kwargs['nworkers']:
                #    lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
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
                #    sys.stdout.write(tempiostream.getvalue())
                #    sys.stdout.flush()
                #    fcntl.flock(sys.stdout, fcntl.LOCK_UN)
                #    bulkdict = {}
                #    tempiostream = cStringIO.StringIO()
                #    countallbatches = 0
                #    os.remove(lockfile)

        #while not stderr_queue.empty():
        #    stderr_line = stderr_queue.get().rstrip("\n")
        #    if stderr_line not in ignoredstrings:
        #        if kwargs['controllername'] != None:
        #            exitcode = get_exitcode(kwargs['stepid'])
        #        with open(filename + ".err", "a") as errstream:
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

        if (len(bulkrequestslist) > 1) and (handler.nlocks() < kwargs['nworkers']):
            #if handler.nlocks() >= kwargs['nworkers']:
            #    overlocked = True
            #    os.kill(process.pid, signal.SIGSTOP)
            #    while handler.nlocks() >= kwargs['nworkers']:
            #        sleep(0.01)
            #else:
            #    overlocked = False
            lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
            with open(lockfile, 'w') as lockstream:
                lockstream.write("Writing " + str(countallbatches[0]) + " items.")
                lockstream.flush()
            del countallbatches[0]
            #print(bulkdict)
            #sys.stdout.flush()
            if dbtype == "mongodb":
                for coll, requests in bulkrequestslist[0].items():
                    try:
                        #bulkdict[bulkcoll].execute()
                        bulkcolls[coll].bulk_write(requests, ordered = False)
                    except BulkWriteError as bwe:
                        pprint(bwe.details)
                del bulkrequestslist[0]
            #while True:
            #    try:
            #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
            #        break
            #    except IOError:
            #        sleep(0.01)
            #tempiostream.close()
            #with open(workpath + "/" + stepname + ".temp", "r") as tempiostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
            #    for line in tempiostream:
            #        iostream.write(line)
            #        iostream.flush()
            #    os.remove(tempiostream.name)
            #sys.stdout.write(tempiostream.getvalue())
            #sys.stdout.flush()
            if kwargs['logging']:
                #print(len(logiolist[0].rstrip("\n").split("\n")))
                sys.stdout.flush()
                logiotime = ""
                for logio in logiolist[0].rstrip("\n").split("\n"):
                    logiotime += "Step " + step + ": " + datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + logio + "\n"
                sys.stdout.write(logiotime)
                sys.stdout.flush()
                del logiolist[0]
            if kwargs['templocal']:
                name = tempiostream.name
                tempiostream.close()
                os.remove(name)
                tempiostream = open(name, "w")
            if kwargs['writelocal'] or kwargs['statslocal']:
                outiostream.write(outiolist[0])
                outiostream.flush()
                del outiolist[0]
            #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
            #bulkdict = {}
            #tempiostream = open(workpath + "/" + stepname + ".temp", "w")
            #countallbatches = 0
            os.remove(lockfile)
            #if overlocked:
            #    os.kill(process.pid, signal.SIGCONT)

    sleep(kwargs['delay'])

while not stdout_queue.empty():
    while (not stdout_queue.empty()) and ((len(bulkrequestslist) <= 1 and countthisbatch < nbatch) or (handler.nlocks() >= kwargs['nworkers'])):
        line = stdout_queue.get().rstrip("\n")
        if line not in ignoredstrings:
            linehead = re.sub("^([- + &@].*?>|None).*", r"\1", line)
            linemarker = linehead[0]
            if linemarker == "-":
                newcollection, strindexdoc = linehead[1:-1].split(".")
                newindexdoc = json.loads(strindexdoc)
                linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
                doc = json.loads(linedoc)
                if dbtype == "mongodb":
                    bsonsize -= get_bsonsize(doc)
                if kwargs['templocal']:
                    tempiostream.write(line + "\n")
                    tempiostream.flush()
                if kwargs['writelocal']:
                    outiolist[-1] += line + "\n"
                if kwargs['writedb']:
                    if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        if dbtype == "mongodb":
                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in bulkrequestslist[-1].keys():
                        bulkrequestslist[-1][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).update({"$unset": doc})
                    if dbtype == "mongodb":
                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$unset": doc})]
            elif linemarker == "+":
                #tempiostream.write(linehead[1:-1].split(".") + "\n")
                #sys.stdout.flush()
                newcollection, strindexdoc = linehead[1:-1].split(".")
                newindexdoc = json.loads(strindexdoc)
                linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
                doc = json.loads(linedoc)
                if dbtype == "mongodb":
                    bsonsize += get_bsonsize(doc)
                if kwargs['templocal']:
                    tempiostream.write(line + "\n")
                    tempiostream.flush()
                if kwargs['writelocal']:
                    outiolist[-1] += line + "\n"
                if kwargs['writedb']:
                    if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        if dbtype == "mongodb":
                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in bulkrequestslist[-1].keys():
                        bulkrequestslist[-1][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": doc})
                    if dbtype == "mongodb":
                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": doc}, upsert = True)]
            elif linemarker == "&":
                newcollection, strindexdoc = linehead[1:-1].split(".")
                newindexdoc = json.loads(strindexdoc)
                linedoc = re.sub("^[-+&@].*?>", "", line).rstrip("\n")
                doc = json.loads(linedoc)
                if dbtype == "mongodb":
                    bsonsize += get_bsonsize(doc)
                if kwargs['templocal']:
                    tempiostream.write(line + "\n")
                    tempiostream.flush()
                if kwargs['writelocal']:
                    outiolist[-1] += line + "\n"
                if kwargs['writedb']:
                    if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        if dbtype == "mongodb":
                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in bulkrequestslist[-1].keys():
                        bulkrequestslist[-1][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet": doc})
                    if dbtype == "mongodb":
                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$addToSet": doc}, upsert = True)]
            elif linemarker == "@":
                if kwargs['statslocal'] or kwargs['statsdb']:
                    cputime = eval("%.2f" % handler.stat("TotalCPUTime"))
                    maxrss = handler.max_stat("Rss")
                    maxvmsize = handler.max_stat("Size")
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
                newcollection = basecoll
                doc = json.loads(temp_queue.get())
                newindexdoc = dict([(x, doc[x]) for x in kwargs['dbindexes']]);               
                if kwargs['logging']:
                    logiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + ">OUT\n"
                    sys.stdout.write("Step " + step + ": " + datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + ">TEMP\n")
                    sys.stdout.flush()
                if kwargs['templocal']:
                    tempiostream.write(line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n")
                    tempiostream.write("CPUTime: " + str(cputime) + " seconds\n")
                    tempiostream.write("MaxRSS: " + str(maxrss) + " bytes\n")
                    tempiostream.write("MaxVMSize: " + str(maxvmsize) + " bytes\n")
                    tempiostream.write("BSONSize: " + str(bsonsize) + " bytes\n")
                    tempiostream.flush()
                if kwargs['writelocal']:
                    outiolist[-1] += line + newcollection + "." + json.dumps(newindexdoc, separators = (',', ':')) + "\n"
                if kwargs['statslocal']:
                    outiolist[-1] += "CPUTime: " + str(cputime) + " seconds\n"
                    outiolist[-1] += "MaxRSS: " + str(maxrss) + " bytes\n"
                    outiolist[-1] += "MaxVMSize: " + str(maxvmsize) + " bytes\n"
                    outiolist[-1] += "BSONSize: " + str(bsonsize) + " bytes\n"
                statsmark = {}
                if kwargs['statsdb']:
                    statsmark.update({modname + "STATS": {"CPUTIME": cputime, "MAXRSS": maxrss, "MAXVMSIZE": maxvmsize, "BSONSIZE": bsonsize}})
                if kwargs['markdone'] != "":
                    statsmark.update({modname + kwargs['markdone']: True})
                # Testing
                #statsmark.update({"HOST": os.environ['HOSTNAME'], "STEP": "job_" + filename.split("_job_")[1], "NBATCH": nbatch, "TIME": datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC"})
                # Done testing
                if len(statsmark) > 0:
                    if newcollection not in bulkcolls.keys():
                        #bulkdict[newcollection] = db[newcollection].initialize_unordered_bulk_op()
                        if dbtype == "mongodb":
                            bulkcolls[newcollection] = db.get_collection(newcollection, write_concern = WriteConcern(w = "majority", fsync = True))
                    if newcollection not in bulkrequestslist[-1].keys():
                        bulkrequestslist[-1][newcollection] = []
                    #bulkdict[newcollection].find(newindexdoc).upsert().update({"$set": statsmark})
                    if dbtype == "mongodb":
                        bulkrequestslist[-1][newcollection] += [UpdateOne(newindexdoc, {"$set": statsmark}, upsert = True)]
                bsonsize = 0
                countthisbatch += 1
                countallbatches[-1] += 1
                if countthisbatch == nbatch:
                    bulkrequestslist += [{}]
                    if kwargs['logging']:
                        logiolist += [""]
                    if kwargs['writelocal'] or kwargs['statslocal']:
                        outiolist += [""]
                    countthisbatch = 0
                    nbatch = randint(1, kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch']
                    countallbatches += [0]
            else:
                if kwargs['templocal']:
                    tempiostream.write(line + "\n")
                    tempiostream.flush()
                if kwargs['writelocal']:
                    outiolist[-1] += line + "\n"
            #if handler.nlocks() < kwargs['nworkers']:
            #    lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
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
            #    sys.stdout.write(tempiostream.getvalue())
            #    sys.stdout.flush()
            #    fcntl.flock(sys.stdout, fcntl.LOCK_UN)
            #    bulkdict = {}
            #    tempiostream = cStringIO.StringIO()
            #    countallbatches = 0
            #    os.remove(lockfile)

    #while not stderr_queue.empty():
    #    stderr_line = stderr_queue.get().rstrip("\n")
    #    if stderr_line not in ignoredstrings:
    #        if kwargs['controllername'] != None:
    #            exitcode = get_exitcode(kwargs['stepid'])
    #        with open(filename + ".err", "a") as errstream:
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

    if (len(bulkrequestslist) > 1) and (handler.nlocks() < kwargs['nworkers']):
        #if handler.nlocks() >= kwargs['nworkers']:
        #    overlocked = True
        #    os.kill(process.pid, signal.SIGSTOP)
        #    while handler.nlocks() >= kwargs['nworkers']:
        #        sleep(0.01)
        #else:
        #    overlocked = False
        lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
        with open(lockfile, 'w') as lockstream:
            lockstream.write("Writing " + str(countallbatches[0]) + " items.")
            lockstream.flush()
        del countallbatches[0]
        #print(bulkdict)
        #sys.stdout.flush()
        if dbtype == "mongodb":
            for coll, requests in bulkrequestslist[0].items():
                try:
                    #bulkdict[bulkcoll].execute()
                    bulkcolls[coll].bulk_write(requests, ordered = False)
                except BulkWriteError as bwe:
                    pprint(bwe.details)
            del bulkrequestslist[0]
        #while True:
        #    try:
        #        fcntl.flock(sys.stdout, fcntl.LOCK_EX | fcntl.LOCK_NB)
        #        break
        #    except IOError:
        #        sleep(0.01)
        #tempiostream.close()
        #with open(workpath + "/" + stepname + ".temp", "r") as tempiostream, open(workpath + "/" + stepname + ".out", "a") as iostream:
        #    for line in tempiostream:
        #        iostream.write(line)
        #        iostream.flush()
        #    os.remove(tempiostream.name)
        #sys.stdout.write(tempiostream.getvalue())
        #sys.stdout.flush()
        if kwargs['logging']:
            #print(len(logiolist[0].rstrip("\n").split("\n")))
            sys.stdout.flush()
            logiotime = ""
            for logio in logiolist[0].rstrip("\n").split("\n"):
                logiotime += "Step " + step + ": " + datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + logio + "\n"
            sys.stdout.write(logiotime)
            sys.stdout.flush()
            del logiolist[0]
        if kwargs['templocal']:
            name = tempiostream.name
            tempiostream.close()
            os.remove(name)
            tempiostream = open(name, "w")
        if kwargs['writelocal'] or kwargs['statslocal']:
            outiostream.write(outiolist[0])
            outiostream.flush()
            del outiolist[0]
        #fcntl.flock(sys.stdout, fcntl.LOCK_UN)
        #bulkdict = {}
        #tempiostream = open(workpath + "/" + stepname + ".temp", "w")
        #countallbatches = 0
        os.remove(lockfile)
        #if overlocked:
        #    os.kill(process.pid, signal.SIGCONT)

while len(bulkrequestslist) > 0:
    while handler.nlocks() >= kwargs['nworkers']:
        sleep(kwargs['delay'])

    if (len(bulkrequestslist) > 0) and (handler.nlocks() < kwargs['nworkers']):
        lockfile = workpath + "/" + kwargs['stepid'] + ".lock"
        with open(lockfile, 'w') as lockstream:
            lockstream.write("Writing " + str(countallbatches[0]) + " items.")
            lockstream.flush()
        del countallbatches[0]

        if dbtype == "mongodb":
            for coll, requests in bulkrequestslist[0].items():
                try:
                    #bulkdict[bulkcoll].execute()
                    bulkcolls[coll].bulk_write(requests, ordered = False)
                except BulkWriteError as bwe:
                    pprint(bwe.details)
            del bulkrequestslist[0]

        if kwargs['logging']:
            #print(len(logiolist[0].rstrip("\n").split("\n")))
            sys.stdout.flush()
            logiotime = ""
            for logio in logiolist[0].rstrip("\n").split("\n"):
                logiotime += "Step " + step + ": " + datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S") + " UTC: " + logio + "\n"
            sys.stdout.write(logiotime)
            sys.stdout.flush()
            del logiolist[0]
        if kwargs['templocal']:
            name = tempiostream.name
            tempiostream.close()
            os.remove(name)
            tempiostream = open(name, "w")
        if kwargs['writelocal'] or kwargs['statslocal']:
            outiostream.write(outiolist[0])
            outiostream.flush()
            del outiolist[0]

        os.remove(lockfile)

if kwargs['input_file'] != None:
    stdin_iter_file.close()

if kwargs['templocal']:
    if not tempiostream.closed:
        tempiostream.close()
    if os.path.exists(tempiostream.name):
        os.remove(tempiostream.name)
if kwargs['writelocal'] or kwargs['statslocal']:
    outiostream.close()

handler.join()
#stderr_reader.join()
#if kwargs['statslocal'] or kwargs['statsdb']:
#    stats_reader.join()

process.stdin.close()
process.stdout.close()
process.stderr.close()

if dbtype == "mongodb":
    dbclient.close()