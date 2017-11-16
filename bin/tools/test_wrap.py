#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,os,cStringIO,json,tempfile;
from time import time,sleep;
from subprocess import PIPE,Popen;
from threading  import Thread;
from argparse import ArgumentParser,REMAINDER;
try:
    from Queue import Queue,Empty;
except ImportError:
    from queue import Queue,Empty;  # python 3.x

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

class AsynchronousThreadStreamReaderWriter(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, in_stream, out_stream, in_iter_arg, in_iter_file, in_queue, out_queue, delimiter = ''):
        assert hasattr(in_iter_arg, '__iter__')
        assert hasattr(in_iter_file, '__iter__')
        assert isinstance(in_queue, Queue)
        assert isinstance(out_queue, Queue)
        assert callable(in_stream.write)
        assert callable(out_stream.readline)
        Thread.__init__(self)
        self._instream = in_stream
        self._outstream = out_stream
        self._initerarg = in_iter_arg
        self._initerfile = in_iter_file
        self._initerargflag = False
        self._initerfileflag = False
        self._inqueue = in_queue
        self._outqueue = out_queue
        self._delimiter = delimiter
        self.daemon = True

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        try:
            in_line = self._initerarg.next()+self._delimiter
        except StopIteration:
            self._initerargflag = True
            in_line = self._initerfile.readline()
            if in_line == "":
                self._initerfileflag = True
                in_line = self._inqueue.get()
                if in_line == "":
                    self._instream.close()
                else:
                    in_line += self._delimiter
                    self._instream.write(in_line)
                    self._instream.flush()
            else:
                in_line = in_line.rstrip("\n")+self._delimiter
                self._instream.write(in_line)
                self._instream.flush()
                with tempfile.NamedTemporaryFile(dir="/".join(self._initerfile.name.split("/")[:-1]), delete=False) as tempstream:
                    in_line = self._initerfile.readline()
                    while in_line != "":
                        tempstream.write(in_line)
                        tempstream.flush()
                        in_line = self._initerfile.readline()
                    name = self._initerfile.name
                    self._initerfile.close()
                    os.rename(tempstream.name, name)
                    self._initerfile = open(name, 'r')
        else:
            self._instream.write(in_line)
            self._instream.flush()
            #print(in_line);
            #sys.stdout.flush();
        for out_line in iter(self._outstream.readline, ''):
            if out_line == "\n".decode('string_escape'):
                try:
                    in_line = self._initerarg.next()+self._delimiter
                except StopIteration:
                    self._initerargflag = True
                    in_line = self._initerfile.readline()
                    if in_line == "":
                        self._initerfileflag = True
                        in_line = self._inqueue.get()
                        if in_line == "":
                            self._instream.close()
                        else:
                            in_line += self._delimiter
                            self._instream.write(in_line)
                            self._instream.flush()
                    else:
                        in_line = in_line.rstrip("\n")+self._delimiter
                        self._instream.write(in_line)
                        self._instream.flush()
                        with tempfile.NamedTemporaryFile(dir="/".join(self._initerfile.name.split("/")[:-1]), delete=False) as tempstream:
                            in_line = self._initerfile.readline()
                            while in_line != "":
                                tempstream.write(in_line)
                                tempstream.flush()
                                in_line = self._initerfile.readline()
                            name = self._initerfile.name
                            self._initerfile.close()
                            os.rename(tempstream.name, name)
                            self._initerfile = open(name, 'r')
                else:
                    self._instream.write(in_line)
                    self._instream.flush()
            self._outqueue.put(out_line.rstrip("\n"))

    def waiting(self):
        return self.is_alive() and self._initerargflag and self._initerfileflag and self._inqueue.empty() and self._outqueue.empty()

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return (not self.is_alive()) and self._initerargflag and self._initerfileflag and self._inqueue.empty() and self._outqueue.empty()

class AsynchronousThreadStatsReader(Thread):
    '''Class to implement asynchronously read output of
    a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''
    def __init__(self, pid, stats, stats_delay = 0):
        Thread.__init__(self)
        self._pid = str(pid)
        self._smaps = open("/proc/"+str(pid)+"/smaps","r")
        self._stat = open("/proc/"+str(pid)+"/stat","r")
        self._uptime = open("/proc/uptime","r")
        self._stats = dict((s,0) for s in stats)
        self._maxstats = dict((s,0) for s in stats)
        self._totstats = dict((s,0) for s in stats)
        self._nstats = 0
        self._statsdelay = stats_delay
        self.daemon = True

    def is_inprog(self):
        '''Check whether there is no more content to expect.'''
        return self.is_alive() and os.path.exists("/proc/"+self._pid+"/smaps")

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        lower_keys = [k.lower() for k in self._stats.keys()]
        time_keys = ["elapsedtime","totalcputime","parentcputime","childcputime","parentcpuusage","childcpuusage","totalcpuusage"]
        if any([k in lower_keys for k in time_keys]):
            hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        while self.is_inprog():
            #for stat_name in self._stats.keys():
            #    self._stats[stat_name] = 0
            if any([k in lower_keys for k in time_keys]):
                self._stat.seek(0);
                stat_line = self._stat.read() if self.is_inprog() else ""
                #print(stat_line);
                #sys.stdout.flush();
                if stat_line != "":
                    stat_line_split = stat_line.split()
                    utime, stime, cutime, cstime = [(float(f)/hz) for f in stat_line_split[13:17]]
                    starttime = float(stat_line_split[21])/hz
                    parent_cputime = utime+stime
                    child_cputime = cutime+cstime
                    total_cputime = parent_cputime+child_cputime
                    self._uptime.seek(0);
                    uptime_line = self._uptime.read()
                    uptime = float(uptime_line.split()[0])
                    elapsedtime = uptime-starttime
                    parent_cpuusage = 100*parent_cputime/elapsedtime if elapsedtime > 0 else 0
                    child_cpuusage = 100*child_cputime/elapsedtime if elapsedtime > 0 else 0
                    total_cpuusage = 100*total_cputime/elapsedtime if elapsedtime > 0 else 0
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
            self._smaps.seek(0);
            smaps_lines = self._smaps.readlines() if self.is_inprog() and os.path.exists("/proc/"+self._pid) else ""
            for smaps_line in smaps_lines:
                smaps_line_split = smaps_line.split()
                if len(smaps_line_split) == 3:
                    stat_name, stat_size, stat_unit = smaps_line_split
                    stat_name = stat_name.rstrip(':')
                    if stat_name.lower() in lower_keys:
                        stat_size = int(stat_size)
                        if stat_unit.lower() == 'b':
                            multiplier = 1
                        elif stat_unit.lower() in ['k','kb']:
                            multiplier = 1024
                        elif stat_unit.lower() in ['m','mb']:
                            multiplier = 1000*1024
                        elif stat_unit.lower() in ['g','gb']:
                            multiplier = 1000*1000*1024
                        else:
                            raise Exception(stat_name+" in "+self._stream.name+" has unrecognized unit: "+stat_unit)
                        self._stats[stat_name] += multiplier*stat_size
                #smaps_line = self._smaps.readline() if self.is_alive() else ""
            self._nstats += 1
            for k in self._stats.keys():
                self._totstats[k] += self._stats[k]
                if self._stats[k] >= self._maxstats[k]:
                    self._maxstats[k] = self._stats[k]
            sleep(self._statsdelay)
        self._smaps.close()
        self._stat.close()
        self._uptime.close()

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
        return self._totstats[stat_name]/self._nstats if self._nstats > 0 else 0

    def avg_stats(self):
        '''Check whether there is no more content to expect.'''
        return dict((k,self._totstats[k]/self._nstats if self._nstats > 0 else 0) for k in self._totstats.keys())

parser=ArgumentParser();
parser.add_argument('--delay',dest='delay',action='store',default=0,help='');
parser.add_argument('--stats','-S',dest='stats_list',nargs='+',action='store',default=[],help='');
parser.add_argument('--stats-delay',dest='stats_delay',action='store',default=0,help='');
parser.add_argument('--delimiter','-d',dest='delimiter',action='store',default='',help='');
parser.add_argument('--input','-i',dest='input_list',nargs='+',action='store',default=[],help='');
parser.add_argument('--file','-f',dest='input_file',action='store',default=None,help='');
parser.add_argument('--interactive',dest='interactive',action='store_true',default=False,help='');
parser.add_argument('--script','-s', dest='scriptcommand',nargs=REMAINDER,required=True,help='');

kwargs=vars(parser.parse_known_args()[0]);

kwargs['delay']=float(kwargs['delay']);
kwargs['stats_delay']=float(kwargs['stats_delay']);
kwargs['delimiter']=kwargs['delimiter'].decode("string_escape");
kwargs['scriptcommand']=" ".join(kwargs['scriptcommand']);

mainpath=os.getcwd();
workpath=mainpath;

if kwargs['input_list']!=None:
    stdin_iter_arg=iter(kwargs['input_list']);
else:
    stdin_iter_arg=iter([]);

if kwargs['input_file']!=None:
    if "/" not in kwargs['input_file']:
        kwargs['input_file']=workpath+"/"+kwargs['input_file'];
    filename=kwargs['input_file'];
    stdin_iter_file=open(filename,"r");
else:
    stdin_iter_file=cStringIO.StringIO();

#print(kwargs);
#sys.stdout.flush();

process=Popen(kwargs['scriptcommand'],shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE,bufsize=1);

stdin_queue=Queue();
if not kwargs['interactive']:
    stdin_queue.put("");
#for x in ["a","b","c","d"]:
#    stdin_queue.put(x+kwargs['delimiter']);

stdout_queue=Queue();
stdout_reader=AsynchronousThreadStreamReaderWriter(process.stdin,process.stdout,stdin_iter_arg,stdin_iter_file,stdin_queue,stdout_queue,delimiter=kwargs['delimiter']);
stdout_reader.start();

stderr_queue=Queue();
stderr_reader=AsynchronousThreadStreamReader(process.stderr,stderr_queue);
stderr_reader.start();

#stats_reader=AsynchronousThreadStatsReader(process.pid,['Size','Rss','Pss','Swap','ElapsedTime','TotalCPUTime'],sleep=0.1);
stats_reader=AsynchronousThreadStatsReader(process.pid,kwargs['stats_list'],stats_delay=kwargs['stats_delay']);
stats_reader.start();

while process.poll()==None and stats_reader.is_inprog() and not (stdout_reader.eof() or stderr_reader.eof()):
    if stdout_reader.waiting():
        stdin_line=sys.stdin.readline().rstrip("\n");
        #print("line: "+line);
        #sys.stdout.flush();
        stdin_queue.put(stdin_line);

    while not stdout_queue.empty():
        line=stdout_queue.get();
        if line!="":
            sys.stdout.write(line+"\n");
        else:
            sys.stdout.write("\n");
            sys.stdout.write("Stats: "+json.dumps(stats_reader.stats(),separators=(',',':')));
            sys.stdout.write("\n");
            sys.stdout.write("Max Stats: "+json.dumps(stats_reader.max_stats(),separators=(',',':')));
            sys.stdout.write("\n");
            sys.stdout.write("Average Stats: "+json.dumps(stats_reader.avg_stats(),separators=(',',':')));
            sys.stdout.write("\n\n");

    while not stderr_queue.empty():
        line=stderr_queue.get();
        sys.stderr.write(line+"\n");

    #sys.stdout.write("\n");
    sys.stdout.flush();

    sleep(kwargs['delay'])

while not stdout_queue.empty():
    line=stdout_queue.get();
    if line!="":
        sys.stdout.write(line+"\n");
    else:
        sys.stdout.write("\n");
        sys.stdout.write("Stats: "+json.dumps(stats_reader.stats(),separators=(',',':')));
        sys.stdout.write("\n");
        sys.stdout.write("Max Stats: "+json.dumps(stats_reader.max_stats(),separators=(',',':')));
        sys.stdout.write("\n");
        sys.stdout.write("Average Stats: "+json.dumps(stats_reader.avg_stats(),separators=(',',':')));
        sys.stdout.write("\n\n");

while not stderr_queue.empty():
    line=stderr_queue.get();
    sys.stderr.write(line+"\n");

sys.stdout.flush();

stdout_reader.join();
stderr_reader.join();
stats_reader.join();

stdin_iter_file.close();
process.stdin.close();
process.stdout.close();
process.stderr.close();