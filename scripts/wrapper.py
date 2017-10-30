#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,cStringIO,os,glob,signal,fcntl,json,mongolink,re,tempfile;#,linecache,traceback
from pymongo.errors import BulkWriteError;
from time import time,sleep;
from random import randint;
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
    def __init__(self, in_stream, out_stream, in_iter_arg, in_iter_file, in_queue, temp_queue, out_queue, delimiter = '', cleanup = None, time_limit = None, start_time = None):
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
        #self._initerfileflag = False
        self._inqueue = in_queue
        self._tempqueue = temp_queue
        self._outqueue = out_queue
        self._delimiter = delimiter
        self._cleanup = cleanup
        self._counter = 0
        self._timelimit = time_limit
        self._starttime = start_time
        self.daemon = True

    def run(self):
        '''The body of the thread: read lines and put them on the queue.'''
        try:
            in_line = self._initerarg.next()
        except StopIteration:
            self._initerargflag = True
            if self._initerfile.closed:
                in_line = self._inqueue.get()
                if in_line == "":
                    self._instream.close()
                    #print("finally!");
                    #sys.stdout.flush();
                else:
                    self._tempqueue.put(in_line)
                    self._instream.write(in_line+self._delimiter)
                    #print("a: "+in_line+self._delimiter);
                    #sys.stdout.flush();
                    self._instream.flush()
            else:
                in_line = self._initerfile.readline().rstrip("\n")
                if in_line == "":
                    self._initerfile.close()
                    #self._initerfileflag = True
                    in_line = self._inqueue.get()
                    if in_line == "":
                        self._instream.close()
                        #print("finally!");
                        #sys.stdout.flush();
                    else:
                        self._tempqueue.put(in_line)
                        self._instream.write(in_line+self._delimiter)
                        #print("a: "+in_line+self._delimiter);
                        #sys.stdout.flush();
                        self._instream.flush()
                else:
                    self._tempqueue.put(in_line)
                    self._instream.write(in_line+self._delimiter)
                    #print("a: "+in_line+self._delimiter);
                    #sys.stdout.flush();
                    self._instream.flush()
                    if (self._cleanup != None and self._counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time()-self._starttime >= self._timelimit):
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
                        self._counter = 0
        else:
            self._tempqueue.put(in_line)
            self._instream.write(in_line+self._delimiter)
            #print("a: "+in_line+self._delimiter);
            #sys.stdout.flush();
            self._instream.flush()
            #self._instream.close()
            #print(in_line);
            #sys.stdout.flush();
        for out_line in iter(self._outstream.readline, ''):
            out_line = out_line.rstrip("\n")
            #if out_line == "\n".decode('string_escape'):
            #print(out_line);
            #sys.stdout.flush();
            if out_line == "@":
                try:
                    in_line = self._initerarg.next()
                except StopIteration:
                    self._initerargflag = True
                    if self._initerfile.closed:
                        in_line = self._inqueue.get()
                        if in_line == "":
                            self._instream.close()
                            #print("finally!");
                            #sys.stdout.flush();
                        else:
                            self._tempqueue.put(in_line)
                            self._instream.write(in_line+self._delimiter)
                            #print("a: "+in_line+self._delimiter);
                            #sys.stdout.flush();
                            self._instream.flush()
                    else:
                        in_line = self._initerfile.readline().rstrip("\n")
                        if in_line == "":
                            self._initerfile.close()
                            #self._initerfileflag = True
                            in_line = self._inqueue.get()
                            if in_line == "":
                                self._instream.close()
                                #print("finally!");
                                #sys.stdout.flush();
                            else:
                                self._tempqueue.put(in_line)
                                self._instream.write(in_line+self._delimiter)
                                #print("a: "+in_line+self._delimiter);
                                #sys.stdout.flush();
                                self._instream.flush()
                        else:
                            self._tempqueue.put(in_line)
                            self._instream.write(in_line+self._delimiter)
                            #print("a: "+in_line+self._delimiter);
                            #sys.stdout.flush();
                            self._instream.flush()
                            if (self._cleanup != None and self._counter >= self._cleanup) or (self._timelimit != None and self._starttime != None and time()-self._starttime >= self._timelimit):
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
                                self._counter = 0
                    if self._cleanup != None:
                        self._counter += 1
                else:
                    self._tempqueue.put(in_line)
                    self._instream.write(in_line+self._delimiter)
                    #print("a: "+in_line+self._delimiter);
                    #sys.stdout.flush();
                    self._instream.flush()
            self._outqueue.put(out_line.rstrip("\n"))

    def waiting(self):
        return self.is_alive() and self._initerargflag and self._initerfile.closed and self._inqueue.empty() and self._outqueue.empty()

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return (not self.is_alive()) and self._initerargflag and self._initerfile.closed and self._inqueue.empty() and self._outqueue.empty()

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

def timestamp2unit(timestamp,unit="seconds"):
    if timestamp=="infinite":
        return timestamp;
    else:
        days=0;
        if "-" in timestamp:
            daysstr,timestamp=timestamp.split("-");
            days=int(daysstr);
        hours,minutes,seconds=[int(x) for x in timestamp.split(":")];
        hours+=days*24;
        minutes+=hours*60;
        seconds+=minutes*60;
        if unit=="seconds":
            return seconds;
        elif unit=="minutes":
            return float(seconds)/60.;
        elif unit=="hours":
            return float(seconds)/(60.*60.);
        elif unit=="days":
            return float(seconds)/(60.*60.*24.);
        else:
            return 0;

parser=ArgumentParser();

parser.add_argument('--controller',dest='controllername',action='store',default=None,help='');
parser.add_argument('--stepid',dest='stepid',action='store',default="1",help='');

parser.add_argument('--nbatch','-n',dest='nbatch',action='store',default=1,help='');
parser.add_argument('--nworkers','-N',dest='nworkers',action='store',default=1,help='');
parser.add_argument('--random-nbatch',dest='random_nbatch',action='store_true',default=False,help='');

parser.add_argument('--dbtype',dest='dbtype',action='store',default=None,help='');
parser.add_argument('--dbhost',dest='dbhost',action='store',default=None,help='');
parser.add_argument('--dbport',dest='dbport',action='store',default=None,help='');
parser.add_argument('--dbusername',dest='dbusername',action='store',default=None,help='');
parser.add_argument('--dbpassword',dest='dbpassword',action='store',default=None,help='');
parser.add_argument('--dbname',dest='dbname',action='store',default=None,help='');

parser.add_argument('--logging',dest='logging',action='store_true',default=False,help='');
parser.add_argument('--temp-local','-t',dest='templocal',action='store_true',default=False,help='');
parser.add_argument('--write-local','-w',dest='writelocal',action='store_true',default=False,help='');
parser.add_argument('--write-db','-W',dest='writedb',action='store_true',default=False,help='');
parser.add_argument('--stats-local','-s',dest='statslocal',action='store_true',default=False,help='');
parser.add_argument('--stats-db','-S',dest='statsdb',action='store_true',default=False,help='');
parser.add_argument('--basecoll',dest='basecoll',action='store',default=None,help='');
parser.add_argument('--dbindexes',dest='dbindexes',nargs='+',default=None,help='');
parser.add_argument('--markdone',dest='markdone',action='store',default="MARK",help='');

parser.add_argument('--delay',dest='delay',action='store',default=0,help='');
parser.add_argument('--stats',dest='stats_list',nargs='+',action='store',default=[],help='');
parser.add_argument('--stats-delay',dest='stats_delay',action='store',default=0,help='');
parser.add_argument('--delimiter','-d',dest='delimiter',action='store',default='\n',help='');
parser.add_argument('--input','-i',dest='input_list',nargs='+',action='store',default=[],help='');
parser.add_argument('--file','-f',dest='input_file',action='store',default=None,help='');
parser.add_argument('--cleanup-after',dest='cleanup',action='store',default=None,help='');
parser.add_argument('--interactive',dest='interactive',action='store_true',default=False,help='');
parser.add_argument('--time-limit',dest='time_limit',action='store',default=None,help='');
parser.add_argument('--script','-c', dest='scriptcommand',nargs=REMAINDER,required=True,help='');

kwargs=vars(parser.parse_known_args()[0]);

#print(kwargs['input_list']);
#sys.stdout.flush();

if kwargs['time_limit']==None:
    start_time=None;
else:
    start_time=time();
    kwargs['time_limit']=timestamp2unit(kwargs['time_limit']);

kwargs['delay']=float(kwargs['delay']);
kwargs['stats_delay']=float(kwargs['stats_delay']);
kwargs['nbatch']=int(kwargs['nbatch']);
kwargs['nworkers']=int(kwargs['nworkers']);
#kwargs['delimiter']=kwargs['delimiter']#.decode("string_escape");
if kwargs['cleanup']=="":
    kwargs['cleanup']=None;
else:
    kwargs['cleanup']=eval(kwargs['cleanup']);

modname=kwargs['scriptcommand'][1].split("/")[-1].split(".")[0];

kwargs['scriptcommand']=" ".join(kwargs['scriptcommand']);

if kwargs['controllername']==None:
    mainpath=os.getcwd();
    workpath=mainpath;
    dbhost=kwargs['dbhost'];
    dbport=kwargs['dbport'];
    dbusername=kwargs['dbusername'];
    dbpassword=kwargs['dbpassword'];
    dbname=kwargs['dbname'];
    basecoll=kwargs['basecoll'];
else:
    mainpath=os.environ['SLURMONGO_ROOT'];
    #softwarefile=mainpath+"/state/software";
    #with open(softwarefile,"r") as softwarestream:
    #    softwarestream.readline();
    #    for line in softwarestream:
    #        ext=line.split(',')[2];
    #        if ext+" " in kwargs['scriptcommand']:
    #            break;
    #modulesfile=mainpath+"/state/modules";
    #with open(modulesfile,"r") as modulesstream:
    #    modulesstream.readline();
    #    for line in modulesstream:
    #        modname=line.rstrip("\n");
    #        if " "+modname+ext+" " in kwargs['scriptcommand'] or "/"+modname+ext+" " in kwargs['scriptcommand']:
    #            break;
    controllerpath=mainpath+"/modules/"+modname+"/"+kwargs['controllername'];
    workpath=controllerpath+"/jobs";
    controllerfile=controllerpath+"/controller_"+modname+"_"+kwargs['controllername']+".job";
    dburifile=mainpath+"/state/mongouri";
    with open(controllerfile,"r") as controllerstream:
        for controllerline in controllerstream:
            if "dbtype=" in controllerline:
                dbtype=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if dbtype=="":
                    dbtype=None;
            elif "dbhost=" in controllerline:
                dbhost=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if dbhost=="":
                    dbhost=None;
            elif "dbport=" in controllerline:
                dbport=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if dbport=="":
                    dbport=None;
            elif "dbusername=" in controllerline:
                dbusername=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if dbusername=="":
                    dbusername=None;
            elif "dbpassword=" in controllerline:
                dbpassword=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if dbpassword=="":
                    dbpassword=None;
            elif "dbname=" in controllerline:
                dbname=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if dbname=="":
                    dbname=None;
            elif "basecollection=" in controllerline:
                basecoll=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                if basecoll=="":
                    basecoll=None;

if len(kwargs['input_list'])>0:
    stdin_iter_arg=iter(kwargs['input_list']);
else:
    stdin_iter_arg=iter([]);

if kwargs['input_file']!=None:
    if "/" not in kwargs['input_file']:
        kwargs['input_file']=workpath+"/"+kwargs['input_file'];
    stdin_iter_file=open(kwargs['input_file'],"r");
else:
    stdin_iter_file=cStringIO.StringIO();

if kwargs['input_file']!=None:
    filename=".".join(kwargs['input_file'].split('.')[:-1]);
else:
    filename=workpath+"/"+kwargs['stepid'];

if kwargs['writelocal'] or kwargs['statslocal']:
    outiostream=open(filename+".out","w");
    if kwargs['templocal']:
        tempiostream=open(filename+".temp","w");
    if (kwargs['dbindexes']==None) or ((kwargs['controllername']==None) and (basecoll==None)):
        parser.error("Both --write-local and --stats-local require either the options:\n--controllername --dbindexes,\nor the options: --basecoll --dbindexes")

if any([kwargs[x] for x in ['writedb','statsdb']]):
    if (kwargs['dbindexes']==None) or ((kwargs['controllername']==None) and any([x==None for x in [dbtype,dbhost,dbport,dbusername,dbpassword,dbname,basecoll]])):
        parser.error("Both --writedb and --statsdb require either the options:\n--controllername --dbindexes,\nor the options:\n--dbtype --dbhost --dbport --dbusername --dbpassword --dbname --basecoll --dbindexes");
    else:
        if dbtype=="mongodb":
            if dbusername==None:
                dbclient=mongolink.MongoClient("mongodb://"+dbhost+":"+dbport+"/"+dbname);
            else:
                dbclient=mongolink.MongoClient("mongodb://"+dbusername+":"+dbpassword+"@"+dbhost+":"+dbport+"/"+dbname+"?authMechanism=SCRAM-SHA-1");
        else:
            raise Exception("Only mongodb is currently supported.");

        db=dbclient[dbname];

if kwargs['logging']:
    logiostream=cStringIO.StringIO();

process=Popen(kwargs['scriptcommand'],shell=True,stdin=PIPE,stdout=PIPE,stderr=PIPE,bufsize=1);

stdin_queue=Queue();
if not kwargs['interactive']:
    stdin_queue.put("");

temp_queue=Queue();

stdout_queue=Queue();
stdout_reader=AsynchronousThreadStreamReaderWriter(process.stdin,process.stdout,stdin_iter_arg,stdin_iter_file,stdin_queue,temp_queue,stdout_queue,delimiter=kwargs['delimiter'],cleanup=kwargs['cleanup'],time_limit=kwargs['time_limit'],start_time=start_time);
stdout_reader.start();

stderr_queue=Queue();
stderr_reader=AsynchronousThreadStreamReader(process.stderr,stderr_queue);
stderr_reader.start();

if kwargs['statslocal'] or kwargs['statsdb']:
    stats_reader=AsynchronousThreadStatsReader(process.pid,kwargs['stats_list'],stats_delay=kwargs['stats_delay']);
    stats_reader.start();

bulkdict={};
#tempiostream=open(workpath+"/"+stepname+".temp","w");
iostream=cStringIO.StringIO();
bsonsize=0;
countresult=0;
nbatch=randint(1,kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch'];
while process.poll()==None and stats_reader.is_inprog() and not (stdout_reader.eof() or stderr_reader.eof()):
    if stdout_reader.waiting():
        stdin_line=sys.stdin.readline().rstrip("\n");
        stdin_queue.put(stdin_line);

    while not stdout_queue.empty():
        while (not stdout_queue.empty()) and ((countresult<nbatch) or (len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers'])):
            line=stdout_queue.get().rstrip("\n");
            linehead=re.sub("^([-+&@].*?>|None).*",r"\1",line);
            linemarker=linehead[0];
            if linemarker=="-":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                bsonsize-=mongolink.bsonsize(doc);
                if kwargs['templocal']:
                    tempiostream.write(line+"\n");
                    tempiostream.flush();
                if kwargs['writelocal']:
                    iostream.write(line+"\n");
                    iostream.flush();
                if kwargs['writedb']:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).update({"$unset":doc});
            elif linemarker=="+":
                #tempiostream.write(linehead[1:-1].split(".")+"\n");
                #sys.stdout.flush();
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                bsonsize+=mongolink.bsonsize(doc);
                if kwargs['templocal']:
                    tempiostream.write(line+"\n");
                    tempiostream.flush();
                if kwargs['writelocal']:
                    iostream.write(line+"\n");
                    iostream.flush();
                if kwargs['writedb']:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":doc});
            elif linemarker=="&":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                bsonsize+=mongolink.bsonsize(doc);
                if kwargs['templocal']:
                    tempiostream.write(line+"\n");
                    tempiostream.flush();
                if kwargs['writelocal']:
                    iostream.write(line+"\n");
                    iostream.flush();
                if kwargs['writedb']:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet":doc});
            elif linemarker=="@":
                if kwargs['statslocal'] or kwargs['statsdb']:
                    cputime="%.2f" % stats_reader.stat("TotalCPUTime");
                    maxrss=stats_reader.max_stat("Rss");
                    maxvmsize=stats_reader.max_stat("Size");
                #    stats=getstats("sstat",["MaxRSS","MaxVMSize"],kwargs['stepid']);
                #    if (len(stats)==1) and (stats[0]==""):
                #        newtotcputime,maxrss,maxvmsize=[eval(x) for x in getstats("sacct",["CPUTimeRAW","MaxRSS","MaxVMSize"],kwargs['stepid'])];
                #    else:
                #        newtotcputime=eval(getstats("sacct",["CPUTimeRAW"],kwargs['stepid'])[0]);
                #        maxrss,maxvmsize=stats;
                #    cputime=newtotcputime-totcputime;
                #    totcputime=newtotcputime;
                #newcollection,strindexdoc=linehead[1:].split("<")[0].split(".");
                #newindexdoc=json.loads(strindexdoc);
                newcollection=basecoll;
                doc=json.loads(temp_queue.get());
                newindexdoc=dict([(x,doc[x]) for x in kwargs['dbindexes']]);               
                if kwargs['logging']:
                    logiostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<OUT\n");
                    logiostream.flush();
                    sys.stdout.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<TEMP\n");
                    sys.stdout.flush();
                if kwargs['templocal']:
                    tempiostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n");
                    tempiostream.write("CPUTime: "+str(cputime)+" seconds\n");
                    tempiostream.write("MaxRSS: "+str(maxrss)+" bytes\n");
                    tempiostream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
                    tempiostream.write("BSONSize: "+str(bsonsize)+" bytes\n");
                    tempiostream.flush();
                if kwargs['writelocal']:
                    iostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n");
                    iostream.flush();
                if kwargs['statslocal']:
                    iostream.write("CPUTime: "+str(cputime)+" seconds\n");
                    iostream.write("MaxRSS: "+str(maxrss)+" bytes\n");
                    iostream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
                    iostream.write("BSONSize: "+str(bsonsize)+" bytes\n");
                    iostream.flush();
                statsmark={};
                if kwargs['statsdb']:
                    statsmark.update({modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}});
                if kwargs['markdone']!="":
                    statsmark.update({modname+kwargs['markdone']:True});
                if len(statsmark)>0:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":statsmark});
                bsonsize=0;
            else:
                if kwargs['templocal']:
                    tempiostream.write(line+"\n");
                    tempiostream.flush();
                if kwargs['writelocal']:
                    iostream.write(line+"\n");
                    iostream.flush();
            countresult+=1;
            #if len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']:
            #    lockfile=workpath+"/"+kwargs['stepid']+".lock";
            #    with open(lockfile,'w') as lockstream:
            #        lockstream.write(str(countresult));
            #        lockstream.flush();
            #    for bulkcoll in bulkdict.keys():
            #        try:
            #            bulkdict[bulkcoll].execute();
            #        except BulkWriteError as bwe:
            #            pprint(bwe.details);
            #    while True:
            #        try:
            #            fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
            #            break;
            #        except IOError:
            #            sleep(0.01);
            #    sys.stdout.write(tempiostream.getvalue());
            #    sys.stdout.flush();
            #    fcntl.flock(sys.stdout,fcntl.LOCK_UN);
            #    bulkdict={};
            #    tempiostream=cStringIO.StringIO();
            #    countresult=0;
            #    os.remove(lockfile);

        if (countresult>=nbatch) and (len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']):
            #if len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
            #    overlocked=True;
            #    os.kill(process.pid,signal.SIGSTOP);
            #    while len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
            #        sleep(0.01);
            #else:
            #    overlocked=False;
            lockfile=workpath+"/"+kwargs['stepid']+".lock";
            with open(lockfile,'w') as lockstream:
                lockstream.write(str(countresult));
                lockstream.flush();
            for bulkcoll in bulkdict.keys():
                try:
                    bulkdict[bulkcoll].execute();
                except BulkWriteError as bwe:
                    pprint(bwe.details);
            #while True:
            #    try:
            #        fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
            #        break;
            #    except IOError:
            #        sleep(0.01);
            #tempiostream.close();
            #with open(workpath+"/"+stepname+".temp","r") as tempiostream, open(workpath+"/"+stepname+".out","a") as iostream:
            #    for line in tempiostream:
            #        iostream.write(line);
            #        iostream.flush();
            #    os.remove(tempiostream.name);
            #sys.stdout.write(tempiostream.getvalue());
            #sys.stdout.flush();
            if kwargs['templocal']:
                tempiostream.truncate(0);
            if kwargs['writelocal'] or kwargs['statslocal']:
                outiostream.write(iostream.getvalue());
                outiostream.flush();
            if kwargs['logging']:
                sys.stdout.write(logiostream.getvalue());
                sys.stdout.flush();
            #fcntl.flock(sys.stdout,fcntl.LOCK_UN);
            bulkdict={};
            #tempiostream=open(workpath+"/"+stepname+".temp","w");
            iostream.truncate(0);
            logiostream.truncate(0);
            countresult=0;
            nbatch=randint(1,kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch'];
            os.remove(lockfile);
            #if overlocked:
            #    os.kill(process.pid,signal.SIGCONT);

    while not stderr_queue.empty():
        stderr_line=stderr_queue.get();
        exitstring="sacct -n -o 'ExitCode' -j \""+kwargs['stepid']+"\" | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2";
        exitcode=subprocess.Popen(exitstring,shell=True,stdout=subprocess.PIPE).communicate()[0];
        with open(filename+".err","a") as errstream:
            errstream.write("ExitCode: "+exitcode+"\n");
            errstream.write(stderr_line)
            errstream.flush();
        while True:
            try:
                fcntl.flock(sys.stderr,fcntl.LOCK_EX | fcntl.LOCK_NB);
                break;
            except IOError:
                sleep(0.01);
        sys.stderr.write(stderr_line);
        sys.stderr.flush();
        fcntl.flock(sys.stderr,fcntl.LOCK_UN);

    sleep(kwargs['delay']);

while not stdout_queue.empty():
    while (not stdout_queue.empty()) and ((countresult<nbatch) or (len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers'])):
        line=stdout_queue.get().rstrip("\n");
        linehead=re.sub("^([-+&@].*?>|None).*",r"\1",line);
        linemarker=linehead[0];
        if linemarker=="-":
            newcollection,strindexdoc=linehead[1:-1].split(".");
            newindexdoc=json.loads(strindexdoc);
            linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
            doc=json.loads(linedoc);
            bsonsize-=mongolink.bsonsize(doc);
            if kwargs['templocal']:
                tempiostream.write(line+"\n");
                tempiostream.flush();
            if kwargs['writelocal']:
                iostream.write(line+"\n");
                iostream.flush();
            if kwargs['writedb']:
                if newcollection not in bulkdict.keys():
                    bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                bulkdict[newcollection].find(newindexdoc).update({"$unset":doc});
        elif linemarker=="+":
            #tempiostream.write(linehead[1:-1].split(".")+"\n");
            #sys.stdout.flush();
            newcollection,strindexdoc=linehead[1:-1].split(".");
            newindexdoc=json.loads(strindexdoc);
            linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
            doc=json.loads(linedoc);
            bsonsize+=mongolink.bsonsize(doc);
            if kwargs['templocal']:
                tempiostream.write(line+"\n");
                tempiostream.flush();
            if kwargs['writelocal']:
                iostream.write(line+"\n");
                iostream.flush();
            if kwargs['writedb']:
                if newcollection not in bulkdict.keys():
                    bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":doc});
        elif linemarker=="&":
            newcollection,strindexdoc=linehead[1:-1].split(".");
            newindexdoc=json.loads(strindexdoc);
            linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
            doc=json.loads(linedoc);
            bsonsize+=mongolink.bsonsize(doc);
            if kwargs['templocal']:
                tempiostream.write(line+"\n");
                tempiostream.flush();
            if kwargs['writelocal']:
                iostream.write(line+"\n");
                iostream.flush();
            if kwargs['writedb']:
                if newcollection not in bulkdict.keys():
                    bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet":doc});
        elif linemarker=="@":
            if kwargs['statslocal'] or kwargs['statsdb']:
                cputime="%.2f" % stats_reader.stat("TotalCPUTime");
                maxrss=stats_reader.max_stat("Rss");
                maxvmsize=stats_reader.max_stat("Size");
            #    stats=getstats("sstat",["MaxRSS","MaxVMSize"],kwargs['stepid']);
            #    if (len(stats)==1) and (stats[0]==""):
            #        newtotcputime,maxrss,maxvmsize=[eval(x) for x in getstats("sacct",["CPUTimeRAW","MaxRSS","MaxVMSize"],kwargs['stepid'])];
            #    else:
            #        newtotcputime=eval(getstats("sacct",["CPUTimeRAW"],kwargs['stepid'])[0]);
            #        maxrss,maxvmsize=stats;
            #    cputime=newtotcputime-totcputime;
            #    totcputime=newtotcputime;
            #newcollection,strindexdoc=linehead[1:].split("<")[0].split(".");
            #newindexdoc=json.loads(strindexdoc);
            newcollection=basecoll;
            doc=json.loads(temp_queue.get());
            newindexdoc=dict([(x,doc[x]) for x in kwargs['dbindexes']]);               
            if kwargs['logging']:
                logiostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<OUT\n");
                logiostream.flush();
                sys.stdout.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"<TEMP\n");
                sys.stdout.flush();
            if kwargs['templocal']:
                tempiostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n");
                tempiostream.write("CPUTime: "+str(cputime)+" seconds\n");
                tempiostream.write("MaxRSS: "+str(maxrss)+" bytes\n");
                tempiostream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
                tempiostream.write("BSONSize: "+str(bsonsize)+" bytes\n");
                tempiostream.flush();
            if kwargs['writelocal']:
                iostream.write(line+newcollection+"."+json.dumps(newindexdoc,separators=(',',':'))+"\n");
                iostream.flush();
            if kwargs['statslocal']:
                iostream.write("CPUTime: "+str(cputime)+" seconds\n");
                iostream.write("MaxRSS: "+str(maxrss)+" bytes\n");
                iostream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
                iostream.write("BSONSize: "+str(bsonsize)+" bytes\n");
                iostream.flush();
            statsmark={};
            if kwargs['statsdb']:
                statsmark.update({modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}});
            if kwargs['markdone']!="":
                statsmark.update({modname+kwargs['markdone']:True});
            if len(statsmark)>0:
                if newcollection not in bulkdict.keys():
                    bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":statsmark});
            bsonsize=0;
        else:
            if kwargs['templocal']:
                tempiostream.write(line+"\n");
                tempiostream.flush();
            if kwargs['writelocal']:
                iostream.write(line+"\n");
                iostream.flush();
        countresult+=1;
        #if len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']:
        #    lockfile=workpath+"/"+kwargs['stepid']+".lock";
        #    with open(lockfile,'w') as lockstream:
        #        lockstream.write(str(countresult));
        #        lockstream.flush();
        #    for bulkcoll in bulkdict.keys():
        #        try:
        #            bulkdict[bulkcoll].execute();
        #        except BulkWriteError as bwe:
        #            pprint(bwe.details);
        #    while True:
        #        try:
        #            fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
        #            break;
        #        except IOError:
        #            sleep(0.01);
        #    sys.stdout.write(tempiostream.getvalue());
        #    sys.stdout.flush();
        #    fcntl.flock(sys.stdout,fcntl.LOCK_UN);
        #    bulkdict={};
        #    tempiostream=cStringIO.StringIO();
        #    countresult=0;
        #    os.remove(lockfile);

    if (countresult>=nbatch) and (len(glob.glob(workpath+"/*.lock"))<kwargs['nworkers']):
        #if len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
        #    overlocked=True;
        #    os.kill(process.pid,signal.SIGSTOP);
        #    while len(glob.glob(workpath+"/*.lock"))>=kwargs['nworkers']:
        #        sleep(0.01);
        #else:
        #    overlocked=False;
        lockfile=workpath+"/"+kwargs['stepid']+".lock";
        with open(lockfile,'w') as lockstream:
            lockstream.write(str(countresult));
            lockstream.flush();
        for bulkcoll in bulkdict.keys():
            try:
                bulkdict[bulkcoll].execute();
            except BulkWriteError as bwe:
                pprint(bwe.details);
        #while True:
        #    try:
        #        fcntl.flock(sys.stdout,fcntl.LOCK_EX | fcntl.LOCK_NB);
        #        break;
        #    except IOError:
        #        sleep(0.01);
        #tempiostream.close();
        #with open(workpath+"/"+stepname+".temp","r") as tempiostream, open(workpath+"/"+stepname+".out","a") as iostream:
        #    for line in tempiostream:
        #        iostream.write(line);
        #        iostream.flush();
        #    os.remove(tempiostream.name);
        #sys.stdout.write(tempiostream.getvalue());
        #sys.stdout.flush();
        if kwargs['templocal']:
            tempiostream.truncate(0);
        if kwargs['writelocal'] or kwargs['statslocal']:
            outiostream.write(iostream.getvalue());
            outiostream.flush();
        if kwargs['logging']:
            sys.stdout.write(logiostream.getvalue());
            sys.stdout.flush();
        #fcntl.flock(sys.stdout,fcntl.LOCK_UN);
        bulkdict={};
        #tempiostream=open(workpath+"/"+stepname+".temp","w");
        iostream.truncate(0);
        logiostream.truncate(0);
        countresult=0;
        nbatch=randint(1,kwargs['nbatch']) if kwargs['random_nbatch'] else kwargs['nbatch'];
        os.remove(lockfile);
        #if overlocked:
        #    os.kill(process.pid,signal.SIGCONT);

while not stderr_queue.empty():
    stderr_line=stderr_queue.get();
    exitstring="sacct -n -o 'ExitCode' -j \""+kwargs['stepid']+"\" | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2";
    exitcode=subprocess.Popen(exitstring,shell=True,stdout=subprocess.PIPE).communicate()[0];
    with open(filename+".err","a") as errstream:
        errstream.write("ExitCode: "+exitcode+"\n");
        errstream.write(stderr_line)
        errstream.flush();
    while True:
        try:
            fcntl.flock(sys.stderr,fcntl.LOCK_EX | fcntl.LOCK_NB);
            break;
        except IOError:
            sleep(0.01);
    sys.stderr.write(stderr_line);
    sys.stderr.flush();
    fcntl.flock(sys.stderr,fcntl.LOCK_UN);

if kwargs['input_file']!=None:
    stdin_iter_file.close();
if kwargs['writelocal'] or kwargs['statslocal']:
    outiostream.close();
    if kwargs['templocal']:
        tempiostream.close();
iostream.close();
if kwargs['logging']:
    logiostream.close();

stdout_reader.join();
stderr_reader.join();
if kwargs['statslocal'] or kwargs['statsdb']:
    stats_reader.join();

process.stdin.close();
process.stdout.close();
process.stderr.close();