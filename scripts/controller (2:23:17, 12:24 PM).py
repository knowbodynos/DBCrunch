#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import time;
#Timer and maxjobcount initialization
starttime=time.time();

import sys,os,errno,re,linecache,signal,fcntl,traceback,subprocess,datetime,tempfile,json,toriccy;#,json;
#from pymongo import MongoClient;

#Misc. function definitions
def PrintException():
    "If an exception is raised, print traceback of it to output log."
    exc_type, exc_obj, tb = sys.exc_info();
    f = tb.tb_frame;
    lineno = tb.tb_lineno;
    filename = f.f_code.co_filename;
    linecache.checkcache(filename);
    line = linecache.getline(filename, lineno, f.f_globals);
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj);
    print "More info: ",traceback.format_exc();

def default_sigpipe():
    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

'''
def py2mat(lst):
    "Converts a Python list to a string depicting a list in Mathematica format."
    return str(lst).replace(" ","").replace("[","{").replace("]","}");

def mat2py(lst):
    "Converts a string depicting a list in Mathematica format to a Python list."
    return eval(str(lst).replace(" ","").replace("{","[").replace("}","]"));

def deldup(lst):
    "Delete duplicate elements in lst."
    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]];

def transpose_list(lst):
    "Get the transpose of a list of lists."
    return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))];

#Module-specific function definitions
def collectionfind(db,collection,query,projection):
    if projection=="Count":
        result=db[collection].find(query).count();
    else:
        result=list(db[collection].find(query,projection));
    #return [dict(zip(y.keys(),[mat2py(y[x]) for x in y.keys()])) for y in result];
    return result;

def collectionfieldexists(db,collection,field):
    result=db[collection].find({},{"_id":0,field:1}).limit(1).next()!={};
    return result;

def listindexes(db,collection,filters,indexes=["POLYID","GEOMN","TRIANGN","INVOLN"]):
    trueindexes=[x for x in indexes if collectionfieldexists(db,collection,x)]
    if len(trueindexes)==0:
        return [];
    indexlist=deldup([dict([(x,z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters]);
    return indexlist;

def sameindexes(filter1,filter2,indexes=["POLYID","GEOMN","TRIANGN","INVOLN"]):
    return all([filter1[x]==filter2[x] for x in filter1 if (x in indexes) and (x in filter2)]);

def querydatabase(db,queries,tiers=["POLY","GEOM","TRIANG","INVOL"]):
    sortedprojqueries=sorted([y for y in queries if y[2]!="Count"],key=lambda x: (len(x[1]),tiers.index(x[0])),reverse=True);
    maxcountquery=[] if len(queries)==len(sortedprojqueries) else [max([y for y in queries if y not in sortedprojqueries],key=lambda x: len(x[1]))];
    sortedqueries=sortedprojqueries+maxcountquery;
    totalresult=collectionfind(db,*sortedqueries[0]);
    if sortedqueries[0][2]=="Count":
        return totalresult;
    for i in range(1,len(sortedqueries)):
        indexlist=listindexes(db,sortedqueries[i][0],totalresult);
        if len(indexlist)==0:
            orgroup=sortedqueries[i][1];
        else:
            orgroup=dict(sortedqueries[i][1].items()+{"$or":indexlist}.items());
        nextresult=collectionfind(db,sortedqueries[i][0],orgroup,sortedqueries[i][2]);
        if sortedqueries[i][2]=="Count":
            return nextresult;
        totalresult=[dict(x.items()+y.items()) for x in totalresult for y in nextresult if sameindexes(x,y)];
    return totalresult;

def querytofile(db,queries,inputpath,inputfile,tiers=["POLY","GEOM","TRIANG","INVOL"]):
    results=querydatabase(db,queries,tiers);
    with open(inputpath+"/"+inputfile,"a") as inputstream:
        for doc in results:
            json.dump(doc,inputstream,separators=(',', ':'));
            inputstream.write("\n");
            inputstream.flush();

def py2matdict(dic):
    return str(dic).replace("u'","'").replace(" ","").replace("'","\\\"").replace(":","->");
'''

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

def seconds2timestamp(seconds):
    timestamp="";
    days=str(seconds/(60*60*24));
    remainder=seconds%(60*60*24);
    hours=str(remainder/(60*60)).zfill(2);
    remainder=remainder%(60*60);
    minutes=str(remainder/60).zfill(2);
    remainder=remainder%60;
    seconds=str(remainder).zfill(2);
    if days!="0":
        timestamp+=days+"-";
    timestamp+=hours+":"+minutes+":"+seconds;
    return timestamp;

#def contractededjobname2jobdocs(jobname,dbindexes):
#    indexsplit=[[eval(y) for y in x.split("_")] for x in jobname.lstrip("[").rstrip("]").split(",")];
#    return [dict([(dbindexes[i],x[i]) for i in range(len(dbindexes))]) for x in indexsplit];

def jobstepname2indexdoc(jobstepname,dbindexes):
    indexsplit=jobstepname.split("_");
    nindexes=min(len(indexsplit)-2,len(dbindexes));
    return dict([(dbindexes[i],eval(indexsplit[i+2])) for i in range(nindexes)]);

def indexdoc2jobstepname(doc,modname,controllername,dbindexes):
    return modname+"_"+controllername+"_"+"_".join([str(doc[x]) for x in dbindexes if x in doc.keys()]);

#def doc2jobjson(doc,dbindexes):
#    return dict([(y,doc[y]) for y in dbindexes]);

#def jobnameexpand(jobname):
#    bracketexpanded=jobname.rstrip("]").split("[");
#    return [bracketexpanded[0]+x for x in bracketexpanded[1].split(",")];

#def jobstepnamescontract(jobstepnames):
#    "3 because modname,controllername are first two."
#    bracketcontracted=[x.split("_") for x in jobstepnames];
#    return '_'.join(bracketcontracted[0][:-3]+["["])+','.join(['_'.join(x[-3:]) for x in bracketcontracted])+"]";

def formatinput(doc):#,scriptlanguage):
    #if scriptlanguage=="python":
    #    formatteddoc=doc;
    #elif scriptlanguage=="sage":
    #    formatteddoc=doc;
    #elif scriptlanguage=="mathematica":
    #    formatteddoc=toriccy.pythondictionary2mathematicarules(doc);
    #return str(formatteddoc).replace(" ","");
    return json.dumps(doc,separators=(',',':')).replace("\"","\\\"");

def getpartitiontimelimit(partition,scripttimelimit,scriptbuffertime):
    maxtimelimit=subprocess.Popen("sinfo -h -o '%l %P' | grep -E '"+partition+"\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    #print "getpartitiontimelimit";
    #print "sinfo -h -o '%l %P' | grep -E '"+partition+"\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1";
    #print "";
    #sys.stdout.flush();
    if scripttimelimit in ["","infinite"]:
        partitiontimelimit=maxtimelimit;
    else:
        if maxtimelimit=="infinite":
            partitiontimelimit=scripttimelimit;
        else:
            partitiontimelimit=min(maxtimelimit,scripttimelimit,key=timestamp2unit);
    if partitiontimelimit=="infinite":
        buffertimelimit=partitiontimelimit;
    else:
        buffertimelimit=seconds2timestamp(timestamp2unit(partitiontimelimit)-timestamp2unit(scriptbuffertime));
    return [partitiontimelimit,buffertimelimit];

def timeleft(starttime,buffertimelimit):
    "Determine if runtime limit has been reached."
    #print str(time.time()-starttime)+" "+str(timestamp2unit(buffertimelimit));
    #sys.stdout.flush();
    if buffertimelimit=="infinite":
        return 1;
    else:
        return timestamp2unit(buffertimelimit)-(time.time()-starttime);

#def timeleftq(controllerjobid,buffertimelimit):
#    "Determine if runtime limit has been reached."
#    if buffertimelimit=="infinite":
#        return True;
#    else:
#        timestats=subprocess.Popen("sacct -n -j \""+controllerjobid+"\" -o 'Elapsed,Timelimit' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
#        elapsedtime,timelimit=timestats.split(",");
#        return timestamp2unit(elapsedtime)<timestamp2unit(buffertimelimit);

#def clusterjobslotsleft(maxjobcount):
#    njobs=eval(subprocess.Popen("squeue -h -r | wc -l",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
#    return njobs<maxjobcount;

def availlicensecount(scriptpath,scriptlanguage):
    navaillicensesplit=[eval(x) for x in subprocess.Popen(scriptpath+"/tools/"+scriptlanguage+"licensecount.bash",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",")];
    #print "licensecount";
    #print scriptpath+"/"+scriptlanguage+"licensecount.bash";
    #print "";
    #sys.stdout.flush();
    return navaillicensesplit;

def pendlicensecount(username,modlist,modulesdirpath,softwarestatefile):
    npendjobsteps=0;
    npendjobthreads=0;
    grepmods="|".join(modlist);
    pendjobnames=subprocess.Popen("squeue -h -u "+username+" -o '%T %j' | grep 'PENDING' | cut -d' ' -f2 | grep -E \"("+grepmods+")\" | grep -v \"controller\" | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    if pendjobnames!="":
        pendjobnamesplit=pendjobnames.split(",");
        for pjn in pendjobnamesplit:
            pjnsplit=pjn.split("_");
            modname=pjnsplit[0];
            controllername=pjnsplit[1];
            nsteps=1-eval(pjnsplit[5]);
            scriptlanguage=subprocess.Popen("cat "+modulesdirpath+"/"+modname+"/"+controllername+"/controller_"+modname+"_"+controllername+".job | grep 'scriptlanguage=' | cut -d'=' -f2 | cut -d'\"' -f2 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
            needslicense=eval(subprocess.Popen("cat "+softwarestatefile+" | grep \""+scriptlanguage+"\" | cut -d',' -f2 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
            njobthreads=eval(subprocess.Popen("echo \"$(cat "+modulesdirpath+"/"+modname+"/"+controllername+"/jobs/"+pjn+".job | grep -E \"njobstepthreads\[[0-9]+\]=\" | cut -d'=' -f2 | tr '\n' '+' | head -c -1)\" | bc | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
            npendjobsteps+=nsteps;
            npendjobthreads+=njobthreads;
    return [npendjobsteps,npendjobthreads];

def licensecount(username,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage):
    navaillicensesplit=availlicensecount(scriptpath,scriptlanguage);
    npendlicensesplit=pendlicensecount(username,modlist,modulesdirpath,softwarestatefile);
    try:
        nlicensesplit=[navaillicensesplit[i]-npendlicensesplit[i] for i in range(len(navaillicensesplit))];
    except IndexError:
        raise;
    return nlicensesplit;

def clusterjobslotsleft(username,maxjobcount):
    njobs=eval(subprocess.Popen("squeue -h -r -u "+username+" | wc -l | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
    jobsleft=(njobs<maxjobcount);
    return jobsleft;

def clusterlicensesleft(nlicensesplit,requiredthreads):#,minnsteps=1):
    nlicenses=nlicensesplit[0];
    licensesleft=(nlicenses>0);#(navaillicenses>=minnsteps));
    if len(nlicensesplit)>1:
        nsublicenses=nlicensesplit[1];
        licensesleft=(licensesleft and (nsublicenses>=requiredthreads));
    return licensesleft;

def userjobsrunningq(username,modname,controllername):
    njobsrunning=eval(subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+controllername+"' | wc -l | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
    #print "userjobsrunningq";
    #print "squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+controllername+"' | wc -l | head -c -1";
    #print "";
    #sys.stdout.flush();
    return njobsrunning>0;

def prevcontrollersrunningq(username,modlist,controllername):
    if len(modlist)==0:
        njobsrunning=0;
    else:
        grepstr="\|".join(["^controller_"+x+"_"+controllername for x in modlist]);
        njobsrunning=eval(subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '"+grepstr+"' | wc -l | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
        #print "prevcontrollersrunningq";
        #print "squeue -h -u "+username+" -o '%j' | grep '"+grepstr+"' | wc -l | head -c -1";
        #print "";
        #sys.stdout.flush();
    return njobsrunning>0;

def userjobsrunninglist(username,modname,controllername):
    jobsrunningstring=subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+controllername+"' | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    #print "userjobsrunninglist";
    #print "squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+controllername+"' | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1";
    #print "";
    #sys.stdout.flush();
    if jobsrunningstring=='':
        return [];
    else:
        jobsrunning=jobsrunningstring.split(",");
        return jobsrunning;

def submitjob(jobpath,jobname,jobstepnames,partition,memoryperstep,nodemaxmemory,resubmit=False):
    submit=subprocess.Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
    submitcomm=submit.communicate()[0].rstrip("\n");
    #Print information about controller job submission
    if resubmit:
        #jobid=submitcomm.split(' ')[-1];
        #maketop=subprocess.Popen("scontrol top "+jobid,shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
        print "";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print "Res"+submitcomm[1:]+" as "+jobname+" on partition "+partition+" with "+str(nodemaxmemory/1000000)+"MB RAM allocated.";
        submitcomm=submitcomm.replace("Submitted batch job ","With job step ");
        for i in range(len(jobstepnames)):
            print "...."+submitcomm[1:]+"."+str(i)+" as "+jobstepnames[i]+" on partition "+partition+" with "+str(memoryperstep/1000000)+"MB RAM allocated.";
        print "";
        print "";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print submitcomm+" as "+jobname+" on partition "+partition+" with "+str(nodemaxmemory/1000000)+"MB RAM allocated.";
        submitcomm=submitcomm.replace("Submitted batch job ","With job step ");
        for i in range(len(jobstepnames)):
            print "...."+submitcomm+"."+str(i)+" as "+jobstepnames[i]+" on partition "+partition+" with "+str(memoryperstep/1000000)+"MB RAM allocated.";
        print "";
    sys.stdout.flush();

def submitcontrollerjob(jobpath,jobname,partition,nodemaxmemory,resubmit=False):
    submit=subprocess.Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
    submitcomm=submit.communicate()[0].rstrip("\n");
    #Print information about controller job submission
    if resubmit:
        #jobid=submitcomm.split(' ')[-1];
        #maketop=subprocess.Popen("scontrol top "+jobid,shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
        print "";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print "Res"+submitcomm[1:]+" as "+jobname+" on partition "+partition+" with "+str(nodemaxmemory/1000000)+"MB RAM allocated.";
        print "";
        print "";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print submitcomm+" as "+jobname+" on partition "+partition+" with "+str(nodemaxmemory/1000000)+"MB RAM allocated.";
        print "";
    sys.stdout.flush();

#def skippedjobslist(username,modname,controllername,workpath):
#    jobsrunning=userjobsrunninglist(username,modname,controllername);
#    blankfilesstring=subprocess.Popen("find '"+workpath+"' -maxdepth 1 -type f -name '*.out' -empty | rev | cut -d'/' -f1 | rev | cut -d'.' -f1 | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
#    if blankfilesstring=='':
#        return [];
#    else:
#        blankfiles=blankfilesstring.split(",");
#        skippedjobs=[modname+"_"+controllername+"_"+x for x in blankfiles if x not in jobsrunning];
#        return skippedjobs;

def reloadskippedjobs(modname,controllername,controllerpath,querystatefilename,basecollection):
    try:
        skippedjobs=[];
        with open(controllerpath+"/skippedstate","r") as skippedstream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream:
            skippedheader=skippedstream.readline();
            tempstream.write(skippedheader);
            tempstream.flush();
            for line in skippedstream:
                [skippedjob,exitcode,resubmitq]=line.rstrip("\n").split(",");
                if eval(resubmitq):
                    skippedjobsplit=skippedjob.split("_");
                    if skippedjobsplit[:2]==[modname,controllername]:
                        skippedjobs+=[skippedjob];
                    else:
                        tempstream.write(line);
                        tempstream.flush();
                else:
                    tempstream.write(line);
                    tempstream.flush();
            os.rename(tempstream.name,skippedstream.name);
        if len(skippedjobs)>0:
            querystatetierfilenames=subprocess.Popen("find "+controllerpath+"/ -maxdepth 1 -type f -name '"+querystatefilename+"*' 2>/dev/null | rev | cut -d'/' -f1 | rev | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
            for querystatetierfilename in querystatetierfilenames:
                #if querystatetierfilename==querystatefilename+basecollection:
                #    with open(controllerpath+"/"+querystatetierfilename,"a") as querystatefilestream:
                #        for i in range(len(skippedjobs)):
                #            line=skippedjobs[i];
                #            if i<len(skippedjobs)-1:
                #                line+="\n";
                #            querystatefilestream.write(line);
                #            querystatefilestream.flush();
                #else:
                if querystatetierfilename!=querystatefilename+basecollection:
                    try:
                        with open(controllerpath+"/"+querystatetierfilename,"r") as querystatefilestream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream:
                            for line in querystatefilestream:
                                linestrip=line.rstrip("\n");
                                if not any([linestrip+"_" in x for x in skippedjobs]):
                                    tempstream.write(line);
                                    tempstream.flush();
                            os.rename(tempstream.name,querystatefilestream.name);
                    except IOError:
                        print "File path \""+controllerpath+"/"+querystatetierfilename+"\" does not exist.";
                        sys.stdout.flush();
    except IOError:
        print "File path \""+controllerpath+"/skippedstate\" does not exist.";
        sys.stdout.flush();

def releaseheldjobs(username,modname,controllername):
    subprocess.Popen("for job in $(squeue -h -u "+username+" -o '%A %j %r' | grep '^"+modname+"_"+controllername+"' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done",shell=True,preexec_fn=default_sigpipe);
    #print "releaseheldjobs";
    #print "for job in $(squeue -h -u "+username+" -o '%A %j %r' | grep '^"+modname+"_"+controllername+"' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done";
    #print "";
    #sys.stdout.flush();

def orderpartitions(partitions):
    greppartitions="|".join(partitions);
    partitionsidle=subprocess.Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    #partsmix=subprocess.Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'mix' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    #partsalloc=subprocess.Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'alloc' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    #partslist=','.join(partsidle+[x for x in partsmix if x not in partsidle]+[x for x in partsalloc if x not in partsidle+partsmix]);
    partitionscomp=subprocess.Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'comp' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    partitionsrun=subprocess.Popen("squeue -h -o '%L %T %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'RUNNING' | sed 's/^\([0-9][0-9]:[0-9][0-9]\s\)/00:\1/g' | sed 's/^\([0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0\1/g' | sed 's/^\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\s\)/0-\1/g' | sort -k1,1 | cut -d' ' -f3 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    partitionspend=subprocess.Popen("sinfo -h -o '%t %c %P' --partition=$(squeue -h -o '%T %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'PENDING' | sort -k2,2 -u | cut -d' ' -f2 | tr '\n' ',' | head -c -1) | grep 'alloc' | sort -k2,2n | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    #print "orderpartitions";
    #print "for rawpart in $(sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | cut -d' ' -f2,3,4 | sed 's/\*//g' | sed 's/\s/,/g'); do echo $(echo $rawpart | cut -d',' -f1,2 | sed 's/,/*/g' | bc),$(echo $rawpart | cut -d',' -f3); done | tr ' ' '\n' | sort -t',' -k1 -n | cut -d',' -f2 | tr '\n' ',' | head -c -1";
    #print "squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'COMPLETING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1";
    #print "squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'RUNNING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1";
    #print "squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'PENDING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1";
    #print "";
    #sys.stdout.flush();
    orderedpartitions=[x for x in toriccy.deldup(partitionsidle+partitionscomp+partitionsrun+partitionspend+partitions) if x!=""];
    return orderedpartitions;

def getnodemaxmemory(resourcesstatefile,partition):
    nodemaxmemory=0;
    try:
        with open(resourcesstatefile,"r") as resourcesstream:
            resourcesheader=resourcesstream.readline();
            for resourcesstring in resourcesstream:
                resources=resourcesstring.rstrip("\n").split(",");
                if resources[0]==partition:
                    nodemaxmemory=eval(resources[1]);
                    break;
    except IOError:
        print "File path \""+resourcesstatefile+"\" does not exist.";
        sys.stdout.flush();
    return nodemaxmemory;

def distributeovernodes(resourcesstatefile,partitions,scriptmemorylimit,maxsteps):
    #print partitions;
    #sys.stdout.flush();
    partition=partitions[0];
    ncoresperpartition=eval(subprocess.Popen("sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
    maxnnodes=eval(subprocess.Popen("scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].rstrip("\n"));
    #print "distributeovernodes";
    #print "sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1";
    #print "scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1";
    #print "";
    #sys.stdout.flush();
    nodemaxmemory=getnodemaxmemory(resourcesstatefile,partition);
    if scriptmemorylimit=="":
        nstepsdistribmem=1;
    else:
        nstepsdistribmem=nodemaxmemory/eval(scriptmemorylimit);
    #nstepsfloat=min(float(ndocsleft),float(ncoresperpartition),nstepsdistribmem);
    #nnodes=int(min(maxnnodes,math.ceil(1./nstepsfloat)));
    #ncores=nnodes*ncoresperpartition;
    #nsteps=int(max(1,nstepsfloat));
    nnodes=1;
    if nstepsdistribmem<1:
        if len(partitions)>1:
            return distributeovernodes(resourcesstatefile,partitions[1:],scriptmemorylimit,maxsteps);
        else:
            print "Memory requirement is too large for this cluster.";
            sys.stdout.flush();
    nstepsfloat=min(ncoresperpartition,nstepsdistribmem,maxsteps);
    ncores=nnodes*ncoresperpartition;
    nsteps=int(nstepsfloat);
    memoryperstep=nodemaxmemory/nstepsdistribmem;
    return [partition,nnodes,ncores,nsteps,memoryperstep,nodemaxmemory];

def writejobfile(modname,dbpushq,markdone,jobname,jobstepnames,controllerpath,controllername,writemode,partitiontimelimit,buffertimelimit,partition,nnodes,ncores,memoryperstep,mongouri,scriptpath,scriptlanguage,scriptcommand,scriptflags,scriptext,basecollection,dbindexes,nthreadsfield,docbatch):
    ndocs=len(docbatch);
    #outputlinemarkertest="[[ "+" && ".join(["! \"$line\" =~ ^"+re.sub(r"([\]\[\(\)\\\.\^\$\?\*\+ ])",r"\\\1",x)+".*" for x in outputlinemarkers])+" ]]";
    #outputlinemarkertest="[[ "+" || ".join(["\"$line\" =~ ^"+re.sub(r"([\]\[\(\)\\\.\^\$\?\*\+ ])",r"\\\1",x)+".*" for x in outputlinemarkers])+" ]]";
    #outputlinemarkertest="[[ "+" && ".join(["! \"$line\" =~ ^"+x+".*" for x in outputlinemarkers])+" ]]";
    jobstring="#!/bin/bash\n";
    jobstring+="\n";
    jobstring+="#Created "+str(datetime.datetime.now().strftime("%Y %m %d %H:%M:%S"))+"\n";
    jobstring+="\n";
    jobstring+="#Job name\n";
    jobstring+="#SBATCH -J \""+jobname+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Working directory\n";
    jobstring+="#SBATCH -D \""+controllerpath+"/jobs\"\n";
    jobstring+="#################\n";
    jobstring+="#Job output file\n";
    jobstring+="#SBATCH -o \""+jobname+".out\"\n";
    jobstring+="#################\n";
    jobstring+="#Job error file\n";
    jobstring+="#SBATCH -e \""+jobname+".err\"\n";
    jobstring+="#################\n";
    jobstring+="#Job file write mode\n";
    jobstring+="#SBATCH --open-mode=\""+writemode+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Job max time\n";
    jobstring+="#SBATCH --time=\""+partitiontimelimit+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Partition (queue) to use for job\n";
    jobstring+="#SBATCH --partition=\""+partition+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Number of tasks (CPUs) allocated for job\n";
    jobstring+="#SBATCH -n "+str(ncores)+"\n";
    jobstring+="#################\n";
    jobstring+="#Number of nodes to distribute n tasks across\n";
    jobstring+="#SBATCH -N "+str(nnodes)+"\n";
    jobstring+="#################\n";
    jobstring+="#Lock down N nodes for job\n";
    jobstring+="#SBATCH --exclusive\n";
    jobstring+="#################\n";
    jobstring+="\n";
    jobstring+="#Database info\n";
    jobstring+="mongouri=\""+mongouri+"\"\n";
    jobstring+="basecollection=\""+basecollection+"\"\n";
    #jobstring+="newcollection=\""+newcollection+"\"\n";
    jobstring+="\n";
    jobstring+="#Cluster info\n";
    jobstring+="scriptpath=\""+scriptpath+"\"\n";
    jobstring+="modulescriptpath=\"${scriptpath}/modules\"\n";
    jobstring+="controllerpath=\""+controllerpath+"\"\n";
    jobstring+="workpath=\"${controllerpath}/jobs\"\n";
    jobstring+="\n";
    jobstring+="#Job info\n";
    jobstring+="modname=\""+modname+"\"\n";
    jobstring+="dbpushq=\""+dbpushq+"\"\n";
    jobstring+="markdone=\""+markdone+"\"\n";
    #jobstring+="outputlinemarkers=\""+",".join(outputlinemarkers)+"\"\n";
    #jobstring+="scripttimelimit=\""+str(scripttimelimit)+"\"\n";
    #jobstring+="scriptmemorylimit=\""+str(memoryperstep)+"\"\n";
    #jobstring+="skippedfile=\"${controllerpath}/skippedstate\"\n";
    jobstring+="\n";
    for i in range(ndocs):
        jobstring+="jobstepnames["+str(i)+"]=\""+jobstepnames[i]+"\"\n";
        #jobstring+="newindexes["+str(i)+"]=\""+str(dict([(x,docs[i][x]) for x in dbindexes]))+"\"\n";
        jobstring+="docs["+str(i)+"]=\""+formatinput(docbatch[i])+"\"\n";#,scriptlanguage)+"\"\n";
        if nthreadsfield=="":
            jobstring+="njobstepthreads["+str(i)+"]=1\n";
        else:
            jobstring+="njobstepthreads["+str(i)+"]="+str(docbatch[i][nthreadsfield])+"\n";
    jobstring+="\n";
    #jobstring+="ndocs=0\n";
    #jobstring+="njobthreads=0\n";
    #jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    #jobstring+="do\n";
    #jobstring+="    ndocs=$((${ndocs}+1))\n";
    #jobstring+="    njobthreads=$((${njobthreads}+${njobstepthreads[${i}]}))\n";
    #jobstring+="done\n";
    #jobstring+="\n";

    #if needslicense:
    #    jobstring+="(\n";
    #    jobstring+="    flock -e 200\n";
    #    jobstring+="    pendlicensefile=$(<"+pendlicensestatefile+" tr '\n' ';')\n";
    #    jobstring+="    pendlicenseheader=$(echo \"${pendlicensefile}\" | cut -d';' -f1)\n";
    #    jobstring+="    pendlicenseline=$(echo \"${pendlicensefile}\" | cut -d';' -f2)\n";
    #    jobstring+="    if [[ \"$pendlicenseline\" =~ .*,.* ]]\n";
    #    jobstring+="    then\n";
    #    jobstring+="        npendlicenses=$(echo \"${pendlicenseline}\" | cut -d',' -f1)\n";
    #    jobstring+="        npendsublicenses=$(echo \"${pendlicenseline}\" | cut -d',' -f2)\n";
    #    jobstring+="        echo -e \"$pendlicenseheader\n$((${npendlicenses}-${ndocs})),$((${npendsublicenses}-${njobthreads}))\" >"+pendlicensestatefile+"\n";
    #    jobstring+="    else\n";
    #    jobstring+="        nlicenses=${licenseline}\n";
    #    jobstring+="        echo -e \"${pendlicenseheader}\n$((${npendlicenses}-1))\" >"+pendlicensestatefile+"\n";
    #    jobstring+="    fi\n";
    #    jobstring+=") 200<>"+pendlicensestatefile+"\n";
    #    jobstring+="\n";

    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    #jobstring+="    srun -N 1 -n 1 --exclusive -J \"${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" "+scripttype+" \"${scriptpath}/"+modname+scriptext+"\" \"${workpath}\" \"${jobstepnames[${i}]}\" \"${mongouri}\" \"${scripttimelimit}\" \"${scriptmemorylimit}\" \"${skippedfile}\" \"${docs[${i}]}\" > \"${workpath}/${jobstepnames[${i}]}.log\" &\n";
    jobstring+="    mpirun -srun -n \"${njobstepthreads[i]}\" -J \"${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" ";
    if buffertimelimit!="infinite":
        jobstring+="--time=\""+buffertimelimit+"\" ";
    jobstring+=scriptcommand+" "+scriptflags+" \"${modulescriptpath}/"+modname+scriptext+"\" \"${mongouri}\" \"${workpath}\" \"${jobstepnames[${i}]}\" \"${docs[${i}]}\" > \"${workpath}/${jobstepnames[${i}]}.log\" &\n";
    jobstring+="    sleep 0.1\n";
    jobstring+="    pids[${i}]=$!\n";
    jobstring+="done\n";
    jobstring+="\n";
    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    jobstring+="    wait ${pids[${i}]}\n\n";
    #jobstring+="    stats=($(sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j ${SLURM_JOBID}.${i} | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g'))\n"
    #jobstring+="    echo \"CPUTime: ${stats[0]}\" >> ${jobstepnames[${i}]}.log\n"
    #jobstring+="    echo \"MaxRSS: ${stats[1]}\" >> ${jobstepnames[${i}]}.log\n"
    #jobstring+="    echo \"MaxVMSize: ${stats[2]}\" >> ${jobstepnames[${i}]}.log\n"
    #jobstring+="    exitcode=$(sacct -n -j \"${SLURM_JOBID}.${i}\" -o 'ExitCode' | sed 's/\s*//g')\n";
    jobstring+="    sleep 0.1\n";
    jobstring+="    jobstepstats=($(sacct -n -o 'ExitCode,CPUTimeRAW,MaxRSS,MaxVMSize' -j \"${SLURM_JOBID}.${i}\" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2))\n";
    jobstring+="    exitcode=${jobstepstats[0]}\n";
    jobstring+="    cputime=${jobstepstats[1]}\n";
    jobstring+="    maxrss=${jobstepstats[2]}\n";
    jobstring+="    maxvmsize=${jobstepstats[3]}\n";
    jobstring+="    while [ \"${exitcode}\" == \"\" ] || [ \"${cputime}\" == \"\" ] || [ \"${maxrss}\" == \"\" ] || [ \"${maxvmsize}\" == \"\" ]\n";
    jobstring+="    do\n";
    jobstring+="        sleep 0.1\n";
    jobstring+="        jobstepstats=($(sacct -n -o 'ExitCode,CPUTimeRAW,MaxRSS,MaxVMSize' -j \"${SLURM_JOBID}.${i}\" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2))\n";
    jobstring+="        exitcode=${jobstepstats[0]}\n";
    jobstring+="        cputime=${jobstepstats[1]}\n";
    jobstring+="        maxrss=${jobstepstats[2]}\n";
    jobstring+="        maxvmsize=${jobstepstats[3]}\n";
    jobstring+="    done\n";
    if scriptlanguage=="mathematica":
        jobstring+="    perl -i -pe 's|^MathLink could not create temporary directory /tmp/MathLink: file exists.*\n$||g' \"${workpath}/${jobstepnames[${i}]}.log\"\n";
        jobstring+="    perl -i -pe 's|^The program may not function correctly.*\n$||g' \"${workpath}/${jobstepnames[${i}]}.log\"\n";
    #jobstring+="    exitcode=$(sacct -n -o 'ExitCode' -j \"${SLURM_JOBID}.${i}\" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2)\n";
    jobstring+="    skipped=false\n";
    jobstring+="    if [ \"${exitcode}\" == \"0:0\" ] && test -s \"${workpath}/${jobstepnames[${i}]}.log\"\n";
    jobstring+="    then\n";
    jobstring+="        while read line\n";
    jobstring+="        do\n";
    jobstring+="            if [[ ! \"$line\" =~ ^\+.* && ! \"$line\" =~ ^-.* && ! \"$line\" =~ ^None.* ]]\n";
    jobstring+="            then\n";
    jobstring+="                skipped=true\n";
    jobstring+="                break\n";
    jobstring+="            fi\n";
    jobstring+="        done < \"${workpath}/${jobstepnames[${i}]}.log\"\n";
    jobstring+="    else\n";
    jobstring+="        skipped=true\n";
    jobstring+="    fi\n";
    jobstring+="    \n";
    jobstring+="    if ! ${skipped}\n";
    jobstring+="    then\n";
    #jobstring+="        srun -N 1 -n 1 --exclusive -J \"stats_${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" python \"${scriptpath}/stats.py\" \"${mongouri}\" \"${modname}\" \"${SLURM_JOBID}.${i}\" \"${basecollection}\" \"${workpath}\" \"${outputlinemarkers}\" \"${jobstepnames[${i}]}\" >> \"${workpath}/${jobstepnames[${i}]}.log\" &\n";# > ${workpath}/${jobname}.log\n";
    jobstring+="        srun -N 1 -n 1 --exclusive -J \"stats_${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" python \"${scriptpath}/stats.py\" \"${mongouri}\" \"${modname}\" \"${basecollection}\" \"${workpath}\" \"${jobstepnames[${i}]}\" \"${dbpushq}\" \"${markdone}\" \"${cputime}\" \"${maxrss}\" \"${maxvmsize}\" >> \"${workpath}/${jobstepnames[${i}]}.log\" &\n";# > ${workpath}/${jobname}.log\n";
    #jobstring+="        srun -N 1 -n 1 --exclusive -J \"stats_${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" python \"${scriptpath}/stats.py\" \"${mongouri}\" \"${modname}\" \"${basecollection}\" \"${workpath}\" \"${dbpushq}\" \"${SLURM_JOBID}.${i}\" \"${jobstepnames[${i}]}\" >> \"${workpath}/${jobstepnames[${i}]}.log\" &\n";
    jobstring+="    else\n";
    #jobstring+="        echo \"${jobstepnames[${i}]},${exitcode}\" >> \"${controllerpath}/hi\"\n";
    jobstring+="        echo \"${jobstepnames[${i}]},${exitcode},False\" >> \"${controllerpath}/skippedstate\"\n";
    jobstring+="    fi\n";
    jobstring+="    sleep 0.1\n";
    jobstring+="    pids[${i}]=$!\n";
    jobstring+="done\n";
    jobstring+="\n";
    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    jobstring+="    wait ${pids[${i}]}\n";
    jobstring+="done";
    jobstream=open(controllerpath+"/jobs/"+jobname+".job","w");
    jobstream.write(jobstring);
    jobstream.flush();
    jobstream.close();

def doinput(docbatch,nthreadsfield,username,maxjobcount,maxstepcount,controllerjobid,controllerbuffertimelimit,starttime,sleeptime,statusstatefile,partitions,scriptmemorylimit,softwarestatefile,modlist,modulesdirpath,scriptpath,scriptlanguage,licensestream):
    #print "Start input";
    #sys.stdout.flush();
    needslicense=(licensestream!=None);
    if (nthreadsfield!="") and (len(docbatch)>0):
        requiredthreads=docbatch[0][nthreadsfield];
    else:
        requiredthreads=1;    
        #print needslicense;
        #sys.stdout.flush();
    if needslicense:
        jobslotsleft=clusterjobslotsleft(username,maxjobcount);
        nlicensesplit=licensecount(username,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage);
        licensesleft=clusterlicensesleft(nlicensesplit,requiredthreads);
        if (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and licensesleft):
            with open(statusstatefile,"w") as statusstream:
                statusstream.truncate(0);
                statusstream.write("Waiting");
                statusstream.flush();
            while (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and licensesleft):
                time.sleep(sleeptime);
                jobslotsleft=clusterjobslotsleft(username,maxjobcount);
                nlicensesplit=licensecount(username,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage);
                licensesleft=clusterlicensesleft(nlicensesplit,requiredthreads);
        #fcntl.flock(licensestream,fcntl.LOCK_EX);
        #fcntl.LOCK_EX might only work on files opened for writing. This one is open as "a+", so instead use bitwise OR with non-blocking and loop until lock is acquired.
        while (timeleft(starttime,controllerbuffertimelimit)>0):
            try:
                fcntl.flock(licensestream,fcntl.LOCK_EX | fcntl.LOCK_NB);
                break;
            except IOError as e:
                if e.errno!=errno.EAGAIN:
                    raise;
                else:
                    time.sleep(0.1);
        if not (timeleft(starttime,controllerbuffertimelimit)>0):
            #print "hi";
            #sys.stdout.flush();
            return False;
        licensestream.seek(0,0);
        licenseheader=licensestream.readline();
        #print licenseheader;
        #sys.stdout.flush();
        licensestream.truncate(0);
        licensestream.seek(0,0);
        licensestream.write(licenseheader);
        licensestream.write(','.join([str(x) for x in nlicensesplit]));
        licensestream.flush();
        nlicenses=nlicensesplit[0];
        if (nthreadsfield!="") and (len(nlicensesplit)>1) and (len(docbatch)>0):
            nsublicenses=nlicensesplit[1];
            maxthreadsteps=0;
            cumulativethreads=0;
            while (maxthreadsteps<len(docbatch)) and (cumulativethreads<=nsublicenses):
                cumulativethreads+=docbatch[maxthreadsteps][nthreadsfield];
                maxthreadsteps+=1;
            if cumulativethreads>nsublicenses:
                maxthreadsteps-=1;
            #pendlicensestream.truncate(0);
            #pendlicensestream.seek(0,0);
            #pendlicensestream.write(pendlicenseheader);
            #pendlicensestream.write(str(nlicenses)+","+str(nsublicenses));
            maxsteps=min(maxstepcount,nlicenses,maxthreadsteps);
        else:
            #pendlicensestream.truncate(0);
            #pendlicensestream.seek(0,0);
            #pendlicensestream.write(licenseheader);
            #pendlicensestream.write(str(nlicenses));
            maxsteps=min(maxstepcount,nlicenses);
    else:
        jobslotsleft=clusterjobslotsleft(username,maxjobcount);
        if (timeleft(starttime,controllerbuffertimelimit)>0) and not jobslotsleft:
            with open(statusstatefile,"w") as statusstream:
                statusstream.truncate(0);
                statusstream.write("Waiting");
                statusstream.flush();
            while (timeleft(starttime,controllerbuffertimelimit)>0) and not jobslotsleft:
                time.sleep(sleeptime);
                jobslotsleft=clusterjobslotsleft(username,maxjobcount);
        if not (timeleft(starttime,controllerbuffertimelimit)>0):
            return False;
        maxsteps=maxstepcount;

    with open(statusstatefile,"w") as statusstream:
            statusstream.truncate(0);
            statusstream.write("Running");
            statusstream.flush();
    #orderedpartitions=orderpartitions(largemempartitions);
    #if doc2jobname(newqueryresult[i],dbindexes) not in skippedjobslist(username,modname,controllername,controllerpath):
    #orderedpartitions=orderpartitions(partitions)+orderedpartitions;
    orderedpartitions=orderpartitions(partitions);
    nodedistribution=distributeovernodes(resourcesstatefile,orderedpartitions,scriptmemorylimit,maxsteps);
    partition,nnodes,ncores,nsteps,memoryperstep,nodemaxmemory=nodedistribution;
    #print "End input";
    #sys.stdout.flush();
    return {"partition":partition,"nnodes":nnodes,"ncores":ncores,"nsteps":nsteps,"memoryperstep":memoryperstep,"nodemaxmemory":nodemaxmemory};

def doaction(batchcounter,stepcounter,inputdoc,docbatch,username,modname,dbpushq,markdone,controllername,scripttimelimit,scriptbuffertime,writemode,mongouri,basecollection,dbindexes,controllerpath,workpath,scriptpath,scriptlanguage,scriptcommand,scriptflags,scriptext,licensestream,nthreadsfield):
    #print "Start action";
    #sys.stdout.flush();
    needslicense=(licensestream!=None);
    #ndocs=len(docbatch);
    #if nthreadsfield=="":
    #    totnthreadsfield=ndocs;
    #else:
    #    totnthreadsfield=sum([x[nthreadsfield] for x in docbatch]);
    #while not clusterjobslotsleft(maxjobcount,scriptext,minnsteps=inputdoc["nsteps"]):
    #    time.sleep(sleeptime);
    #doc=json.loads(doc.rstrip('\n'));
    if len(docbatch)>0:
        jobstepnames=[indexdoc2jobstepname(x,modname,controllername,dbindexes) for x in docbatch];
        #jobstepnamescontract=jobstepnamescontract(jobstepnames);
        jobname=modname+"_"+controllername+"_job_"+str(batchcounter)+"_steps_"+str(stepcounter)+"-"+str(stepcounter+len(docbatch)-1);
        partitiontimelimit,buffertimelimit=getpartitiontimelimit(inputdoc["partition"],scripttimelimit,scriptbuffertime);
        #if len(docbatch)<inputdoc["nsteps"]:
        #    inputdoc["memoryperstep"]=(memoryperstep*inputdoc["nsteps"])/len(docbatch);
        writejobfile(modname,dbpushq,markdone,jobname,jobstepnames,controllerpath,controllername,writemode,partitiontimelimit,buffertimelimit,inputdoc["partition"],inputdoc["nnodes"],inputdoc["ncores"],inputdoc["memoryperstep"],mongouri,scriptpath,scriptlanguage,scriptcommand,scriptflags,scriptext,basecollection,dbindexes,nthreadsfield,docbatch);
        #Submit job file
        submitjob(workpath,jobname,jobstepnames,inputdoc["partition"],inputdoc["memoryperstep"],inputdoc["nodemaxmemory"],resubmit=False);
    if needslicense:
        #fcntl.flock(pendlicensestream,fcntl.LOCK_EX);
        #pendlicensestream.seek(0,0);
        #pendlicenseheader=pendlicensestream.readline();
        #npendlicensesplit=[eval(x) for x in pendlicensestream.readline().rstrip("\n").split(",")];
        #print npendlicensesplit;
        #pendlicensestream.truncate(0);
        #print "hi";
        #pendlicensestream.seek(0,0);
        #pendlicensestream.write(pendlicenseheader);
        #npendlicenses=npendlicensesplit[0];
        #if len(npendlicensesplit)>1:
        #    npendsublicenses=npendlicensesplit[1];
        #    pendlicensestream.write(str(npendlicenses+ndocs)+","+str(npendsublicenses+totnthreadsfield));
        #else:
        #    pendlicensestream.write(str(npendlicenses+ndocs));
        fcntl.flock(licensestream,fcntl.LOCK_UN);
    releaseheldjobs(username,modname,controllername);
    #print "End action";
    #sys.stdout.flush();
    #seekstream.write(querystream.tell());
    #seekstream.flush();
    #seekstream.seek(0);
    #doc=querystream.readline();

def docounterupdate(batchcounter,stepcounter,counterstatefile,counterheader):
    with open(counterstatefile,"w") as counterstream:
        counterstream.write(counterheader);
        counterstream.write(str(batchcounter)+","+str(stepcounter));
        counterstream.flush();

try:
    maxjobcount,maxstepcount=[eval(x) for x in subprocess.Popen("scontrol show config | grep 'MaxJobCount\|MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",")];
    #print "main";
    #print "scontrol show config | grep 'MaxJobCount\|MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | tr '\n' ',' | head -c -1";
    #print "";
    #sys.stdout.flush();

    #Cluster info
    username=subprocess.Popen("echo $USER | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];

    #Input controller info
    modname=sys.argv[1];
    controllername=sys.argv[2];
    controllerjobid=sys.argv[3];
    controllerpartition=sys.argv[4];
    controllerbuffertime=sys.argv[5];
    #largemempartitions=sys.argv[6].split(",");
    sleeptime=eval(sys.argv[6]);

    #seekfile=sys.argv[7]; 

    #Input path info
    mainpath=sys.argv[7];
    packagepath=sys.argv[8];
    scriptpath=sys.argv[9];

    #Input script info
    scriptlanguage=sys.argv[10];
    partitions=sys.argv[11].split(",");
    writemode=sys.argv[12];
    #scripttimelimit=timestamp2unit(sys.argv[15]);
    scriptmemorylimit=sys.argv[13];
    scripttimelimit=sys.argv[14];
    scriptbuffertime=sys.argv[15];
    #outputlinemarkers=sys.argv[15].split(",");

    #Input database info
    mongouri=sys.argv[16];#"mongodb://manager:toric@129.10.135.170:27017/ToricCY";
    queries=eval(sys.argv[17]);
    #dumpfile=sys.argv[13];
    basecollection=sys.argv[18];
    nthreadsfield=sys.argv[19];
    #newcollection,newfield=sys.argv[18].split(",");

    #Options
    dbpushq=sys.argv[20];
    markdone=sys.argv[21];
    
    #Read seek position from file
    #with open(controllerpath+"/"+seekfile,"r") as seekstream:
    #    seekpos=eval(seekstream.read());

    #Open seek file stream
    #seekstream=open(controllerpath+"/"+seekfile,"w");

    #If first submission, read from database
    #if seekpos==-1:
    #Open connection to remote database

    controllertimelimit=subprocess.Popen("sacct -n -j \""+controllerjobid+"\" -o 'Timelimit' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];

    print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
    print "Starting job controller_"+modname+"_"+controllername;
    print "";
    print "";
    sys.stdout.flush();

    statepath=packagepath+"/state";
    modulesdirpath=mainpath+"/modules";
    modulepath=modulesdirpath+"/"+modname;
    controllerpath=modulepath+"/"+controllername;
    workpath=controllerpath+"/jobs";  
    resourcesstatefile=statepath+"/resources";
    modulesstatefile=statepath+"/modules";
    softwarestatefile=statepath+"/software";
    counterstatefile=controllerpath+"/counterstate";
    statusstatefile=controllerpath+"/statusstate";
    querystatefilename="querystate";
    #querystatefile=controllerpath+"/querystate";

    with open(statusstatefile,"w") as statusstream:
            statusstream.truncate(0);
            statusstream.write("Starting");

    if scriptbuffertime=="":
        scriptbuffertime="00:00:00";

    controllerpartitiontimelimit,controllerbuffertimelimit=getpartitiontimelimit(controllerpartition,controllertimelimit,controllerbuffertime);
    
    mongoclient=toriccy.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=toriccy.getintersectionindexes(db,basecollection);
    #print dbindexes;
    #sys.stdout.flush();

    allindexes=toriccy.getunionindexes(db);
    needslicense=False;
    try:
        with open(softwarestatefile,"r") as softwarestream:
            softwareheader=softwarestream.readline();
            for softwareline in softwarestream:
                softwarelinesplit=softwareline.rstrip("\n").split(",");
                if softwarelinesplit[0]==scriptlanguage:
                    needslicense=eval(softwarelinesplit[1]);
                    scriptext,scriptcommand,scriptflags=softwarelinesplit[2:];
                    if needslicense:
                        licensestatefile=statepath+"/licenses/"+scriptlanguage+"licenses";
                        licensestream=open(licensestatefile,"a+");
                        #licenseheader=licensestream.readline();
                    else:
                        licensestream=None;
                    break;
        #print pendlicensestream;
        #sys.stdout.flush();
    except IOError:
        print "File path \""+softwarestatefile+"\" does not exist.";
        sys.stdout.flush();
        raise;
    
    try:
        with open(modulesstatefile,"r") as modstream:
            modulesheader=modstream.readline();
            modlist=[x.rstrip('\n') for x in modstream.readlines()];
    except IOError:
        print "File path \""+modulesstatefile+"\" does not exist.";
        sys.stdout.flush();
        raise;

    prevmodlist=modlist[:modlist.index(modname)];
    
    firstlastrun=(not (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername)));
    #if firstlastrun and needslicense:
    #   fcntl.flock(pendlicensestream,fcntl.LOCK_UN);
    #firstrun=True;
    try:
        with open(counterstatefile,"r") as counterstream:
            counterheader=counterstream.readline();
            counterline=counterstream.readline();
            [batchcounter,stepcounter]=[int(x) for x in counterline.rstrip("\n").split(",")];
    except IOError:
        print "File path \""+counterstatefile+"\" does not exist.";
        sys.stdout.flush();
        raise;
    #batchcounter=1;
    #stepcounter=1;
    while (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername) or firstlastrun) and (timeleft(starttime,controllerbuffertimelimit)>0):
        #oldqueryresultinds=[dict([(y,x[y]) for y in dbindexes]+[(newfield,{"$exists":True})]) for x in queryresult];
        #if len(oldqueryresultinds)==0:
        #    oldqueryresult=[];
        #else:
        #    oldqueryresult=toriccy.collectionfind(db,newcollection,{"$or":oldqueryresultinds},dict([("_id",0)]+[(y,1) for y in dbindexes]));
        #oldqueryresultrunning=[y for x in userjobsrunninglist(username,modname,controllername) for y in contractededjobname2jobdocs(x,dbindexes) if len(x)>0];
        #with open(querystatefile,"a") as iostream:
        #    for line in oldqueryresult+oldqueryresultrunning:
        #        iostream.write(str(dict([(x,line[x]) for x in allindexes if x in line.keys()])).replace(" ","")+"\n");
        #        iostream.flush();
        #print "Next Run";
        #print "";
        #sys.stdout.flush();

        reloadskippedjobs(modname,controllername,controllerpath,querystatefilename,basecollection);
        #print str(starttime)+" "+str(controllerbuffertimelimit);
        #sys.stdout.flush();
        [batchcounter,stepcounter]=toriccy.dbcrawl(db,queries,controllerpath,statefilename=querystatefilename,inputfunc=lambda x:doinput(x,nthreadsfield,username,maxjobcount,maxstepcount,controllerjobid,controllerbuffertimelimit,starttime,sleeptime,statusstatefile,partitions,scriptmemorylimit,softwarestatefile,modlist,modulesdirpath,scriptpath,scriptlanguage,licensestream),inputdoc=doinput([],nthreadsfield,username,maxjobcount,maxstepcount,controllerjobid,controllerbuffertimelimit,starttime,sleeptime,statusstatefile,partitions,scriptmemorylimit,softwarestatefile,modlist,modulesdirpath,scriptpath,scriptlanguage,licensestream),action=lambda w,x,y,z:doaction(w,x,y,z,username,modname,dbpushq,markdone,controllername,scripttimelimit,scriptbuffertime,writemode,mongouri,basecollection,dbindexes,controllerpath,workpath,scriptpath,scriptlanguage,scriptcommand,scriptflags,scriptext,licensestream,nthreadsfield),readform=lambda x:jobstepname2indexdoc(x,dbindexes),writeform=lambda x:indexdoc2jobstepname(x,modname,controllername,dbindexes),timeleft=lambda:timeleft(starttime,controllerbuffertimelimit),batchcounter=batchcounter,stepcounter=stepcounter,counterupdate=lambda x,y:docounterupdate(x,y,counterstatefile,counterheader),resetstatefile=False,toplevel=True);
        #firstrun=False;
        if (timeleft(starttime,controllerbuffertimelimit)>0):
            firstlastrun=(not (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername) or firstlastrun));
            releaseheldjobs(username,modname,controllername);

    #while userjobsrunningq(username,modname,controllername) and (timeleft(starttime,controllerbuffertimelimit)>0):
    #    releaseheldjobs(username,modname,controllername);
    #    skippedjobs=skippedjobslist(username,modname,controllername,workpath);
    #    for x in skippedjobs:
    #        nodemaxmemory=getnodemaxmemory(resourcesstatefile,controllerpartition);
    #        submitjob(workpath,x,controllerpartition,nodemaxmemory,nodemaxmemory,resubmit=True);
    #    time.sleep(sleeptime);

    if (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername) or firstlastrun) and not (timeleft(starttime,controllerbuffertimelimit)>0):
        #Resubmit controller job
        nodemaxmemory=getnodemaxmemory(resourcesstatefile,controllerpartition);
        submitcontrollerjob(controllerpath,"controller_"+modname+"_"+controllername,controllerpartition,nodemaxmemory,resubmit=True);
        with open(statusstatefile,"w") as statusstream:
            statusstream.truncate(0);
            statusstream.write("Resubmitting");
    else:
        #if pdffile!="":
        #    plotjobgraph(modname,controllerpath,controllername,workpath,pdffile);
        with open(statusstatefile,"w") as statusstream:
            statusstream.truncate(0);
            statusstream.write("Completing");

    #querystream.close();
    #seekstream.close();
    if needslicense:
        #fcntl.flock(pendlicensestream,fcntl.LOCK_UN);
        licensestream.close();
    mongoclient.close();

    print "";
    print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
    print "Completing job controller_"+modname+"_"+controllername;
    sys.stdout.flush();
except Exception as e:
    PrintException();