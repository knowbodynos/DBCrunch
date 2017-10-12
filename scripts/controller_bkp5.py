#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import time;
#Timer and maxjobcount initialization
starttime=time.time();

import sys,os,errno,re,linecache,signal,fcntl,traceback,operator,functools,subprocess,datetime,tempfile,json,mongolink;#,json;
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
    #return dict([(dbindexes[i],eval(indexsplit[i+2])) for i in range(nindexes)]);
    return dict([(dbindexes[i],eval(indexsplit[i+2]) if indexsplit[i+2].isdigit() else indexsplit[i+2]) for i in range(nindexes)]);

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
    #    formatteddoc=mongolink.pythondictionary2mathematicarules(doc);
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
    njobs=eval(subprocess.Popen("squeue -h -r -u "+username+" | grep -v \" CG \" | wc -l | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
    jobsleft=(njobs<maxjobcount);
    return jobsleft;

def clusterlicensesleft(nlicensesplit,minthreads):#,minnsteps=1):
    nlicenses=nlicensesplit[0];
    licensesleft=(nlicenses>0);#(navaillicenses>=minnsteps));
    if len(nlicensesplit)>1:
        nsublicenses=nlicensesplit[1];
        licensesleft=(licensesleft and (nsublicenses>=minthreads));
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

#def islimitreached(controllerpath,querylimit):
#    if querylimit==None:
#        return False;
#    else:
#        #if niters==1:
#        #    ntot=eval(subprocess.Popen("echo \"$(cat "+controllerpath+"/jobs/*.error 2>/dev/null | wc -l)+$(cat "+controllerpath+"/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
#        #elif niters>1:
#        ntot=eval(subprocess.Popen("echo \"$(cat $(find "+controllerpath+"/jobs/ -type f -name '*.docs' -o -name '*.docs.pend' 2>/dev/null) 2>/dev/null | wc -l)+$(cat "+controllerpath+"/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
#        return ntot>=querylimit;

def submitjob(jobpath,jobname,jobstepnames,nnodes,ncores,nthreads,partition,memoryperstep,maxmemorypernode,resubmit=False):
    submit=subprocess.Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
    submitcomm=submit.communicate()[0].rstrip("\n");
    #Print information about controller job submission
    if resubmit:
        #jobid=submitcomm.split(' ')[-1];
        #maketop=subprocess.Popen("scontrol top "+jobid,shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
        print "";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print "Res"+submitcomm[1:]+" as "+jobname+" on partition "+partition+" with "+str(nnodes)+" nodes, "+str(ncores)+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
        submitcomm=submitcomm.replace("Submitted batch job ","With job step ");
        for i in range(len(jobstepnames)):
            #with open(jobpath+"/"+jobstepnames[i]+".error","a") as statstream:
            #    statstream.write(jobstepnames[i]+",-1:0,False\n");
            #    statstream.flush();
            print "...."+submitcomm[1:]+"."+str(i)+" as "+jobstepnames[i]+" on partition "+partition+" with "+str(nthreads[i])+" CPU(s) and "+str(memoryperstep/1000000)+"MB RAM allocated.";
        print "";
        print "";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print submitcomm+" as "+jobname+" on partition "+partition+" with "+str(nnodes)+" nodes, "+str(ncores)+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
        submitcomm=submitcomm.replace("Submitted batch job ","With job step ");
        for i in range(len(jobstepnames)):
            #with open(jobpath+"/"+jobstepnames[i]+".error","a") as statstream:
            #    statstream.write(jobstepnames[i]+",-1:0,False\n");
            #    statstream.flush();
            print "...."+submitcomm+"."+str(i)+" as "+jobstepnames[i]+" on partition "+partition+" with "+str(nthreads[i])+" CPU(s) and "+str(memoryperstep/1000000)+"MB RAM allocated.";
        print "";
    sys.stdout.flush();

def submitcontrollerjob(jobpath,jobname,controllernnodes,controllerncores,partition,maxmemorypernode,resubmit=False):
    submit=subprocess.Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
    submitcomm=submit.communicate()[0].rstrip("\n");
    #Print information about controller job submission
    if resubmit:
        #jobid=submitcomm.split(' ')[-1];
        #maketop=subprocess.Popen("scontrol top "+jobid,shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
        print "";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print "Res"+submitcomm[1:]+" as "+jobname+" on partition "+partition+" with "+controllernnodes+" nodes, "+controllerncores+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
        print "";
        print "";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print submitcomm+" as "+jobname+" on partition "+partition+" with "+controllernnodes+" nodes, "+controllerncores+" CPU(s), and "+str(maxmemorypernode/1000000)+"MB RAM allocated.";
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

def reloadskippedjobs(modname,controllername,controllerpath,querystatefilename,basecollection,dbindexes):
    try:
        skippedjobs=[];
        with open(controllerpath+"/skippedstate","r") as skippedstream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream:
            skippedheader=skippedstream.readline();
            tempstream.write(skippedheader);
            tempstream.flush();
            for line in skippedstream:
                skippedjob,exitcode,resubmitq=line.rstrip("\n").split(",");
                if eval(resubmitq):
                    skippedjobsplit=skippedjob.split("_");
                    if skippedjobsplit[:2]==[modname,controllername]:
                        #if niters==1:
                        skippedjobs+=[skippedjob];
                        #elif niters>1:
                        #with open(controllerpath+"/jobs/"+skippedjob+".docs","r") as skippeddocstream:
                        #    for docsline in skippeddocstream:
                        #        doc=json.loads(docsline.rstrip("\n"));
                        #        skippedjobs+=[indexdoc2jobstepname(doc,modname,controllername,dbindexes)];
                        #os.remove(controllerpath+"/jobs/"+skippedjob+".docs");
                        #os.remove(controllerpath+"/jobs/"+skippedjob+".docs.in");
                        #os.remove(controllerpath+"/jobs/"+skippedjob+".error");
                        #os.remove(controllerpath+"/jobs/"+skippedjob+".batch.log");
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
                try:
                    if querystatetierfilename==querystatefilename+basecollection:
                        #with open(controllerpath+"/"+querystatetierfilename,"a") as querystatefilestream:
                        #    for i in range(len(skippedjobs)):
                        #        line=skippedjobs[i];
                        #        if i<len(skippedjobs)-1:
                        #            line+="\n";
                        #        querystatefilestream.write(line);
                        #        querystatefilestream.flush();
                        with open(controllerpath+"/"+querystatetierfilename,"r") as querystatefilestream, tempfile.NamedTemporaryFile(dir=controllerpath,delete=False) as tempstream:
                            for line in querystatefilestream:
                                linestrip=line.rstrip("\n");
                                if not linestrip in skippedjobs:
                                    tempstream.write(line);
                                    tempstream.flush();
                            os.rename(tempstream.name,querystatefilestream.name);
                    else:
                    #if querystatetierfilename!=querystatefilename+basecollection:
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
    subprocess.Popen("for job in $(squeue -h -u "+username+" -o '%j %A %r' | grep '^"+modname+"_"+controllername+"' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done",shell=True,preexec_fn=default_sigpipe);
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
    orderedpartitions=[x for x in mongolink.deldup(partitionsidle+partitionscomp+partitionsrun+partitionspend+partitions) if x!=""];
    return orderedpartitions;

def orderfreepartitions(partitions):
    greppartitions="|".join(partitions);
    partitionsidle=subprocess.Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    partitionscomp=subprocess.Popen("sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'comp' | awk '$0=$1\" \"$2*$3\" \"$4' | sort -k2,2nr | cut -d' ' -f3 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(',');
    orderedfreepartitions=[x for x in mongolink.deldup(partitionsidle+partitionscomp) if x!=""];
    return orderedfreepartitions;

def getmaxmemorypernode(resourcesstatefile,partition):
    maxmemorypernode=0;
    try:
        with open(resourcesstatefile,"r") as resourcesstream:
            resourcesheader=resourcesstream.readline();
            for resourcesstring in resourcesstream:
                resources=resourcesstring.rstrip("\n").split(",");
                if resources[0]==partition:
                    maxmemorypernode=eval(resources[1]);
                    break;
    except IOError:
        print "File path \""+resourcesstatefile+"\" does not exist.";
        sys.stdout.flush();
    return maxmemorypernode;

def distributeovernodes(resourcesstatefile,partition,scriptmemorylimit,nnodes,maxstepcount,niters):#,maxthreads):
    #print partitions;
    #sys.stdout.flush();
    #partition=partitions[0];
    ncoresperpartitionnode=eval(subprocess.Popen("sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0]);
    ncores=nnodes*ncoresperpartitionnode;
    maxnnodes=eval(subprocess.Popen("scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].rstrip("\n"));
    #print "distributeovernodes";
    #print "sinfo -h -p '"+partition+"' -o '%c' | head -n1 | head -c -1";
    #print "scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g' | head -c -1";
    #print "";
    #sys.stdout.flush();
    maxmemorypernode=getmaxmemorypernode(resourcesstatefile,partition);
    #tempnnodes=nnodes;
    #if scriptmemorylimit=="":
    #    nstepsdistribmempernode=float(maxsteps/tempnnodes);
    #    while (tempnnodes>1) and (nstepsdistribmempernode<1):
    #        tempnnodes-=1;
    #        nstepsdistribmempernode=float(maxsteps/tempnnodes);
    #else:
    if (scriptmemorylimit=="") or (eval(scriptmemorylimit)==maxmemorypernode):
        nsteps=min(nnodes,maxstepcount)*niters;
        #memoryperstep=maxmemorypernode;
    elif eval(scriptmemorylimit)>maxmemorypernode:
        return None;
    else:
        nstepsdistribmem=nnodes*maxmemorypernode/eval(scriptmemorylimit);
        #print "a: "+str(nstepsdistribmem);
        nsteps=min(ncores,maxstepcount,nstepsdistribmem)*niters;
        #print "b: "+str(nsteps);
        #memoryperstep=nnodes*maxmemorypernode/nsteps;
        #print "c: "+str(memoryperstep);

    #nstepsfloat=min(float(ndocsleft),float(ncoresperpartitionnode),nstepsdistribmempernode);
    #nnodes=int(min(maxnnodes,math.ceil(1./nstepsfloat)));
    #ncores=nnodes*ncoresperpartitionnode;
    #nsteps=int(max(1,nstepsfloat));
    #nnodes=1;
    #if nstepsdistribmempernode<1:
    #    if len(partitions)>1:
    #        return distributeovernodes(resourcesstatefile,partitions[1:],scriptmemorylimit,nnodes,maxsteps,maxthreads);
    #    else:
    #        print "Memory requirement is too large for this cluster.";
    #        sys.stdout.flush();
    #nnodes=tempnnodes;
    #nsteps=min(ncores,nstepsdistribmem,maxsteps);
    #nsteps=int(nstepsfloat);
    #if nsteps>0:
    #    memoryperstep=nnodes*maxmemorypernode/nsteps;
    #else:
    #    memoryperstep=maxmemorypernode;
    return [ncores,nsteps,maxmemorypernode];

def writejobfile(modname,dbpush,markdone,writestats,jobname,jobstepnames,controllerpath,controllername,writemode,partitiontimelimit,buffertimelimit,partition,nnodes,batchcounter,totmem,ncoresused,scriptlanguage,scriptcommand,scriptflags,scriptext,dbindexes,nthreadsfield,nbatch,nbatcheswrite,docbatches):
    ndocbatches=len(docbatches);
    outputlinemarkers=["-","+","&","@","CPUTime:","MaxRSS:","MaxVMSize:","BSONSize:","None"];
    scriptcommandflags=scriptcommand;
    if len(scriptflags)>0:
        scriptcommandflags+=" "+scriptflags;
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
    jobstring+="#SBATCH -o \""+jobname+".log\"\n";
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
    jobstring+="#SBATCH -n "+str(ncoresused)+"\n";
    jobstring+="#################\n";
    jobstring+="#Number of nodes to distribute n tasks across\n";
    jobstring+="#SBATCH -N "+str(nnodes)+"\n";
    jobstring+="#################\n";
    jobstring+="#Lock down N nodes for job\n";
    jobstring+="#SBATCH --exclusive\n";
    jobstring+="#################\n";
    jobstring+="\n";
    jobstring+="#Job info\n";
    jobstring+="modname=\""+modname+"\"\n";
    jobstring+="controllername=\""+controllername+"\"\n";
    jobstring+="outputlinemarkers=\""+str(outputlinemarkers).replace(" ","")+"\"\n";
    jobstring+="jobnum="+str(batchcounter)+"\n";
    jobstring+="nsteps="+str(ncoresused)+"\n";
    jobstring+="memunit=\"M\"\n";
    jobstring+="totmem="+str(totmem/1000000)+"\n";
    #jobstring+="stepmem=$((${totmem}/${nsteps}))\n";
    if buffertimelimit=="infinite":
        jobstring+="steptime=\"0\"\n";
    else:
        jobstring+="steptime=\""+buffertimelimit+"\"\n";
    jobstring+="nbatch="+str(nbatch)+"\n";
    jobstring+="nbatcheswrite="+str(min(nbatcheswrite,ncoresused))+"\n";
    jobstring+="\n";
    jobstring+="#Option info\n";
    jobstring+="dbpush=\""+dbpush+"\"\n";
    jobstring+="markdone=\""+markdone+"\"\n";
    jobstring+="writestats=\""+writestats+"\"\n";
    jobstring+="\n";
    jobstring+="#File system info\n";
    jobstring+="mainpath=\"${SLURMONGO_ROOT}\"\n";
    jobstring+="scriptpath=\"${mainpath}/scripts\"\n";
    jobstring+="\n";
    jobstring+="#Script info\n";
    jobstring+="scriptlanguage=\""+scriptlanguage+"\"\n";
    jobstring+="scriptcommand=\""+scriptcommand+"\"\n";
    jobstring+="scriptflags=\""+scriptflags+"\"\n";
    jobstring+="scriptext=\""+scriptext+"\"\n";
    jobstring+="\n";
    jobstring+="#Database info\n";
    jobstring+="dbindexes=\""+str([str(x) for x in dbindexes]).replace(" ","")+"\"\n";
    jobstring+="\n";
    jobstring+="#MPI info\n";
    for i in range(ndocbatches):
        with open(controllerpath+"/jobs/"+jobstepnames[i]+".docs","w") as docstream:
            for doc in docbatches[i]:
                docstream.write(json.dumps(doc,separators=(',',':'))+"\n");
                docstream.flush()
        if nthreadsfield=="":
            jobstring+="nstepthreads["+str(i)+"]=1\n";
        else:
            jobstring+="nstepthreads["+str(i)+"]="+str(max([x[nthreadsfield] for x in docbatches[i]]))+"\n";
    jobstring+="\n";
    jobstring+="python \"${scriptpath}/jobmanager.py\" \"${modname}\" \"${controllername}\" \"${scriptlanguage}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nbatcheswrite}\" \"${dbpush}\" \"${markdone}\" \"${writestats}\" \"${mainpath}\" \"${dbindexes}\" \"${nstepthreads[@]}\"";
    jobstream=open(controllerpath+"/jobs/"+jobname+".job","w");
    jobstream.write(jobstring);
    jobstream.flush();
    jobstream.close();

def waitforslots(licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes):
    needslicense=(licensestream!=None);
    if needslicense:
        jobslotsleft=clusterjobslotsleft(username,maxjobcount);
        nlicensesplit=licensecount(username,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage);
        licensesleft=clusterlicensesleft(nlicensesplit,maxthreads);
        orderedfreepartitions=orderfreepartitions(partitions);
        releaseheldjobs(username,modname,controllername);
        if (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and licensesleft and (len(orderedfreepartitions)>0)):
            with open(statusstatefile,"w") as statusstream:
                statusstream.truncate(0);
                statusstream.write("Waiting");
                statusstream.flush();
            while (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and licensesleft and (len(orderedfreepartitions)>0)):
                time.sleep(sleeptime);
                jobslotsleft=clusterjobslotsleft(username,maxjobcount);
                nlicensesplit=licensecount(username,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage);
                licensesleft=clusterlicensesleft(nlicensesplit,maxthreads);
                orderedfreepartitions=orderfreepartitions(partitions);
                releaseheldjobs(username,modname,controllername);
        #fcntl.flock(licensestream,fcntl.LOCK_EX);
        #fcntl.LOCK_EX might only work on files opened for writing. This one is open as "a+", so instead use bitwise OR with non-blocking and loop until lock is acquired.
        while (timeleft(starttime,controllerbuffertimelimit)>0):
            releaseheldjobs(username,modname,controllername);
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
            return None;
        licensestream.seek(0,0);
        licenseheader=licensestream.readline();
        #print licenseheader;
        #sys.stdout.flush();
        licensestream.truncate(0);
        licensestream.seek(0,0);
        licensestream.write(licenseheader);
        licensestream.write(','.join([str(x) for x in nlicensesplit]));
        licensestream.flush();
    else:
        jobslotsleft=clusterjobslotsleft(username,maxjobcount);
        orderedfreepartitions=orderfreepartitions(partitions);
        releaseheldjobs(username,modname,controllername);
        if (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and (len(orderedfreepartitions)>0)):
            with open(statusstatefile,"w") as statusstream:
                statusstream.truncate(0);
                statusstream.write("Waiting");
                statusstream.flush();
            while (timeleft(starttime,controllerbuffertimelimit)>0) and not (jobslotsleft and (len(orderedfreepartitions)>0)):
                time.sleep(sleeptime);
                jobslotsleft=clusterjobslotsleft(username,maxjobcount);
                orderedfreepartitions=orderfreepartitions(partitions);
                releaseheldjobs(username,modname,controllername);
        if not (timeleft(starttime,controllerbuffertimelimit)>0):
            return None;

    reloadskippedjobs(modname,controllername,controllerpath,querystatefilename,basecollection,dbindexes);

    return orderedfreepartitions;

def doinput(docbatch,nthreadsfield,licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,resourcesstatefile,scriptmemorylimit,maxstepcount,dbindexes,niters):
    if (len(docbatch)>0) and (nthreadsfield!=""):
        maxthreads=max([x[nthreadsfield] for x in docbatch[0:niters]]);
    else:
        maxthreads=1;
    orderedfreepartitions=waitforslots(licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes);
    if orderedfreepartitions==None:
        return None;
    #orderedfreepartitions=orderpartitions(partitions);
    nnodes=1;
    i=0;
    while i<len(orderedfreepartitions):
        partition=orderedfreepartitions[i];
        #nnodes=1;
        distribution=distributeovernodes(resourcesstatefile,partition,scriptmemorylimit,nnodes,maxstepcount,niters);
        if distribution==None:
            i+=1;
        else:
            break;
    if i==len(orderedfreepartitions):
        raise Exception("Memory requirement is too large for this cluster.");
    ncores,nsteps,maxmemorypernode=distribution;
    if len(docbatch)>0:
        maxthreads=0;
        nextdocind=0;
        if nthreadsfield!="":
            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
                maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
                nextdocind+=niters;
            if maxthreads>ncores:
                nextdocind-=niters;
                maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
        else:
            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
                maxthreads+=1;
                nextdocind+=niters;
            if maxthreads>ncores:
                nextdocind-=niters;
                maxthreads-=1;
        while maxthreads==0:
            nnodes+=1;
            orderedfreepartitions=waitforslots(licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes);
            if orderedfreepartitions==None:
                return None;
            #orderedfreepartitions=orderpartitions(partitions);
            i=0;
            while i<len(orderedfreepartitions):
                partition=orderedfreepartitions[i];
                #nnodes=1;
                distribution=distributeovernodes(resourcesstatefile,partition,scriptmemorylimit,nnodes,maxstepcount,niters);
                if distribution==None:
                    i+=1;
                else:
                    break;
            if i==len(orderedfreepartitions):
                raise Exception("Memory requirement is too large for this cluster.");
            ncores,nsteps,maxmemorypernode=distribution;
            maxthreads=0;
            nextdocind=0;
            if nthreadsfield!="":
                while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
                    maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
                    nextdocind+=niters;
                if maxthreads>ncores:
                    nextdocind-=niters;
                    maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
            else:
                while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
                    maxthreads+=1;
                    nextdocind+=niters;
                if maxthreads>ncores:
                    nextdocind-=niters;
                    maxthreads-=1;
        #nsteps=0;
    #print {"partition":partition,"nnodes":nnodes,"ncores":ncores,"nsteps":nsteps,"maxmemorypernode":maxmemorypernode};
    #sys.stdout.flush();
    return {"partition":partition,"nnodes":nnodes,"ncores":ncores,"nsteps":nsteps,"maxmemorypernode":maxmemorypernode};

def doaction(batchcounter,stepcounter,inputdoc,docbatch,nthreadsfield,licensestream,username,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,resourcesstatefile,scriptmemorylimit,maxstepcount,modname,controllername,dbindexes,dbpush,markdone,writestats,controllerpath,writemode,mongouri,scriptcommand,scriptflags,scriptext,querystatefilename,basecollection,niters,nbatch,nbatcheswrite):
    partition=inputdoc['partition'];
    nnodes=inputdoc['nnodes'];
    ncores=inputdoc['ncores'];
    maxmemorypernode=inputdoc["maxmemorypernode"];
    maxthreads=0;
    nextdocind=0;
    if nthreadsfield!="":
        while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
            maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
            nextdocind+=niters;
        if maxthreads>ncores:
            nextdocind-=niters;
            maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
    else:
        while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
            maxthreads+=1;
            nextdocind+=niters;
        if maxthreads>ncores:
            nextdocind-=niters;
            maxthreads-=1;
    while maxthreads==0:
        nnodes+=1;
        orderedfreepartitions=waitforslots(licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,maxthreads,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,dbindexes);
        if orderedfreepartitions==None:
            return None;
        #orderedfreepartitions=orderpartitions(partitions);
        i=0;
        while i<len(orderedfreepartitions):
            partition=orderedfreepartitions[i];
            #nnodes=1;
            distribution=distributeovernodes(resourcesstatefile,partition,scriptmemorylimit,nnodes,maxstepcount,niters);
            if distribution==None:
                i+=1;
            else:
                break;
        if i==len(orderedfreepartitions):
            raise Exception("Memory requirement is too large for this cluster.");
        ncores,nsteps,maxmemorypernode=distribution;
        maxthreads=0;
        nextdocind=0;
        if nthreadsfield!="":
            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
                maxthreads+=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
                nextdocind+=niters;
            if maxthreads>ncores:
                nextdocind-=niters;
                maxthreads-=max([x[nthreadsfield] for x in docbatch[nextdocind:nextdocind+niters]]);
        else:
            while (nextdocind<len(docbatch)) and (maxthreads<=ncores):
                maxthreads+=1;
                nextdocind+=niters;
            if maxthreads>ncores:
                nextdocind-=niters;
                maxthreads-=1;
    #docbatchwrite=docbatch[:nextdocind];
    docbatchwrite=[docbatch[i:i+niters] for i in range(0,nextdocind,niters)];
    #docbatchpass=docbatch[nextdocind:];

    totmem=nnodes*maxmemorypernode;
    ncoresused=len(docbatchwrite);
    memoryperstep=totmem/ncoresused;
    #ndocs=len(docbatch);
    #if nthreadsfield=="":
    #    totnthreadsfield=ndocs;
    #else:
    #    totnthreadsfield=sum([x[nthreadsfield] for x in docbatch]);
    #while not clusterjobslotsleft(maxjobcount,scriptext,minnsteps=inputdoc["nsteps"]):
    #    time.sleep(sleeptime);
    #doc=json.loads(doc.rstrip('\n'));
    #if niters==1:
    #    jobstepnames=[indexdoc2jobstepname(x,modname,controllername,dbindexes) for x in docbatchwrite];
    #else:
    jobstepnames=[modname+"_"+controllername+"_job_"+str(batchcounter)+"_step_"+str(i+1) for i in range(len(docbatchwrite))];
    #jobstepnamescontract=jobstepnamescontract(jobstepnames);
    jobname=modname+"_"+controllername+"_job_"+str(batchcounter)+"_steps_"+str((stepcounter+niters-1)/niters)+"-"+str((stepcounter+niters*len(docbatchwrite)-1)/niters);
    partitiontimelimit,buffertimelimit=getpartitiontimelimit(partition,scripttimelimit,scriptbuffertime);
    #if len(docbatch)<inputdoc["nsteps"]:
    #    inputdoc["memoryperstep"]=(memoryperstep*inputdoc["nsteps"])/len(docbatch);
    writejobfile(modname,dbpush,markdone,writestats,jobname,jobstepnames,controllerpath,controllername,writemode,partitiontimelimit,buffertimelimit,partition,nnodes,batchcounter,totmem,ncoresused,scriptlanguage,scriptcommand,scriptflags,scriptext,dbindexes,nthreadsfield,nbatch,nbatcheswrite,docbatchwrite);
    #Submit job file
    if nthreadsfield!="":
        nthreads=[max([y[nthreadsfield] for y in x]) for x in docbatchwrite];
    else:
        nthreads=[1 for x in docbatchwrite];
    submitjob(workpath,jobname,jobstepnames,nnodes,ncores,nthreads,partition,memoryperstep,maxmemorypernode,resubmit=False);
    needslicense=(licensestream!=None);
    if needslicense:
        fcntl.flock(licensestream,fcntl.LOCK_UN);
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
    with open(statusstatefile,"w") as statusstream:
        statusstream.truncate(0);
        statusstream.write("Running");
        statusstream.flush();
    #releaseheldjobs(username,modname,controllername);
    #print "End action";
    #sys.stdout.flush();
    #seekstream.write(querystream.tell());
    #seekstream.flush();
    #seekstream.seek(0);
    #doc=querystream.readline();
    releaseheldjobs(username,modname,controllername);
    return nextdocind;#docbatchpass;

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
    username,packagepath=subprocess.Popen("echo \"${USER},${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");

    #Input controller info
    modname=sys.argv[1];
    controllername=sys.argv[2];
    controllerjobid=sys.argv[3];
    #controllerpartition=sys.argv[4];
    controllerbuffertime=sys.argv[4];
    #largemempartitions=sys.argv[6].split(",");
    sleeptime=eval(sys.argv[5]);

    #seekfile=sys.argv[7]; 

    #Input path info
    #packagepath=sys.argv[7];
    #packagepath=sys.argv[8];
    #scriptpath=sys.argv[9];

    #Input script info
    scriptlanguage=sys.argv[6];
    partitions=sys.argv[7].split(",");
    writemode=sys.argv[8];
    #scripttimelimit=timestamp2unit(sys.argv[15]);
    scriptmemorylimit=sys.argv[9];
    scripttimelimit=sys.argv[10];
    scriptbuffertime=sys.argv[11];
    #outputlinemarkers=sys.argv[15].split(",");

    #Input database info
    dbname=sys.argv[12];
    dbusername=sys.argv[13];
    dbpassword=sys.argv[14];
    queries=eval(sys.argv[15]);
    #dumpfile=sys.argv[13];
    basecollection=sys.argv[16];
    nthreadsfield=sys.argv[17];
    #newcollection,newfield=sys.argv[18].split(",");

    #Options
    blocking=eval(sys.argv[18]);
    dbpush=sys.argv[19];
    markdone=sys.argv[20];
    writestats=sys.argv[21];
    niters=eval(sys.argv[22]);
    nbatch=eval(sys.argv[23]);
    nbatcheswrite=eval(sys.argv[24]);
    
    #Read seek position from file
    #with open(controllerpath+"/"+seekfile,"r") as seekstream:
    #    seekpos=eval(seekstream.read());

    #Open seek file stream
    #seekstream=open(controllerpath+"/"+seekfile,"w");

    #If first submission, read from database
    #if seekpos==-1:
    #Open connection to remote database
    #sys.stdout.flush();
    controllerstats=subprocess.Popen("sacct -n -j \""+controllerjobid+"\" -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
    while len(controllerstats)<4:
        time.sleep(sleeptime);
        controllerstats=subprocess.Popen("sacct -n -j \""+controllerjobid+"\" -o 'Partition%30,Timelimit,NNodes,NCPUs' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0].split(",");
    controllerpartition,controllertimelimit,controllernnodes,controllerncores=controllerstats;

    print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
    print "Starting job controller_"+modname+"_"+controllername;
    print "";
    print "";
    sys.stdout.flush();

    statepath=packagepath+"/state";
    modulesdirpath=packagepath+"/modules";
    scriptpath=packagepath+"/scripts";
    modulepath=modulesdirpath+"/"+modname;
    controllerpath=modulepath+"/"+controllername;
    workpath=controllerpath+"/jobs";  
    resourcesstatefile=statepath+"/resources";
    modulesstatefile=statepath+"/modules";
    softwarestatefile=statepath+"/software";
    mongourifile=statepath+"/mongouri";
    custommaxjobsfile=statepath+"/maxjobs";
    counterstatefile=controllerpath+"/counterstate";
    statusstatefile=controllerpath+"/statusstate";
    querystatefilename="querystate";
    #querystatefile=controllerpath+"/querystate";

    with open(statusstatefile,"w") as statusstream:
        statusstream.truncate(0);
        statusstream.write("Starting");

    with open(custommaxjobsfile,"r") as custommaxjobsstream:
        try:
            custommaxjobsheader=custommaxjobsstream.readline();
            custommaxjobcountstr,custommaxstepcountstr=custommaxjobsstream.readline().rstrip("\n").split(",");
            if custommaxjobcountstr!="":
                maxjobcount=min(maxjobcount,eval(custommaxjobcountstr));
            if custommaxstepcountstr!="":
                maxstepcount=min(maxstepcount,eval(custommaxstepcountstr));
        except IOError:
            print "Max jobs defined in \""+custommaxjobsfile+"\" must be a number.";
            sys.stdout.flush();
            raise;

    if scriptbuffertime=="":
        scriptbuffertime="00:00:00";

    controllerpartitiontimelimit,controllerbuffertimelimit=getpartitiontimelimit(controllerpartition,controllertimelimit,controllerbuffertime);

    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n").replace("mongodb://","mongodb://"+dbusername+":"+dbpassword+"@");
    
    mongoclient=mongolink.MongoClient(mongouri+dbname+"?authMechanism=SCRAM-SHA-1");
    #dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=mongolink.getintersectionindexes(db,basecollection);
    #print dbindexes;
    #sys.stdout.flush();

    allindexes=mongolink.getunionindexes(db);
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

    if blocking:
        while prevcontrollersrunningq(username,prevmodlist,controllername) and (timeleft(starttime,controllerbuffertimelimit)>0):
            time.sleep(sleeptime);

    querylimitlist=[x[3]['LIMIT'] for x in queries if (len(x)>3) and ("LIMIT" in x[3].keys())];
    if len(querylimitlist)>0:
        querylimit=functools.reduce(operator.mul,querylimitlist,1);
    else:
        querylimit=None;
    
    firstlastrun=(not (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername)));
    #batchcounter=1;
    #stepcounter=1;
    while (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername) or firstlastrun) and ((querylimit==None) or (stepcounter<=querylimit+1)) and (timeleft(starttime,controllerbuffertimelimit)>0):
        #oldqueryresultinds=[dict([(y,x[y]) for y in dbindexes]+[(newfield,{"$exists":True})]) for x in queryresult];
        #if len(oldqueryresultinds)==0:
        #    oldqueryresult=[];
        #else:
        #    oldqueryresult=mongolink.collectionfind(db,newcollection,{"$or":oldqueryresultinds},dict([("_id",0)]+[(y,1) for y in dbindexes]));
        #oldqueryresultrunning=[y for x in userjobsrunninglist(username,modname,controllername) for y in contractededjobname2jobdocs(x,dbindexes) if len(x)>0];
        #with open(querystatefile,"a") as iostream:
        #    for line in oldqueryresult+oldqueryresultrunning:
        #        iostream.write(str(dict([(x,line[x]) for x in allindexes if x in line.keys()])).replace(" ","")+"\n");
        #        iostream.flush();
        #print "Next Run";
        #print "hi";
        #sys.stdout.flush();
        reloadskippedjobs(modname,controllername,controllerpath,querystatefilename,basecollection,dbindexes);
        #print str(starttime)+" "+str(controllerbuffertimelimit);
        #sys.stdout.flush();
        #if not islimitreached(controllerpath,querylimit):
        if (querylimit==None) or (stepcounter<=querylimit):
            [batchcounter,stepcounter]=mongolink.dbcrawl(db,queries,controllerpath,statefilename=querystatefilename,inputfunc=lambda x:doinput(x,nthreadsfield,licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,resourcesstatefile,scriptmemorylimit,maxstepcount,dbindexes,niters),inputdoc=doinput([],nthreadsfield,licensestream,username,modname,controllername,controllerpath,querystatefilename,basecollection,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,resourcesstatefile,scriptmemorylimit,maxstepcount,dbindexes,niters),action=lambda w,x,y,z:doaction(w,x,y,z,nthreadsfield,licensestream,username,maxjobcount,modlist,modulesdirpath,softwarestatefile,scriptpath,scriptlanguage,starttime,controllerbuffertimelimit,statusstatefile,sleeptime,partitions,resourcesstatefile,scriptmemorylimit,maxstepcount,modname,controllername,dbindexes,dbpush,markdone,writestats,controllerpath,writemode,mongouri,scriptcommand,scriptflags,scriptext,querystatefilename,basecollection,niters,nbatch,nbatcheswrite),readform=lambda x:jobstepname2indexdoc(x,dbindexes),writeform=lambda x:indexdoc2jobstepname(x,modname,controllername,dbindexes),timeleft=lambda:timeleft(starttime,controllerbuffertimelimit),batchcounter=batchcounter,stepcounter=stepcounter,counterupdate=lambda x,y:docounterupdate(x,y,counterstatefile,counterheader),resetstatefile=False,toplevel=True);
        #print "bye";
        #firstrun=False;
        releaseheldjobs(username,modname,controllername);
        if (timeleft(starttime,controllerbuffertimelimit)>0):
            firstlastrun=(not (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername) or firstlastrun));
    #while userjobsrunningq(username,modname,controllername) and (timeleft(starttime,controllerbuffertimelimit)>0):
    #    releaseheldjobs(username,modname,controllername);
    #    skippedjobs=skippedjobslist(username,modname,controllername,workpath);
    #    for x in skippedjobs:
    #        maxmemorypernode=getmaxmemorypernode(resourcesstatefile,controllerpartition);
    #        submitjob(workpath,x,controllerpartition,maxmemorypernode,maxmemorypernode,resubmit=True);
    #    time.sleep(sleeptime);

    if (prevcontrollersrunningq(username,prevmodlist,controllername) or userjobsrunningq(username,modname,controllername) or firstlastrun) and not (timeleft(starttime,controllerbuffertimelimit)>0):
        #Resubmit controller job
        maxmemorypernode=getmaxmemorypernode(resourcesstatefile,controllerpartition);
        submitcontrollerjob(controllerpath,"controller_"+modname+"_"+controllername,controllernnodes,controllerncores,controllerpartition,maxmemorypernode,resubmit=True);
        with open(statusstatefile,"w") as statusstream:
            statusstream.truncate(0);
            statusstream.write("Resubmitting");
    else:
        #if pdffile!="":
        #    plotjobgraph(modname,controllerpath,controllername,workpath,pdffile);
        with open(statusstatefile,"w") as statusstream:
            statusstream.truncate(0);
            statusstream.write("Completing");
        print "";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print "Completing job controller_"+modname+"_"+controllername+"\n";
        sys.stdout.flush();

    #querystream.close();
    #seekstream.close();
    if needslicense:
        #fcntl.flock(pendlicensestream,fcntl.LOCK_UN);
        licensestream.close();
    mongoclient.close();
except Exception as e:
    PrintException();