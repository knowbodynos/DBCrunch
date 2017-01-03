#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,subprocess,time,datetime,toriccy;#,json;
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

def jobname2jobjson(jobname,dbindexes):
    indexsplit=[[eval(y) for y in x.split("_")] for x in jobname.lstrip("[").rstrip("]").split(",")];
    return [dict([(dbindexes[i],x[i]) for i in range(len(dbindexes))]) for x in indexsplit];

def doc2jobname(doc,dbindexes):
    return '_'.join([str(doc[x]) for x in dbindexes]);

def doc2jobjson(doc,dbindexes):
    return dict([(y,doc[y]) for y in dbindexes]);

def jobnameexpand(jobname):
    bracketexpanded=jobname.rstrip("]").split("[");
    return [bracketexpanded[0]+x for x in bracketexpanded[1].split(",")];

def jobstepnamescontract(jobstepnames):
    "3 because modname,primername are first two."
    bracketcontracted=[x.split("_") for x in jobstepnames];
    return '_'.join(bracketcontracted[0][:-3]+["["])+','.join(['_'.join(x[-3:]) for x in bracketcontracted])+"]";

def getpartitiontimelimit(partition,SLURMtimelimit,buffertime):
    maxtimelimit=subprocess.Popen("sinfo -h -o '%l %P' | grep -E '"+partition+"\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1",shell=True,stdout=subprocess.PIPE).communicate()[0];
    if SLURMtimelimit in ["","infinite"]:
        timelimit=maxtimelimit;
    else:
        if maxtimelimit=="infinite":
            timelimit=SLURMtimelimit;
        else:
            timelimit=min(maxtimelimit,SLURMtimelimit,key=timestamp2unit);
    if timelimit=="infinite":
        buffertimelimit=timelimit;
    else:
        buffertimelimit=timestamp2unit(timelimit)-timestamp2unit(buffertime);
    return [timelimit,buffertimelimit];

def timeleftq(starttime,buffertimelimit):
    "Determine if runtime limit has been reached."
    if buffertimelimit=="infinite":
        return True;
    else:
        return (time.time()-starttime)<buffertimelimit;

#def clusterjobslotsleft(maxjobcount):
#    njobs=eval(subprocess.Popen("squeue -h -r | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
#    return njobs<maxjobcount;

def clusterjobslotsleft(maxjobcount):
    njobs=eval(subprocess.Popen("squeue -h -r -u altman.ro | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    return njobs<maxjobcount;

def userjobsrunningq(username,modname,primername):
    njobsrunning=eval(subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+primername+"' | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    return njobsrunning>0;

def prevprimersrunningq(username,modlist,primername):
    if len(modlist)==0:
        njobsrunning=0;
    else:
        grepstr="\|".join(["^primer_"+x+"_"+primername for x in modlist]);
        njobsrunning=eval(subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '"+grepstr+"' | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    return njobsrunning>0;

def userjobsrunninglist(username,modname,primername):
    jobsrunningstring=subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+primername+"' | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0];
    if jobsrunningstring=='':
        return [];
    else:
        jobsrunning=jobsrunningstring.split(",");
        return jobsrunning;

def submitjob(jobpath,jobname,partition,memory,resubmit=False):
    submit=subprocess.Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=subprocess.PIPE);
    submitcomm=submit.communicate()[0].rstrip("\n");
    #Print information about primer job submission
    if resubmit:
        #jobid=submitcomm.split(' ')[-1];
        #maketop=subprocess.Popen("scontrol top "+jobid,shell=True,stdout=subprocess.PIPE);
        print "\n";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        if "primer" in jobname:
            print "Res"+submitcomm[1:]+" as "+jobname+" on partition "+partition+" with "+str(memory/1000000)+"MB RAM allocated.";
        else:
            jobstepnames=jobnameexpand(jobname);
            submitcomm=submitcomm.replace("job ","job step ");
            for i in range(len(jobstepnames)):
                print "Res"+submitcomm[1:]+"."+str(i)+" as "+jobstepnames[i]+" on partition "+partition+" with "+str(memory/1000000)+"MB RAM allocated.";
        print "\n\n";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        if "primer" in jobname:
            print submitcomm+" as "+jobname+" on partition "+partition+" with "+str(memory/1000000)+"MB RAM allocated.";
        else:
            jobstepnames=jobnameexpand(jobname);
            submitcomm=submitcomm.replace("job ","job step ");
            for i in range(len(jobstepnames)):
                print submitcomm+"."+str(i)+" as "+jobstepnames[i]+" on partition "+partition+" with "+str(memory/1000000)+"MB RAM allocated.";
        print "\n";
    sys.stdout.flush();

#def skippedjobslist(username,modname,primername,workpath):
#    jobsrunning=userjobsrunninglist(username,modname,primername);
#    blankfilesstring=subprocess.Popen("find '"+workpath+"' -maxdepth 1 -type f -name '*.out' -empty | rev | cut -d'/' -f1 | rev | cut -d'.' -f1 | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0];
#    if blankfilesstring=='':
#        return [];
#    else:
#        blankfiles=blankfilesstring.split(",");
#        skippedjobs=[modname+"_"+primername+"_"+x for x in blankfiles if x not in jobsrunning];
#        return skippedjobs;

def skippedjobslist(username,modname,primername,primerpath):
    with open(primerpath+"/skipped","r") as skippedstream:
        skippedjobs=[];
        for skippedjob in skippedstream:
            skippedjobsplit=skippedjob.rstrip("\n").split("_");
            if skippedjobsplit[:2]==[modname,primername]:
                skippedjobs+=['_'.join(skippedjobsplit[2:])];
    return skippedjobs;

def releaseheldjobs(username,modname,primername):
    subprocess.Popen("for job in $(squeue -h -u "+username+" -o '%A %j %r' | grep '^"+modname+"_"+primername+"' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done",shell=True);

def orderpartitions(partitions):
    greppartitions="|".join(partitions);
    partitionsidle=subprocess.Popen("for rawpart in $(sinfo -h -o '%t %c %D %P' | grep -E '("+greppartitions+")\*?\s*$' | grep 'idle' | cut -d' ' -f2,3,4 | sed 's/\*//g' | sed 's/\s/,/g'); do echo $(echo $rawpart | cut -d',' -f1,2 | sed 's/,/*/g' | bc),$(echo $rawpart | cut -d',' -f3); done | tr ' ' '\n' | sort -t',' -k1 -n | cut -d',' -f2 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    #partsmix=subprocess.Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'mix' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    #partsalloc=subprocess.Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'alloc' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    #partslist=','.join(partsidle+[x for x in partsmix if x not in partsidle]+[x for x in partsalloc if x not in partsidle+partsmix]);
    partitionscomp=subprocess.Popen("squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'COMPLETING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    partitionsrun=subprocess.Popen("squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'RUNNING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    partitionspend=subprocess.Popen("squeue -h -o '%P %T %L' | grep -E '("+greppartitions+")\*?\s*$' | grep 'PENDING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    orderedpartitions=[x for x in toriccy.deldup(partitionsidle+partitionscomp+partitionsrun+partitionspend) if x!=""];
    return orderedpartitions;

def getnodemaxmemory(statepath,partition):
    nodemaxmemory=0;
    with open(statepath+"/resources","r") as resourcesstream:
        for resourcesstring in resourcesstream:
            resources=resourcesstring.rstrip("\n").split(" ");
            if resources[0]==partition:
                nodemaxmemory=eval(resources[1]);
                break;
    return nodemaxmemory;

def distributeovernodes(statepath,partitions,ndocsleft,scriptmemorylimit,maxstepcount):
    partition=partitions[0];
    ncoresperpartition=eval(subprocess.Popen("sinfo -h -p '"+partition+"' -o '%c' | head -n1",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    maxnnodes=eval(subprocess.Popen("scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g'",shell=True,stdout=subprocess.PIPE).communicate()[0].rstrip("\n"));
    nodemaxmemory=getnodemaxmemory(statepath,partition);
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
            return distributeovernodes(statepath,partitions[1:],ndocsleft,scriptmemorylimit);
        else:
            print "Memory requirement is too large for this cluster.";
            sys.stdout.flush();
            return "Error";
    else:
        nstepsfloat=min(ndocsleft,ncoresperpartition,nstepsdistribmem,maxstepcount);
        ncores=nnodes*ncoresperpartition;
        nsteps=nstepsfloat;
        memoryperstep=nodemaxmemory/nstepsdistribmem;
        return [partition,nnodes,ncores,nsteps,memoryperstep];

def writejobfile(modname,jobname,primerpath,primername,writemode,partitiontimelimit,partition,nnodes,ncores,memoryperstep,mongouri,scriptpath,scripttype,scriptext,buffertimelimit,dbcoll,dbindexes,docs):
    ndocs=len(docs);
    jobstepnames=jobnameexpand(jobname);
    jobstring="#!/bin/bash\n";
    jobstring+="\n";
    jobstring+="#Created "+str(datetime.datetime.now().strftime("%Y %m %d %H:%M:%S"))+"\n";
    jobstring+="\n";
    jobstring+="#Job name\n";
    jobstring+="#SBATCH -J \""+jobname+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Working directory\n";
    jobstring+="#SBATCH -D \""+primerpath+"/jobs\"\n";
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
    jobstring+="dbcoll=\""+dbcoll+"\"\n";
    jobstring+="\n";
    jobstring+="#Cluster info\n";
    jobstring+="scriptpath=\""+scriptpath+"\"\n";
    jobstring+="primerpath=\""+primerpath+"\"\n";
    jobstring+="workpath=\"${primerpath}/jobs\"\n";
    jobstring+="\n";
    jobstring+="#Job info\n";
    jobstring+="modname=\""+modname+"\"\n";
    #jobstring+="scripttimelimit=\""+str(scripttimelimit)+"\"\n";
    #jobstring+="scriptmemorylimit=\""+str(memoryperstep)+"\"\n";
    #jobstring+="skippedfile=\"${primerpath}/skipped\"\n";
    for i in range(ndocs):
        jobstring+="jobstepnames["+str(i)+"]=\""+jobstepnames[i]+"\"\n";
        jobstring+="newindexes["+str(i)+"]=\""+str(dict([(x,docs[i][x]) for x in dbindexes]))+"\"\n";
        if scriptext==".m":
            jobstring+="docs["+str(i)+"]=\""+str(toriccy.pythondictionary2mathematicarules(docs[i]))+"\"\n";
        else:
            jobstring+="docs["+str(i)+"]=\""+str(docs[i])+"\"\n";
        jobstring+="\n";
    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    #jobstring+="    srun -N 1 -n 1 --exclusive -J \"${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" "+scripttype+" \"${scriptpath}/"+modname+scriptext+"\" \"${workpath}\" \"${jobstepnames[${i}]}\" \"${mongouri}\" \"${scripttimelimit}\" \"${scriptmemorylimit}\" \"${skippedfile}\" \"${docs[${i}]}\" > \"${workpath}/${jobstepnames[${i}]}.log\" &\n";
    jobstring+="    srun -N 1 -n 1 --exclusive -J \"${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" --time=\""+buffertimelimit+"\" "+scripttype+" \"${scriptpath}/"+modname+scriptext+"\" \"${workpath}\" \"${jobstepnames[${i}]}\" \"${mongouri}\" \"${docs[${i}]}\" > \"${workpath}/${jobstepnames[${i}]}.log\" &\n";
    jobstring+="    pids[${i}]=$!\n";
    jobstring+="done\n";
    jobstring+="\n";
    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    jobstring+="    wait ${pids[${i}]}\n";
    #jobstring+="    stats=($(sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j ${SLURM_JOBID}.${i} | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g'))\n"
    #jobstring+="    echo \"CPUTime: ${stats[0]}\" >> ${jobstepnames[${i}]}.log\n"
    #jobstring+="    echo \"MaxRSS: ${stats[1]}\" >> ${jobstepnames[${i}]}.log\n"
    #jobstring+="    echo \"MaxVMSize: ${stats[2]}\" >> ${jobstepnames[${i}]}.log\n"
    jobstring+="    if test -s \"${workpath}/${jobstepnames[${i}]}.log\"\n";
    jobstring+="    then\n";
    jobstring+="        srun -N 1 -n 1 --exclusive -J \"stats_${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperstep/1000000)+"M\" python \"${scriptpath}/stats.py\" \"${mongouri}\" \"${modname}\" \"${SLURM_JOBID}.${i}\" \"${dbcoll}\" \"${newindexes[${i}]}\" \"${workpath}/${jobstepnames[${i}]}.log\" >> \"${workpath}/${jobstepnames[${i}]}.log\" &\n";# > ${workpath}/${jobname}.log\n";
    jobstring+="    else\n";
    jobstring+="        echo \"${jobstepnames[${i}]}\" >> \"${primerpath}/skipped\"; sacct -j \"${SLURM_JOBID}.${i}\" -o 'State,ExitCode,DerivedExitCode' >> \"${workpath}/${jobstepnames[${i}]}.log\" &\n";
    jobstring+="    fi\n";
    jobstring+="    pids[${i}]=$!\n";
    jobstring+="done\n";
    jobstring+="\n";
    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    jobstring+="    wait ${pids[${i}]}\n";
    jobstring+="done";
    jobstream=open(primerpath+"/jobs/"+jobname+".job","w");
    jobstream.write(jobstring);
    jobstream.flush();
    jobstream.close();

try:
    #Timer and maxjobcount initialization
    starttime=time.time();
    maxjobcount,maxstepcount=[eval(x) for x in subprocess.Popen("scontrol show config | grep 'MaxJobCount\|MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(",")];

    #Cluster info
    username=sys.argv[1];

    #Input primer info
    modname=sys.argv[2];
    primername=sys.argv[3];
    primerpartition=sys.argv[4];
    partitions=sys.argv[5].split(",");
    largemempartitions=sys.argv[6].split(",");
    writemode=sys.argv[7];
    SLURMtimelimit=sys.argv[8];
    buffertime=sys.argv[9];
    sleeptime=timestamp2unit(sys.argv[10]);

    #seekfile=sys.argv[7]; 

    #Input path info
    mainpath=sys.argv[11];
    packagepath=sys.argv[12];
    scriptpath=sys.argv[13];

    #Input script info
    scripttype=sys.argv[14];
    scriptext=sys.argv[15];
    scripttimelimit=timestamp2unit(sys.argv[16]);
    scriptmemorylimit=sys.argv[17];

    #Input database info
    mongouri=sys.argv[18];#"mongodb://manager:toric@129.10.135.170:27017/ToricCY";
    queries=eval(sys.argv[19]);
    #dumpfile=sys.argv[13];
    dbcoll=sys.argv[20];
    newcollection,newfield=sys.argv[21].split(",");
    
    #Read seek position from file
    #with open(primerpath+"/"+seekfile,"r") as seekstream:
    #    seekpos=eval(seekstream.read());

    #Open seek file stream
    #seekstream=open(primerpath+"/"+seekfile,"w");

    #If first submission, read from database
    #if seekpos==-1:
    #Open connection to remote database

    statepath=packagepath+"/state";
    modulepath=mainpath+"/modules/"+modname;
    primerpath=modulepath+"/"+primername;
    workpath=primerpath+"/jobs";  
    scriptfile=modname+scriptext;

    primerpartitiontimelimit,primerbuffertimelimit=getpartitiontimelimit(primerpartition,"",buffertime);
    
    mongoclient=toriccy.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=toriccy.getindexes(db,dbcoll);

    with open(statepath+"/modules","r") as modstream:
        modlist=[x.rstrip('\n') for x in modstream.readlines()];
    prevmodlist=modlist[:modlist.index(modname)];
    lastrun=(not (prevprimersrunningq(username,prevmodlist,primername) or userjobsrunningq(username,modname,primername)));
    while (prevprimersrunningq(username,prevmodlist,primername) or userjobsrunningq(username,modname,primername) or lastrun) and timeleftq(starttime,primerbuffertimelimit):
        queryresult=toriccy.querydatabase(db,queries);
        oldqueryresultinds=[dict([(y,x[y]) for y in dbindexes]+[(newfield,{"$exists":True})]) for x in queryresult];
        if len(oldqueryresultinds)==0:
            oldqueryresult=[];
        else:
            oldqueryresult=toriccy.collectionfind(db,newcollection,{"$or":oldqueryresultinds},dict([("_id",0)]+[(y,1) for y in dbindexes]));
        oldqueryresultrunning=[y for x in userjobsrunninglist(username,modname,primername) for y in jobname2jobjson(x,dbindexes) if len(x)>0];
        newqueryresult=[x for x in queryresult if dict([(y,x[y]) for y in dbindexes]) not in oldqueryresult+oldqueryresultrunning];
        #Query database and dump to file
        #querytofile(db,queries,primerpath,"querydump");
        #mongoclient.close();
        #seekpos=0;

        #Open file stream
        #querystream=open(primerpath+"/"+dumpfile,"r");
        #querystream.seek(seekpos);

        #doc=querystream.readline();
        #while doc and timeleftq(starttime,primerbuffertimelimit):
        nnewqueryresult=len(newqueryresult);
        i=0;
        while (i<nnewqueryresult) and timeleftq(starttime,primerbuffertimelimit):
            releaseheldjobs(username,modname,primername);
            ndocsleft=nnewqueryresult-i;
            orderedpartitions=orderpartitions(largemempartitions);
            #if doc2jobname(newqueryresult[i],dbindexes) not in skippedjobslist(username,modname,primername,primerpath):
            orderedpartitions=orderpartitions(partitions)+orderedpartitions;
            nodedistribution=distributeovernodes(statepath,orderedpartitions,ndocsleft,scriptmemorylimit);
            if nodedistribution=="Error":
                nsteps=1;
            else:
                partition,nnodes,ncores,nsteps,memoryperstep=nodedistribution;
                docs=newqueryresult[i:i+nsteps];
                if clusterjobslotsleft(1000):#maxjobcount):
                    #doc=json.loads(doc.rstrip('\n'));
                    jobstepnames=[modname+"_"+primername+"_"+doc2jobname(y,dbindexes) for y in docs];
                    jobname=jobstepnamescontract(jobstepnames);
                    partitiontimelimit,buffertimelimit=getpartitiontimelimit(partition,SLURMtimelimit,buffertime);
                    writejobfile(modname,jobname,primerpath,primername,writemode,partitiontimelimit,partition,nnodes,ncores,memoryperstep,mongouri,scriptpath,scripttype,scriptext,buffertimelimit,dbcoll,dbindexes,docs);
                    #Submit job file
                    submitjob(workpath,jobname,partition,memoryperstep,resubmit=False);
                    #seekstream.write(querystream.tell());
                    #seekstream.flush();
                    #seekstream.seek(0);
                    #doc=querystream.readline();
                else:
                    time.sleep(sleeptime);
            i+=nsteps;
        
        if timeleftq(starttime,primerbuffertimelimit):
            lastrun=(not (prevprimersrunningq(username,prevmodlist,primername) or userjobsrunningq(username,modname,primername) or lastrun));
            releaseheldjobs(username,modname,primername);

    #while userjobsrunningq(username,modname,primername) and timeleftq(starttime,primerbuffertimelimit):
    #    releaseheldjobs(username,modname,primername);
    #    skippedjobs=skippedjobslist(username,modname,primername,workpath);
    #    for x in skippedjobs:
    #        nodemaxmemory=getnodemaxmemory(statepath,primerpartition);
    #        submitjob(workpath,x,primerpartition,nodemaxmemory,resubmit=True);
    #    time.sleep(sleeptime);

    if (prevprimersrunningq(username,prevmodlist,primername) or userjobsrunningq(username,modname,primername) or lastrun) and not timeleftq(starttime,primerbuffertimelimit):
        #Resubmit primer job
        nodemaxmemory=getnodemaxmemory(statepath,primerpartition);
        submitjob(primerpath,"primer_"+modname+"_"+primername,primerpartition,nodemaxmemory,resubmit=True);

    #querystream.close();
    #seekstream.close();
    mongoclient.close();
except Exception as e:
    PrintException();