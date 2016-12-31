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

def timestamp2seconds(timestamp,unit="seconds"):
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

def timeleft(starttime,maxtime=82800):
    "Determine if runtime limit has been reached."
    return (time.time()-starttime)<maxtime;

def jobslotsleft(username,maxnjobs):
    njobs=eval(subprocess.Popen("squeue -h -u "+username+" | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    return njobs<maxnjobs;

def jobsrunningq(username,modname,primername):
    njobsrunning=eval(subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+primername+"' | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    return njobsrunning>0;

def primersrunningq(username,modlist,primername):
    if len(modlist)==0:
        njobsrunning=0;
    else:
        grepstr="\|".join(["^primer_"+x+"_"+primername for x in modlist]);
        njobsrunning=eval(subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '"+grepstr+"' | wc -l",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    return njobsrunning>0;

def jobsrunninglist(username,modname,primername):
    jobsrunningstring=subprocess.Popen("squeue -h -u "+username+" -o '%j' | grep '^"+modname+"_"+primername+"' | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0];
    if jobsrunningstring=='':
        return [];
    else:
        jobsrunning=jobsrunningstring.split(",");
        return jobsrunning;

def submitjob(jobpath,jobname,resubmit=False):
    submit=subprocess.Popen("sbatch "+jobpath+"/"+jobname+".job",shell=True,stdout=subprocess.PIPE);
    submitcomm=submit.communicate()[0].rstrip("\n");
    #Print information about primer job submission
    if resubmit:
        #jobid=submitcomm.split(' ')[-1];
        #maketop=subprocess.Popen("scontrol top "+jobid,shell=True,stdout=subprocess.PIPE);
        print "\n";
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        if "primer" in jobname:
            print "Res"+submitcomm[1:]+" as "+jobname+".";
        else:
            jobstepnames=jobnameexpand(jobname);
            submitcomm=submitcomm.replace("job ","job step ");
            for i in range(len(jobstepnames)):
                print "Res"+submitcomm[1:]+"."+str(i)+" as "+jobstepnames[i]+".";
        print "\n\n";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        if "primer" in jobname:
            print submitcomm+" as "+jobname+".";
        else:
            jobstepnames=jobnameexpand(jobname);
            submitcomm=submitcomm.replace("job ","job step ");
            for i in range(len(jobstepnames)):
                print submitcomm+"."+str(i)+" as "+jobstepnames[i]+".";
        print "\n";
    sys.stdout.flush();

#def skippedjobslist(username,modname,primername,workpath):
#    jobsrunning=jobsrunninglist(username,modname,primername);
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

def nodedistribution(statepath,partitions,ndocsleft,scriptmemorylimit):
    partition=partitions[0];
    ncoresperpartition=eval(subprocess.Popen("sinfo -h -p '"+partition+"' -o '%c' | head -n1",shell=True,stdout=subprocess.PIPE).communicate()[0]);
    maxnnodes=eval(subprocess.Popen("scontrol show partition '"+partition+"' | grep 'MaxNodes=' | sed 's/^.*\sMaxNodes=\([0-9]*\)\s.*$/\\1/g'",shell=True,stdout=subprocess.PIPE).communicate()[0].rstrip("\n"));
    nodemaxmemory=0;
    with open(statepath+"/resources","r") as resourcesstream:
        for resourcesstring in resourcesstream:
            resources=resourcesstring.rstrip("\n").split(" ");
            if resources[0]==partition:
                nodemaxmemory=eval(resources[1]);
                break;
    ncoresdistribmem=nodemaxmemory/scriptmemorylimit;
    #nprocsfloat=min(float(ndocsleft),float(ncoresperpartition),ncoresdistribmem);
    #nnodes=int(min(maxnnodes,math.ceil(1./nprocsfloat)));
    #ncores=nnodes*ncoresperpartition;
    #nprocs=int(max(1,nprocsfloat));
    nnodes=1;
    if ncoresdistribmem<1:
        if len(partition)>1:
            return nodedistribution(statepath,partitions[1:],ndocsleft,scriptmemorylimit);
        else:
            return "Memory requirement is too large for this cluster.";
    else:
        nprocsfloat=min(ndocsleft,ncoresperpartition,ncoresdistribmem);
        ncores=nnodes*ncoresperpartition;
        nprocs=nprocsfloat;
        memoryperprocM=nodemaxmemory/(1000000*ncoresdistribmem);
        return [partition,nnodes,ncores,nprocs,memoryperprocM];

def writejobfile(modname,jobname,primerpath,primername,writemode,SLURMtimelimit,partition,nnodes,ncores,memoryperprocM,mongouri,scriptpath,scripttype,scriptext,scripttimelimit,scriptmemorylimit,dbcoll,docs):
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
    jobstring+="#SBATCH --time=\""+SLURMtimelimit+"\"\n";
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
    jobstring+="scripttimelimit=\""+str(scripttimelimit)+"\"\n";
    jobstring+="scriptmemorylimit=\""+str(scriptmemorylimit)+"\"\n";
    jobstring+="skippedfile=\"${primerpath}/skipped\"\n";
    for i in range(ndocs):
        jobstring+="jobstepnames["+str(i)+"]=\""+jobstepnames[i]+"\"\n";
        jobstring+="docs["+str(i)+"]=\""+str(docs[i])+"\"\n";
        jobstring+="\n";
    jobstring+="for i in {0.."+str(ndocs-1)+"}\n";
    jobstring+="do\n";
    jobstring+="    srun -N 1 -n 1 --exclusive -J \"${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperprocM)+"M\" "+scripttype+" \"${scriptpath}/"+modname+scriptext+"\" \"${workpath}\" \"${jobstepnames[${i}]}\" \"${mongouri}\" \"${scripttimelimit}\" \"${scriptmemorylimit}\" \"${skippedfile}\" \"${docs[${i}]}\" > ${jobstepnames[${i}]}.log &\n";# > ${workpath}/${jobname}.log\n";
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
    jobstring+="    srun -N 1 -n 1 --exclusive -J \"stats_${jobstepnames[${i}]}\" --mem-per-cpu=\""+str(memoryperprocM)+"M\" python \"${scriptpath}/stats.py\" \"${mongouri}\" \"${modname}\" \"${SLURM_JOBID}.${i}\" \"${dbcoll}\" \"${docs[${i}]}\" >> ${jobstepnames[${i}]}.log &\n";# > ${workpath}/${jobname}.log\n";
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
    #Timer and maxnjobs initialization
    starttime=time.time();
    maxnjobs=eval(subprocess.Popen("scontrol show config | grep 'MaxJobCount' | sed 's/\s*//g' | cut -d'=' -f2",shell=True,stdout=subprocess.PIPE).communicate()[0]);

    #Cluster info
    username=sys.argv[1];

    #Input primer info
    modname=sys.argv[2];
    primername=sys.argv[3];
    sleeptime=timestamp2seconds(sys.argv[4]);
    partitions=sys.argv[5].split(",");
    largemempartitions=sys.argv[6].split(",");
    writemode=sys.argv[7];
    SLURMtimelimit=sys.argv[8];

    #seekfile=sys.argv[7]; 

    #Input path info
    mainpath=sys.argv[9];
    packagepath=sys.argv[10];
    scriptpath=sys.argv[11];

    #Input script info
    scripttype=sys.argv[12];
    scriptext=sys.argv[13];
    scripttimelimit=timestamp2seconds(sys.argv[14]);
    scriptmemorylimit=eval(sys.argv[15]);

    #Input database info
    mongouri=sys.argv[16];#"mongodb://frontend:password@129.10.135.170:27017/ToricCY";
    queries=eval(sys.argv[17]);
    #dumpfile=sys.argv[13];
    dbcoll=sys.argv[18];
    newcollection,newfield=sys.argv[19].split(",");
    
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
    
    mongoclient=toriccy.MongoClient(mongouri);
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=toriccy.getindexes(db,dbcoll);

    with open(statepath+"/modules","r") as modstream:
        modlist=[x.rstrip('\n') for x in modstream.readlines()];
    prevmodlist=modlist[:modlist.index(modname)];
    lastrun=(not (primersrunningq(username,prevmodlist,primername) or jobsrunningq(username,modname,primername)));
    while (primersrunningq(username,prevmodlist,primername) or jobsrunningq(username,modname,primername) or lastrun) and timeleft(starttime):
        queryresult=toriccy.querydatabase(db,queries);
        oldqueryresultinds=[dict([(y,x[y]) for y in dbindexes]+[(newfield,{"$exists":True})]) for x in queryresult];
        if len(oldqueryresultinds)==0:
            oldqueryresult=[];
        else:
            oldqueryresult=toriccy.collectionfind(db,newcollection,{"$or":oldqueryresultinds},dict([("_id",0)]+[(y,1) for y in dbindexes]));
        oldqueryresultrunning=[y for x in jobsrunninglist(username,modname,primername) for y in jobname2jobjson(x,dbindexes) if len(x)>0];
        newqueryresult=[x for x in queryresult if dict([(y,x[y]) for y in dbindexes]) not in oldqueryresult+oldqueryresultrunning];
        #Query database and dump to file
        #querytofile(db,queries,primerpath,"querydump");
        #mongoclient.close();
        #seekpos=0;

        #Open file stream
        #querystream=open(primerpath+"/"+dumpfile,"r");
        #querystream.seek(seekpos);

        #doc=querystream.readline();
        #while doc and timeleft(starttime):
        nnewqueryresult=len(newqueryresult);
        i=0;
        while (i<nnewqueryresult) and timeleft(starttime):
            releaseheldjobs(username,modname,primername);
            ndocsleft=nnewqueryresult-i;
            orderedpartitions=orderpartitions(largemempartitions);
            if doc2jobname(newqueryresult[i],dbindexes) not in skippedjobslist(username,modname,primername,primerpath):
                orderedpartitions=orderpartitions(partitions)+orderedpartitions;
            partition,nnodes,ncores,nprocs,memoryperprocM=nodedistribution(statepath,orderedpartitions,ndocsleft,scriptmemorylimit);
            docs=newqueryresult[i:i+nprocs];
            if jobslotsleft(username,maxnjobs):
                #doc=json.loads(doc.rstrip('\n'));
                jobstepnames=[modname+"_"+primername+"_"+doc2jobname(y,dbindexes) for y in docs];
                jobname=jobstepnamescontract(jobstepnames);
                if scriptext==".m":
                    docs=[toriccy.pythondictionary2mathematicarules(x) for x in docs];
                writejobfile(modname,jobname,primerpath,primername,writemode,SLURMtimelimit,partition,nnodes,ncores,memoryperprocM,mongouri,scriptpath,scripttype,scriptext,scripttimelimit,scriptmemorylimit,dbcoll,docs);
                #Submit job file
                submitjob(workpath,jobname,resubmit=False);
                #seekstream.write(querystream.tell());
                #seekstream.flush();
                #seekstream.seek(0);
                #doc=querystream.readline();
            else:
                time.sleep(sleeptime);
            i+=nprocs;
        
        if timeleft(starttime):
            time.sleep(sleeptime);
            lastrun=(not (primersrunningq(username,prevmodlist,primername) or jobsrunningq(username,modname,primername) or lastrun));
            releaseheldjobs(username,modname,primername);

    #while jobsrunningq(username,modname,primername) and timeleft(starttime):
    #    releaseheldjobs(username,modname,primername);
    #    skippedjobs=skippedjobslist(username,modname,primername,workpath);
    #    for x in skippedjobs:
    #        submitjob(workpath,x,resubmit=True);
    #    time.sleep(sleeptime);

    if (primersrunningq(username,prevmodlist,primername) or jobsrunningq(username,modname,primername) or lastrun) and not timeleft(starttime):
        #Resubmit primer job
        submitjob(primerpath,"primer_"+modname+"_"+primername,resubmit=True);

    #querystream.close();
    #seekstream.close();
    mongoclient.close();
except Exception as e:
    PrintException();