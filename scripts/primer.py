#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,os,subprocess,time,datetime,toriccy;#,json;
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

def jobname2json(jobname,dbindexes):
    indexlist=jobname.split("_");
    return dict([(dbindexes[i],eval(indexlist[i])) for i in range(len(dbindexes))]);

def json2jobname(json,dbindexes):
    return '_'.join([json[x] for x in dbindexes]);

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
        print "Res"+submitcomm[1:]+" as "+jobname+".\n\n";
    else:
        print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        print submitcomm+" as "+jobname+".\n";
    sys.stdout.flush();

def skippedjobslist(username,modname,primername,workpath):
    jobsrunning=jobsrunninglist(username,modname,primername);
    blankfilesstring=subprocess.Popen("find '"+workpath+"' -maxdepth 1 -type f -name '*.out' -empty | rev | cut -d'/' -f1 | rev | cut -d'.' -f1 | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0];
    if blankfilesstring=='':
        return [];
    else:
        blankfiles=blankfilesstring.split(",");
        skippedjobs=[modname+"_"+primername+"_"+x for x in blankfiles if x not in jobsrunning];
        return skippedjobs;

def releaseheldjobs(username,modname,primername):
    subprocess.Popen("for job in $(squeue -h -u "+username+" -o '%A %j %r' | grep '^"+modname+"_"+primername+"' | grep 'job requeued in held state' | sed 's/\s\s*/ /g' | cut -d' ' -f2); do scontrol release $job; done",shell=True);

def getpartslist():
    partsidle=subprocess.Popen("for rawpart in $(sinfo -h -o '%t %c %D %P' | grep 'ser-par-10g' | grep 'idle' | cut -d' ' -f2,3,4 | sed 's/\*//g' | sed 's/\s/,/g'); do echo $(echo $rawpart | cut -d',' -f1,2 | sed 's/,/*/g' | bc),$(echo $rawpart | cut -d',' -f3); done | tr ' ' '\n' | sort -t',' -k1 -n | cut -d',' -f2 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    #partsmix=subprocess.Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'mix' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    #partsalloc=subprocess.Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'alloc' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    #partslist=','.join(partsidle+[x for x in partsmix if x not in partsidle]+[x for x in partsalloc if x not in partsidle+partsmix]);
    partscomp=subprocess.Popen("squeue -h -o '%P %T %L' | grep 'ser-par-10g' | grep 'COMPLETING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    partsrun=subprocess.Popen("squeue -h -o '%P %T %L' | grep 'ser-par-10g' | grep 'RUNNING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    partspend=subprocess.Popen("squeue -h -o '%P %T %L' | grep 'ser-par-10g' | grep 'PENDING' | sort -k2,2 -k3,3n | cut -d' ' -f1 | tr '\n' ',' | head -c -1",shell=True,stdout=subprocess.PIPE).communicate()[0].split(',');
    partslist=','.join([x for x in toriccy.deldup(partsidle+partscomp+partsrun+partspend) if x not in ["","ser-par-10g-5"]]);
    return partslist;

def writejobfile(jobname,workpath,scriptpath,scripttype,scriptfile,timelimit,memorylimit,doc,SLURMtimelimit="24:00:00",partitions="ser-par-10g,ser-par-10g-2,ser-par-10g-3,ser-par-10g-4",ntasks=1,nnodes=1,nnodesexclusive=True,exclusive=True):
    jobstring="#!/bin/bash\n";
    jobstring+="\n";
    jobstring+="#Created "+str(datetime.datetime.now().strftime("%Y %m %d %H:%M:%S"))+"\n";
    jobstring+="\n";
    jobstring+="#Job name\n";
    jobstring+="#SBATCH -J \""+jobname+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Working directory\n";
    jobstring+="#SBATCH -D \""+workpath+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Job output file\n";
    jobstring+="#SBATCH -o \""+jobname+".out\"\n";
    jobstring+="#################\n";
    jobstring+="#Job error file\n";
    jobstring+="#SBATCH -e \""+jobname+".err\"\n";
    jobstring+="#################\n";
    jobstring+="#Job max time\n";
    jobstring+="#SBATCH --time=\""+SLURMtimelimit+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Partition (queue) to use for job\n";
    jobstring+="#SBATCH --partition=\""+partitions+"\"\n";
    jobstring+="#################\n";
    jobstring+="#Number of tasks (CPUs) allocated for job\n";
    jobstring+="#SBATCH -n "+str(ntasks)+"\n";
    jobstring+="#################\n";
    if nnodesexclusive:
        jobstring+="#Number of nodes to distribute n tasks across\n";
        jobstring+="#SBATCH -N "+str(nnodes)+"\n";
        jobstring+="#################\n";
    if exclusive:
        jobstring+="#Lock down N nodes for job\n";
        jobstring+="#SBATCH --exclusive\n";
        jobstring+="#################\n";
    jobstring+="\n";
    jobstring+="#Database info\n";
    jobstring+="mongouri=\"mongodb://frontend:password@129.10.135.170:27017/ToricCY\"\n";
    jobstring+="\n";
    jobstring+="#Cluster info\n";
    jobstring+="scriptpath=\""+scriptpath+"\"\n";
    jobstring+="workpath=\""+workpath+"\"\n";
    jobstring+="\n";
    jobstring+="#Job info\n";
    jobstring+="jobname=\""+jobname+"\"\n";
    jobstring+="timelimit=\""+str(timelimit)+"\"\n";
    jobstring+="memorylimit=\""+str(memorylimit)+"\"\n";
    jobstring+="doc=\""+str(doc)+"\"\n";
    jobstring+="\n";
    jobstring+=scripttype+" \"${scriptpath}/"+scriptfile+"\" \"${workpath}\" \"${jobname}\" \"${mongouri}\" \"${timelimit}\" \"${memorylimit}\" \"${doc}\"\n";# > ${workpath}/${jobname}.log\n";
    jobstream=open(workpath+"/"+jobname+".job","w");
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
    modstatefile=sys.argv[2];
    modname=sys.argv[3];
    primername=sys.argv[4];
    sleeptime=float(sys.argv[5]);
    #seekfile=sys.argv[7]; 

    #Input path info
    mainpath=sys.argv[6];
    statepath=sys.argv[7];
    scriptpath=sys.argv[8];
    logpath=sys.argv[9];
    workpath=sys.argv[10];  

    #Input script info
    scripttype=sys.argv[11];
    scriptext=sys.argv[12];
    timelimit=eval(sys.argv[13]);
    memorylimit=eval(sys.argv[14]);

    #Input database info
    mongouri=sys.argv[15];#"mongodb://frontend:password@129.10.135.170:27017/ToricCY";
    queries=eval(sys.argv[16]);
    #dumpfile=sys.argv[13];
    dbindexes=sys.argv[17].split(",");
    newcollfield=sys.argv[18].split(",");
    
    #Read seek position from file
    #with open(logpath+"/"+seekfile,"r") as seekstream:
    #    seekpos=eval(seekstream.read());

    #Open seek file stream
    #seekstream=open(logpath+"/"+seekfile,"w");

    #If first submission, read from database
    #if seekpos==-1:
    #Open connection to remote database

    scriptfile=modname+scriptext;
    
    mongoclient=MongoClient(mongouri);
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    modstream=open(statepath+"/"+modstatefile,"r");
    modlist=[x.rstrip('\n') for x in modstream.readlines()];
    modstream.close();
    prevmodlist=modlist[:modlist.index(modname)];
    lastrunind=0;
    if len(prevmodlist)==0:
        lastrunind+=1;
    while (primersrunningq(username,prevmodlist,primername) or (lastrunind<2)) and timeleft(starttime):
        queryresult=toriccy.querydatabase(db,queries);
        oldinds=[dict([(y,x[y]) for y in dbindexes]+[(newcollfield[1],{"$exists":True})]) for x in queryresult];
        if len(oldinds)==0:
            oldqueryresult=[];
        else:
            oldqueryresult=toriccy.collectionfind(db,newcollfield[0],{"$or":oldinds},dict([("_id",0)]+[(y,1) for y in dbindexes]));
        oldqueryrunning=[jobname2json(x,dbindexes) for x in jobsrunninglist(username,modname,primername) if len(x)>0];
        newqueryresult=[x for x in queryresult if dict([(y,x[y]) for y in dbindexes]) not in oldqueryresult+oldqueryrunning];
        #Query database and dump to file
        #querytofile(db,queries,logpath,"querydump");
        #mongoclient.close();
        #seekpos=0;

        #Open file stream
        #querystream=open(logpath+"/"+dumpfile,"r");
        #querystream.seek(seekpos);

        #doc=querystream.readline();
        #while doc and timeleft(starttime):
        for doc in newqueryresult:
            releaseheldjobs(username,modname,primername);
            if timeleft(starttime):
                if jobslotsleft(username,maxnjobs):
                    #doc=json.loads(doc.rstrip('\n'));
                    jobname=modname+"_"+primername+"_"+"_".join([str(doc[x]) for x in dbindexes]);
                    matdoc=toriccy.pythondictionary2mathematicarules(doc);
                    writejobfile(jobname,workpath,scriptpath,scripttype,scriptfile,timelimit,memorylimit,matdoc,partitions=getpartslist(),nnodesexclusive=True,exclusive=True);
                    #Submit job file
                    submitjob(workpath,jobname,resubmit=False);
                    #seekstream.write(querystream.tell());
                    #seekstream.flush();
                    #seekstream.seek(0);
                    #doc=querystream.readline();
                else:
                    time.sleep(sleeptime);
            else:
                break;
        time.sleep(sleeptime);
        if not primersrunningq(username,prevmodlist,primername):
            lastrunind+=1;

    while jobsrunningq(username,modname,primername) and timeleft(starttime):
        releaseheldjobs(username,modname,primername);
        skippedjobs=skippedjobslist(username,modname,primername,workpath);
        for x in skippedjobs:
            submitjob(workpath,x,resubmit=True);
        time.sleep(sleeptime);

    if ((primersrunningq(username,prevmodlist,primername) or (lastrunind<2)) or jobsrunningq(username,modname,primername)) and not timeleft(starttime):
        #Resubmit primer job
        submitjob(logpath,"primer_"+modname+"_"+primername,resubmit=True);

    #querystream.close();
    #seekstream.close();
    mongoclient.close();
except Exception as e:
    PrintException();