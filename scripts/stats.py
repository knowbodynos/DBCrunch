#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,subprocess,re,toriccy;

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

def jobstepname2doc(jobstepname,dbindexes):
    indexsplit=jobstepname.split("_")[2:];
    return dict([(dbindexes[i],indexsplit[i]) for i in range(len(dbindexes))]);

def merge_dicts(*dicts):
    result={};
    for dictionary in dicts:
        result.update(dictionary);
    return result;

try:
    mongouri=sys.argv[1];#"mongodb://manager:toric@129.10.135.170:27017/ToricCY";
    modname=sys.argv[2];
    jobstepid=sys.argv[3];
    dbcollection=sys.argv[4];
    workpath=sys.argv[5];
    jobstepname=sys.argv[6];

    mongoclient=toriccy.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=toriccy.getindexes(db,dbcollection);

    cputime,maxrss,maxvmsize=subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0].split(",");

    indexdoc=jobstepname2doc(jobstepname,dbindexes);

    bsonsize=0;
    with open(workpath+"/"+jobstepname+".log","r") as logstream:
        for line in logstream:
            doc=eval(re.sub(":[nN]ull",":None",line.rstrip("\n").replace("Output:","").replace(" ","")));
            bsonsize+=toriccy.bsonsize(doc);
            fulldoc=merge_dicts(indexdoc,doc);
            newcollection=toriccy.gettierfromdoc(db,fulldoc);
            newindexdoc=dict([(x,fulldoc[x]) for x in toriccy.getindexes(db,newcollection)]);
            db[newcollection].update(newindexdoc,{"$set":fulldoc},upsert=True);

    db[dbcollection].update(indexdoc,{"$set":{modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}}});

    print "CPUTime: "+str(cputime)+" seconds";
    print "MaxRSS: "+str(maxrss)+" bytes";
    print "MaxVMSize: "+str(maxvmsize)+" bytes";
    print "BSONSize: "+str(bsonsize)+" bytes";
    sys.stdout.flush();

    mongoclient.close();
except Exception as e:
    PrintException();