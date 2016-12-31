#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,subprocess,toriccy;

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

try:
    mongouri=sys.argv[1];#"mongodb://frontend:password@129.10.135.170:27017/ToricCY";
    modname=sys.argv[2];
    jobstepid=sys.argv[3];
    dbcoll=sys.argv[4];
    doc=eval(sys.argv[5]);

    mongoclient=toriccy.MongoClient(mongouri);
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=toriccy.getindexes(db,dbcoll);
    newindexes=dict([(x,doc[x]) for x in dbindexes]);

    cputime,maxrss,maxvmsize=subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0].split(",");

    db[dbcoll].update(newindexes,{"$set":{modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize}}});

    print "CPUTime: "+str(cputime);
    print "MaxRSS: "+str(maxrss);
    print "MaxVMSize: "+str(maxvmsize);
    sys.stdout.flush();

    mongoclient.close();
except Exception as e:
    PrintException();