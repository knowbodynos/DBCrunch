#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,subprocess,signal,json,mongolink;

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

try:
    basecollection=sys.argv[1];
    modname=sys.argv[2];
    markdone=sys.argv[3];
    query=json.loads(sys.argv[4]);

    packagepath=subprocess.Popen("echo \"${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    statepath=packagepath+"/state";
    mongourifile=statepath+"/mongouri";
    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n");

    mongoclient=mongolink.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    query.update({modname+markdone:{"$exists":True,"$eq":True}});
    db[basecollection].update(query,{"$unset":{modname+markdone:""}},multi=True);

    mongoclient.close();
except Exception as e:
    PrintException();