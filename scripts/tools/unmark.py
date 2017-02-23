#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,json,toriccy;

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
    mongouri=sys.argv[1];
    basecollection=sys.argv[2];
    modname=sys.argv[3];
    markdone=sys.argv[4];
    query=json.loads(sys.argv[5]);

    mongoclient=toriccy.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    query.update({modname+markdone:{"$exists":True,"$eq":True}});
    db[basecollection].update(query,{"$unset":{modname+markdone:""}},multi=True);

    mongoclient.close();
except Exception as e:
    PrintException();