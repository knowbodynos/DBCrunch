#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

from sage.all_cmdline import *;

import sys,os,linecache,traceback,subprocess,signal,json,mongolink;
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;

#################################################################################
#Misc. function definitions
def PrintException():
    "If an exception is raised, print traceback of it to output log."
    exc_type,exc_obj,tb=sys.exc_info();
    f=tb.tb_frame;
    lineno=tb.tb_lineno;
    filename=f.f_code.co_filename;
    linecache.checkcache(filename);
    line=linecache.getline(filename,lineno,f.f_globals);
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename,lineno,line.strip(),exc_obj);
    print "More info: ",traceback.format_exc();

def default_sigpipe():
    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

#################################################################################
#Main body
try:
    #IO Definitions
    triangdoc=json.loads(sys.argv[1]);
    #Read in pertinent fields from JSON
    polyid=triangdoc['POLYID'];
    oldgeomn=triangdoc['OLDGEOMN'];
    oldtriangn=triangdoc['OLDTRIANGN'];
    h11=triangdoc['H11'];

    packagepath=subprocess.Popen("echo \"${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    statepath=packagepath+"/state";
    mongourifile=statepath+"/mongouri";
    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n");

    mongoclient=mongolink.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];
    involdocs=mongolink.collectionfind(db,'INVOL',{'H11':h11,'POLYID':polyid,'GEOMN':oldgeomn,'TRIANGN':oldtriangn},{'_id':0,'INVOLN':1},formatresult='expression');
    #Add new properties to base tier of JSON
    if len(involdocs)==0:
        print "None";
    else:
        for i in range(len(involdocs)):
            print "+INVOL."+json.dumps({'POLYID':polyid,'GEOMN':oldgeomn,'TRIANGN':oldtriangn,'INVOLN':involdocs[i]['INVOLN']},separators=(',',':'))+">"+json.dumps({'OLDGEOMN':oldgeomn,'OLDTRIANGN':oldtriangn,'GEOMN':triangdoc['GEOMN'],'TRIANGN':triangdoc['TRIANGN'],'ALLTRIANGN':triangdoc['ALLTRIANGN']},separators=(',',':'));
    sys.stdout.flush();
    
except Exception as e:
    PrintException();