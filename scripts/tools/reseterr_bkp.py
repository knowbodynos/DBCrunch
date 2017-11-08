#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,os,itertools,glob,json,mongolink;

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

def glob_many(*patterns):
	return itertools.chain.from_iterable(glob.iglob(pattern) for pattern in patterns);

def indexdoc2jobstepname(doc,modname,controllername,dbindexes):
    return modname+"_"+controllername+"_"+"_".join([str(doc[x]) for x in dbindexes if x in doc.keys()]);

try:
    mainpath=sys.argv[1];
    modname=sys.argv[2];
    controllername=sys.argv[3];
    patterns=sys.argv[4:];

    controllerpath=mainpath+"/modules/"+modname+"/"+controllername;
    workpath=controllerpath+"/jobs"
    controllerfile=controllerpath+"/controller_"+modname+"_"+controllername+".job";
    mongourifile=mainpath+"/state/mongouri";

    with open(controllerfile,"r") as controllerstream:
        for controllerline in controllerstream:
            if "dbname=" in controllerline:
                dbname=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
            elif "dbusername=" in controllerline:
                dbusername=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
            elif "dbpassword=" in controllerline:
                dbpassword=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
            elif "basecollection=" in controllerline:
            	basecollection=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");

    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n").replace("mongodb://","mongodb://"+dbusername+":"+dbpassword+"@");

    mongoclient=mongolink.MongoClient(mongouri+dbname+"?authMechanism=SCRAM-SHA-1");
    db=mongoclient[dbname];

    dbindexes=mongolink.getintersectionindexes(db,basecollection);

    with open(controllerpath+"/skippedstate","a") as skippedstream:
	    for docsfile in glob_many(*patterns):
	    	with open(docsfile,"r") as docsfilestream:
                for docsline in docsfilestream:
                    doc=json.loads(docsline.rstrip("\n"));
                    skippedjob=indexdoc2jobstepname(doc,modname,controllername,dbindexes);
                    if ".err" in docsfile:
                    	errfile=docsfile.replace(".docs","");
                    	with open(errfile,"r") as errfilestream:
                    		for line in errfilestream:
                    			if "ExitCode:" in line:
                    				exitcode=line.rstrip("\n").split(" ")[1];
                    else:
	                    exitcode="-1:0";
                    resubmitq="True";
                    skippedstream.write(skippedjob+","+exitcode+","+resubmitq+"\n");
                    skippedstream.flush();
                os.remove(docsfilestream.name);
    mongoclient.close();
except Exception as e:
    PrintException();