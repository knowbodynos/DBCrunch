#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,os,time,linecache,traceback,re,tempfile,json,mongolink,datetime;#,errno,fcntl
from pprint import pprint;
from pymongo.errors import BulkWriteError;

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
    mainpath=sys.argv[1];
    modname=sys.argv[2];
    controllername=sys.argv[3];
    #jobnum=sys.argv[4];
    #statsnum=sys.argv[5];
    #infileext=sys.argv[6];
    infilename=sys.argv[4];
    #nbatcheswrite=sys.argv[5];
    dbwrite=eval(sys.argv[5]);
    markdone=sys.argv[6];
    writestats=eval(sys.argv[7]);

    #jobname=modname+"_"+controllername+"_job_"+jobnum;
    #statsname=jobname+"_stats_"+statsnum;

    controllerpath=mainpath+"/modules/"+modname+"/"+controllername;
    workpath=controllerpath+"/jobs"
    controllerfile=controllerpath+"/controller_"+modname+"_"+controllername+".job";
    mongourifile=mainpath+"/state/mongouri";
    infile=workpath+"/"+infilename;#statsname+infileext;
    
    with open(controllerfile,"r") as controllerstream:
        for controllerline in controllerstream:
            if "dbname=" in controllerline:
                dbname=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
            elif "dbusername=" in controllerline:
                dbusername=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
            elif "dbpassword=" in controllerline:
                dbpassword=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");

    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n");
        if dbusername!="":
            mongouri=mongouri.replace("mongodb://","mongodb://"+dbusername+":"+dbpassword+"@");
            mongoclient=mongolink.MongoClient(mongouri+dbname+"?authMechanism=SCRAM-SHA-1");
        else:
            mongoclient=mongolink.MongoClient(mongouri+dbname);

    db=mongoclient[dbname];

    bulkdict={};
    #rmdocs=[];#{};

    #bsonsize=0;
    with open(infile,"r") as instream:
        line=instream.readline().rstrip("\n");
        while line!="":
            linehead=re.sub("^([-+&@].*?>|None).*",r"\1",line);
            linemarker=linehead[0];
            if linemarker=="-":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                #bsonsize-=mongolink.bsonsize(doc);
                if dbwrite:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).update({"$unset":doc});
                print(line);
                line=instream.readline().rstrip("\n");
            elif linemarker=="+":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                #bsonsize+=mongolink.bsonsize(doc);
                if dbwrite:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":doc});
                print(line);
                line=instream.readline().rstrip("\n");
            elif linemarker=="&":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                #bsonsize+=mongolink.bsonsize(doc);
                if dbwrite:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).upsert().update({"$addToSet":doc});
                print(line);
                line=instream.readline().rstrip("\n");
            elif linemarker=="@":
                #if linehead[-1]==">":
                #    newcollection,strindexdoc=linehead[1:-1].split(".");
                #    newindexdoc=json.loads(strindexdoc);
                #    stepnum=re.sub("^[-+&@].*?>","",line).rstrip("\n");
                    #if stepnum not in rmdocs:#.keys():
                    #    #rmdocs[stepnum]=[newindexdoc];
                    #    rmdocs=[stepnum];
                    #else:
                    #    #rmdocs[stepnum]+=[newindexdoc];
                    #    rmdocs+=[stepnum];
                #    print(line.split(">")[0]);
                #else:
                newcollection,strindexdoc=linehead[1:].split("<")[0].split(".");
                newindexdoc=json.loads(strindexdoc);
                print(line);
                line=instream.readline().rstrip("\n");
                while (line!="") and any([line[:len(x)]==x for x in ["CPUTime","MaxRSS","MaxVMSize","BSONSize"]]):
                    if "CPUTime: " in line:
                        cputime=eval(line.split(" ")[1]);
                    if "MaxRSS: " in line:
                        maxrss=eval(line.split(" ")[1]);
                    if "MaxVMSize: " in line:
                        maxvmsize=eval(line.split(" ")[1]);
                        #print("BSONSize: "+str(bsonsize)+" bytes");
                    if "BSONSize: " in line:
                        #instream.seek(statstell);
                        bsonsize=eval(line.split(" ")[1]);
                    print(line);
                    line=instream.readline().rstrip("\n");
                statsmark={};
                if writestats:
                    statsmark.update({modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}});
                if markdone!="":
                    statsmark.update({modname+markdone:True});
                if dbwrite and len(statsmark)>0:
                    if newcollection not in bulkdict.keys():
                        bulkdict[newcollection]=db[newcollection].initialize_unordered_bulk_op();
                    bulkdict[newcollection].find(newindexdoc).upsert().update({"$set":statsmark});
                #bsonsize=0;
            else:
                print(line);
                line=instream.readline().rstrip("\n");
            sys.stdout.flush();      
    #print("+a:"+datetime.datetime.now().strftime("%H.%M.%S"));
    #sys.stdout.flush();
    for bulkcoll in bulkdict.keys():
        try:
            bulkdict[bulkcoll].execute();
        except BulkWriteError as bwe:
            pprint(bwe.details);
    #print("+b:"+datetime.datetime.now().strftime("%H.%M.%S"));
    #sys.stdout.flush();
    #if infileext!=".log":
    #    stepnames=jobname+"_step_";
    #    statsstepnames=statsname+"_step_";
    #    stepfiles=workpath+"/"+jobname+"_step_";
    #    statsstepfiles=workpath+"/"+statsname+"_step_";
    #    for stepnum in rmdocs:#.keys():
            #with open(statsstepfiles+stepnum+".inbatch","r") as batchdocstream, tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempdocstream:
            #        ndocsleft=0;
            #        for checkline in batchdocstream:
            #            checklinedoc=json.loads(checkline.rstrip("\n"));
            #            if not any([all([y in checklinedoc.items() for y in x.items()]) for x in rmdocs[stepnum]]):
            #                tempdocstream.write(checkline);
            #                tempdocstream.flush();
            #                ndocsleft+=1;
            #        os.rename(tempdocstream.name,batchdocstream.name);
            #if ndocsleft>0:
            #    with open(statsstepfiles+stepnum+".inbatch","r") as batchdocstream, open(stepfiles+stepnum+".docs","a") as docstream:
            #        while True:
            #            try:
            #                fcntl.flock(docstream,fcntl.LOCK_EX | fcntl.LOCK_NB);
            #                break;
            #            except IOError as e:
            #                if e.errno!=errno.EAGAIN:
            #                    raise;
            #                else:
            #                    time.sleep(0.1);
            #        for line in batchdocstream:
            #            docstream.write(line);
            #            docstream.flush();
            #        fcntl.flock(docstream,fcntl.LOCK_UN);
    #        os.remove(statsstepfiles+stepnum+".inbatch");
    #if infileext!=".log":
    #    os.remove(infile);
    mongoclient.close();
except Exception as e:
    PrintException();