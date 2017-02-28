#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,re,subrocess,signal,json,mongolink;

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

def jobstepname2indexdoc(jobstepname,dbindexes):
    indexsplit=jobstepname.split("_");
    nindexes=min(len(indexsplit)-2,len(dbindexes));
    return dict([(dbindexes[i],eval(indexsplit[i+2])) for i in range(nindexes)]);

def merge_dicts(*dicts):
    result={};
    for dictionary in dicts:
        result.update(dictionary);
    return result;

try:
    modname=sys.argv[1];
    #jobstepid=sys.argv[3];
    basecollection=sys.argv[2];
    workpath=sys.argv[3];
    jobstepname=sys.argv[4];
    #dbpush=eval(sys.argv[6]);
    #markdone=sys.argv[7];
    
    cputime="";
    maxrss="";
    maxvmsize="";
    bsonsize="";

    packagepath=subprocess.Popen("echo \"${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    statepath=packagepath+"/state";
    mongourifile=statepath+"/mongouri";
    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n");

    #sacctstats=subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0].split(",");#,preexec_fn=default_sigpipe).communicate()[0].split(",");
    #if len(sacctstats)==3:
    #    cputime,maxrss,maxvmsize=sacctstats;

    mongoclient=mongolink.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=mongolink.getintersectionindexes(db,basecollection);

    indexdoc=jobstepname2indexdoc(jobstepname,dbindexes);

    try:
        with open(workpath+"/"+jobstepname+".log","r") as logstream:
            for line in logstream:
                #linedoc=line.rstrip("\n");#re.sub(":[nN]ull",":None",line.rstrip("\n"));
                linehead=re.sub("^([-+].*?>|None).*",r"\1",line).rstrip("\n");
                if linehead[0] in ["-","+"]:
                    linemarker=linehead[0];
                    newcollection,strindexdoc=linehead[1:-1].split(".");
                    #print strindexdoc;
                    #sys.stdout.flush();
                    newindexdoc=json.loads(strindexdoc);
                    linedoc=re.sub("^[-+].*?>","",line).rstrip("\n");
                    #for x in outputlinemarkers:
                    #    linedoc=linedoc.replace(x,"");
                    #print doc;#.replace(" ","");
                    #print linedoc;
                    #sys.stdout.flush();
                    doc=json.loads(linedoc);#.replace(" ",""));
                    #fulldoc=merge_dicts(indexdoc,doc);
                    #newcollection=mongolink.gettierfromdoc(db,fulldoc);
                    #newindexdoc=dict([(x,fulldoc[x]) for x in mongolink.getintersectionindexes(db,newcollection)]);
                    #db[newcollection].update(newindexdoc,{"$set":fulldoc},upsert=True);
                    #if dbpush:
                    if linemarker=="+":
                        bsonsize+=mongolink.bsonsize(doc);
                        if dbpush:
                            db[newcollection].update(newindexdoc,{"$set":doc},upsert=True);
                    elif linemarker=="-":
                        if len(doc)>0:
                            bsonsize-=mongolink.bsonsize(doc);
                            if dbpush:
                                db[newcollection].update(newindexdoc,{"$unset":doc});
                        else:
                            removedocs=list(db[newcollection].find(newindexdoc));
                            for removedoc in removedocs:
                                bsonsize-=mongolink.bsonsize(removedoc);
                                if dbpush:
                                    db[newcollection].remove(removedoc);
                    #print "db["+str(newcollection)+"].update("+str(newindexdoc)+","+str({"$set":fulldoc})+",upsert=True);";
                    #sys.stdout.flush();
                elif linehead=="CPUTime: ":
                    cputime=eval(line[len(linehead):].split(" ")[0]);
                elif linehead=="MaxRSS: ":
                    maxrss=eval(line[len(linehead):].split(" ")[0]);
                elif linehead=="MaxVMSize: ":
                    maxvmsize=eval(line[len(linehead):].split(" ")[0]);
                elif linehead=="BSONSize: ":
                    bsonsize=eval(line[len(linehead):].split(" ")[0]);

        db[basecollection].update(indexdoc,{"$set":{modname+"STATS":{"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize,"BSONSIZE":bsonsize}}});

        #print "CPUTime: "+str(cputime)+" seconds";
        #print "MaxRSS: "+str(maxrss)+" bytes";
        #print "MaxVMSize: "+str(maxvmsize)+" bytes";
        #print "BSONSize: "+str(bsonsize)+" bytes";
        #print "CPUTime: "+cputime+" seconds";
        #print "MaxRSS: "+maxrss+" bytes";
        #print "MaxVMSize: "+maxvmsize+" bytes";
        #print "BSONSize: "+str(bsonsize)+" bytes";
    except IOError:
        print "File path \""+workpath+"/"+jobstepname+".log\" does not exist.";
    #sys.stdout.flush();
    #else:
    #    #print subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0];#,preexec_fn=default_sigpipe).communicate()[0];
    #    #print jobstepid;
    #   #print sacctstats;
    #    #print "sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2";
    #    print "CPUTime: N/A";
    #    print "MaxRSS: N/A";
    #    print "MaxVMSize: N/A";
    #    print "BSONSize: N/A";
    mongoclient.close();
except Exception as e:
    PrintException();