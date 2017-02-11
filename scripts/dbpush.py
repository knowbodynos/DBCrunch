#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,re,json,toriccy;#,signal,subprocess;

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

#def default_sigpipe():
#    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

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
    #jobstepid=sys.argv[3];
    basecollection=sys.argv[3];
    workpath=sys.argv[4];
    jobstepname=sys.argv[5];

    cputime="";
    maxrss="";
    maxvmsize="";
    bsonsize="";

    #sacctstats=subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0].split(",");#,preexec_fn=default_sigpipe).communicate()[0].split(",");
    #if len(sacctstats)==3:
    #    cputime,maxrss,maxvmsize=sacctstats;

    mongoclient=toriccy.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];

    dbindexes=toriccy.getintersectionindexes(db,basecollection);

    indexdoc=jobstepname2doc(jobstepname,dbindexes);

    bsonsize=0;
    try:
        with open(workpath+"/"+jobstepname+".log","r") as logstream:
            for line in logstream:
                #linedoc=line.rstrip("\n");#re.sub(":[nN]ull",":None",line.rstrip("\n"));
                line=line.rstrip("\n");
                linehead=re.sub("^([-+].*?>|None|CPUTime: |MaxRSS: |MaxVMSize: |BSONSize: ).*",r"\1",line);
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
                    #newcollection=toriccy.gettierfromdoc(db,fulldoc);
                    #newindexdoc=dict([(x,fulldoc[x]) for x in toriccy.getintersectionindexes(db,newcollection)]);
                    #db[newcollection].update(newindexdoc,{"$set":fulldoc},upsert=True);
                    if linemarker=="+":
                        db[newcollection].update(newindexdoc,{"$set":doc},upsert=True);
                    elif linemarker=="-":
                        db[newcollection].update(newindexdoc,{"$unset":doc});
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