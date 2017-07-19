#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,os,linecache,traceback,re,subprocess,signal,json,mongolink;

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
    return dict([(dbindexes[i],eval(indexsplit[i+2]) if indexsplit[i+2].isdigit() else indexsplit[i+2]) for i in range(nindexes)]);

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
    dbpush=eval(sys.argv[5]);
    markdone=sys.argv[6];
    writestats=eval(sys.argv[7]);
    writestorage=eval(sys.argv[8]);
    cputime=sys.argv[9];
    maxrss=sys.argv[10];
    maxvmsize=sys.argv[11];

    try:
        if dbpush or writestorage or (markdone!=""):
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

            statsset={};

            if dbpush or writestorage:
                bsonsize=0;
                with open(workpath+"/"+jobstepname+".log","r") as logstream:
                    for line in logstream:
                        #print "a";
                        #sys.stdout.flush();
                        #linedoc=line.rstrip("\n");#re.sub(":[nN]ull",":None",line.rstrip("\n"));
                        linehead=re.sub("^([-+&].*?>|None).*",r"\1",line).rstrip("\n");
                        if linehead[0] in ["-","+","&"]:
                            linemarker=linehead[0];
                            newcollection,strindexdoc=linehead[1:-1].split(".");
                            #print strindexdoc;
                            #sys.stdout.flush();
                            newindexdoc=json.loads(strindexdoc);
                            linedoc=re.sub("^[-+&].*?>","",line).rstrip("\n");
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
                            #print "b";
                            #sys.stdout.flush();
                            if linemarker=="+":
                                #print "c";
                                #sys.stdout.flush();
                                if writestorage:
                                    bsonsize+=mongolink.bsonsize(doc);
                                #print "d";
                                #sys.stdout.flush();
                                if dbpush:
                                    db[newcollection].update(newindexdoc,{"$set":doc},upsert=True);
                            elif linemarker=="&":
                                #print "c";
                                #sys.stdout.flush();
                                if writestorage:
                                    bsonsize+=mongolink.bsonsize(doc);
                                #print "d";
                                #sys.stdout.flush();
                                if dbpush:
                                    db[newcollection].update(newindexdoc,{"$push":doc},upsert=True);
                            elif linemarker=="-":
                                if len(doc)>0:
                                    #print "e";
                                    #sys.stdout.flush();
                                    if writestorage:
                                        bsonsize-=mongolink.bsonsize(doc);
                                    #print "f";
                                    #sys.stdout.flush();
                                    if dbpush:
                                        db[newcollection].update(newindexdoc,{"$unset":doc});
                                else:
                                    #print mongolink.collectionfind(db,newcollection,newindexdoc,{},formatresult="expression");
                                    #sys.stdout.flush();
                                    #print "g";
                                    #sys.stdout.flush();
                                    #removedocs=list(db[newcollection].find(newindexdoc));
                                    #for removedoc in removedocs:
                                    if writestorage:
                                        removedoc=db[newcollection].find_one(newindexdoc);
                                        #print "h";
                                        #sys.stdout.flush();
                                        bsonsize-=mongolink.bsonsize(removedoc);
                                    #print "i";
                                    #sys.stdout.flush();
                                    if dbpush:
                                        #db[newcollection].remove(removedoc);
                                        db[newcollection].remove(newindexdoc,multi=False);
                            #print "db["+str(newcollection)+"].update("+str(newindexdoc)+","+str({"$set":fulldoc})+",upsert=True);";
                            #sys.stdout.flush();
                #print "j";
                #sys.stdout.flush();
                if dbpush:
                    stats={};
                    if writestats:
                        stats.update({"CPUTIME":eval(cputime),"MAXRSS":eval(maxrss),"MAXVMSIZE":eval(maxvmsize)});
                    if writestorage:
                        stats.update({"BSONSIZE":bsonsize});
                    statsset.update({modname+"STATS":stats});
                #print "k";
                #sys.stdout.flush();
            if markdone!="":
                statsset.update({modname+markdone:True});
            #print "l";
            #sys.stdout.flush();
            if len(statsset)>0:
                db[basecollection].update(indexdoc,{"$set":statsset});

            #print "CPUTime: "+str(cputime)+" seconds";
            #print "MaxRSS: "+str(maxrss)+" bytes";
            #print "MaxVMSize: "+str(maxvmsize)+" bytes";
            #print "BSONSize: "+str(bsonsize)+" bytes";
            if writestats:
                print "CPUTime: "+cputime+" seconds";
                print "MaxRSS: "+maxrss+" bytes";
                print "MaxVMSize: "+maxvmsize+" bytes";
            if writestorage:
                print "BSONSize: "+str(bsonsize)+" bytes";
            mongoclient.close();
        os.remove(workpath+"/"+jobstepname+".stat");
    except IOError:
        print "File path \""+workpath+"/"+jobstepname+".log\" does not exist.";
    sys.stdout.flush();
    #else:
    #    #print subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0];#,preexec_fn=default_sigpipe).communicate()[0];
    #    #print jobstepid;
    #   #print sacctstats;
    #    #print "sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2";
    #    print "CPUTime: N/A";
    #    print "MaxRSS: N/A";
    #    print "MaxVMSize: N/A";
    #    print "BSONSize: N/A";
except Exception as e:
    PrintException();