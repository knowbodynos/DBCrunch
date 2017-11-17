#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,os,shutil,linecache,traceback,re,subprocess,signal,tempfile,json,mongolink;

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

def indexdoc2jobstepname(doc,modname,controllername,dbindexes):
    return modname+"_"+controllername+"_"+"_".join([str(doc[x]) for x in dbindexes if x in doc.keys()]);

def merge_dicts(*dicts):
    result={};
    for dictionary in dicts:
        result.update(dictionary);
    return result;

try:
    modname=sys.argv[1];
    controllername=sys.argv[2];
    #jobstepid=sys.argv[3];
    basecollection=sys.argv[3];
    controllerpath=sys.argv[4];
    jobstepname=sys.argv[5];
    dbpush=eval(sys.argv[6]);
    markdone=sys.argv[7];
    writestats=eval(sys.argv[8]);
    writestorage=eval(sys.argv[9]);
    niters=eval(sys.argv[10]);
    cputime=eval(sys.argv[11]);
    maxrss=eval(sys.argv[12]);
    maxvmsize=eval(sys.argv[13]);

    workpath=controllerpath+"/jobs"

    try:
        packagepath=subprocess.Popen("echo \"${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
        statepath=packagepath+"/state";
        mongourifile=statepath+"/mongouri";
        with open(mongourifile,"r") as mongouristream:
            mongouri=mongouristream.readline().rstrip("\n");

        #sacctstats=subprocess.Popen("sacct -n -o 'CPUTimeRAW,MaxRSS,MaxVMSize' -j "+jobstepid+" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | tr ' ' ',' | head -c -2",shell=True,stdout=subprocess.PIPE).communicate()[0].split(",");#,preexec_fn=default_sigpipe).communicate()[0].split(",");
        #if len(sacctstats)==3:
        #    cputime,maxrss,maxvmsize=sacctstats;

        mongoclient=mongolink.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
        with open(controllerpath+"/controller_"+modname+"_"+controllername+".job") as controllerstream:
            for controllerline in controllerstream:
                if "dbname=" in controllerline:
                    dbname=controllerline.split("=")[1].lstrip("\"").rstrip("\"\n");
                    break;
        #dbname=mongouri.split("/")[-1];
        db=mongoclient[dbname];

        dbindexes=mongolink.getintersectionindexes(db,basecollection);

        statsset={};

        #newbasedoc={};
        #basedocs=[];

        if niters>1:
            with open(workpath+"/"+jobstepname+".docs","r") as docstream:
                docscount=0;
                for line in docstream:
                    docscount+=1;
            if docscount>0:
                cputime=float(cputime/docscount);
            else:
                cputime=0;

        bsonsize=0;
        #newindexdoc="";
        #with open(workpath+"/"+jobstepname+".log","r") as logstream:
        logfile=workpath+"/"+jobstepname+".log";
        logstream=open(logfile,"r");

        line=logstream.readline();
        #for line in logstream:
        while line!="":
            #print "a";
            #sys.stdout.flush();
            #linedoc=line.rstrip("\n");#re.sub(":[nN]ull",":None",line.rstrip("\n"));
            linehead=re.sub("^([-+&@].*?>|None).*",r"\1",line).rstrip("\n");
            linemarker=linehead[0];
            if linemarker=="-":
                print(line+"\n");
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                if writestorage:
                    bsonsize-=mongolink.bsonsize(doc);
                if dbpush:
                    db[newcollection].update(newindexdoc,{"$unset":doc});
            elif linemarker=="+":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                if writestorage:
                    bsonsize+=mongolink.bsonsize(doc);
                if dbpush:
                    db[newcollection].update(newindexdoc,{"$set":doc},upsert=True);
            elif linemarker=="&":
                newcollection,strindexdoc=linehead[1:-1].split(".");
                newindexdoc=json.loads(strindexdoc);
                linedoc=re.sub("^[-+&].*?>","",line).rstrip("\n");
                doc=json.loads(linedoc);
                if writestorage:
                    bsonsize+=mongolink.bsonsize(doc);
                if dbpush:
                    db[newcollection].update(newindexdoc,{"$push":doc},upsert=True);
            elif linemarker=="@":
                newcollection,strindexdoc=linehead[1:].split(".");
                newindexdoc=json.loads(strindexdoc);
                with open(workpath+"/"+jobstepname+".docs","r") as docstream, tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempdocstream:
                    for checkline in docstream:
                        checklinedoc=json.loads(checkline.rstrip("\n"));
                        if not all([x in checklinedoc.items() for x in newindexdoc.items()]):
                            tempdocstream.write(checkline);
                            tempdocstream.flush();
                    os.rename(tempdocstream.name,docstream.name);
                statsset={};
                if markdone!="":
                    statsset.update({modname+markdone:True});
                stats={};
                if writestats:
                    stats.update({"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize});
                if writestorage:
                    stats.update({"BSONSIZE":bsonsize});
                if dbpush and len(statsset)>0:
                    db[newcollection].update(newindexdoc,{"$set":statsset});
                with tempfile.NamedTemporaryFile(dir=workpath,delete=False) as templogstream:
                    logtell=logstream.tell();
                    #print("a: "+str(logtell)+"\n");
                    #sys.stdout.flush();
                    logstream.seek(0);
                    line=logstream.readline();
                    #for line in logstream:
                    while (line!="") and (logstream.tell()<=logtell):
                        templogstream.write(line);
                        templogstream.flush();
                        #if logstream.tell()==logtell:
                        #    print("b: "+str(logstream.tell())+" "+str(templogstream.tell())+"\n");
                        #    sys.stdout.flush();
                        line=logstream.readline();
                    if writestats:
                        templogstream.write("CPUTime: "+str(cputime)+" seconds\n");
                        templogstream.write("MaxRSS: "+str(maxrss)+" bytes\n");
                        templogstream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
                    if writestorage:
                        templogstream.write("BSONSize: "+str(bsonsize)+" bytes\n");
                    templogstream.flush();
                    logtell=templogstream.tell();
                    #print("c: "+str(logtell)+"\n");
                    #sys.stdout.flush();
                    #for line in logstream:
                    while line!="":
                        templogstream.write(line);
                        templogstream.flush();
                        line=logstream.readline();
                    os.rename(templogstream.name,logstream.name);
                    logstream.close();
                    logstream=open(logfile,"r");
                bsonsize=0;
                #logstream.close();
                #urrline=line;
                #for line in logstream:
                #    templogstream.write(line);
                #shutil.copy(templogstream.name,logstream.name);
                #logstream=open(logfile,"r");
                #logstream.seek(0);
                logstream.seek(logtell);
                #print("d: "+str(logstream.tell())+"\n");
                #sys.stdout.flush();
            line=logstream.readline();
                
                #line="";
                #while line!=currline:
                #    line=logstream.readline();
                #if writestats:
                #    line=logstream.readline();
                #    line=logstream.readline();
                #    line=logstream.readline();
                #if writestorage:
                #    line=logstream.readline();
            #line=logstream.readline();

            #if linemarker in ["@","-","+","&"]:
            #    newcollection,strindexdoc=linehead[1:-1].split(".");
                #print strindexdoc;
                #sys.stdout.flush();
            #    newindexdoc=json.loads(strindexdoc);
                #lastbasedoc=newbasedoc;
                #newbasedoc=dict([(x,newindexdoc[x]) for x in newindexdoc.keys() if x in dbindexes]);
                #basedocs+=[dict([(x,newindexdoc[x]) for x in newindexdoc.keys() if x in dbindexes])];
            #    if linemarker!="@":
            #        linedoc=re.sub("^[-+&].*?>","",line).rstrip("\n");
                    #for x in outputlinemarkers:
                    #    linedoc=linedoc.replace(x,"");
                    #print doc;#.replace(" ","");
                    #print linedoc;
                    #sys.stdout.flush();
            #        doc=json.loads(linedoc);#.replace(" ",""));
                    #fulldoc=merge_dicts(indexdoc,doc);
                    #newcollection=mongolink.gettierfromdoc(db,fulldoc);
                    #newindexdoc=dict([(x,fulldoc[x]) for x in mongolink.getintersectionindexes(db,newcollection)]);
                    #db[newcollection].update(newindexdoc,{"$set":fulldoc},upsert=True);
                    #print "b";
                    #sys.stdout.flush();
            #        if linemarker=="+":
                        #print "c";
                        #sys.stdout.flush();
            #            if writestorage:
            #                bsonsize+=mongolink.bsonsize(doc);
                        #print "d";
                        #sys.stdout.flush();
            #            if dbpush:
            #                db[newcollection].update(newindexdoc,{"$set":doc},upsert=True);
            #        elif linemarker=="&":
                        #print "c";
                        #sys.stdout.flush();
            #            if writestorage:
            #                bsonsize+=mongolink.bsonsize(doc);
                        #print "d";
                        #sys.stdout.flush();
            #            if dbpush:
            #                db[newcollection].update(newindexdoc,{"$push":doc},upsert=True);
            #        elif linemarker=="-":
            #            if len(doc)>0:
                            #print "e";
                            #sys.stdout.flush();
            #                if writestorage:
            #                    bsonsize-=mongolink.bsonsize(doc);
                            #print "f";
                            #sys.stdout.flush();
            #                if dbpush:
            #                    db[newcollection].update(newindexdoc,{"$unset":doc});
            #            else:
                            #print mongolink.collectionfind(db,newcollection,newindexdoc,{},formatresult="expression");
                            #sys.stdout.flush();
                            #print "g";
                            #sys.stdout.flush();
                            #removedocs=list(db[newcollection].find(newindexdoc));
                            #for removedoc in removedocs:
            #                if writestorage:
            #                    removedoc=db[newcollection].find_one(newindexdoc);
                                #print "h";
                                #sys.stdout.flush();
            #                    bsonsize-=mongolink.bsonsize(removedoc);
                            #print "i";
                            #sys.stdout.flush();
            #                if dbpush:
                                #db[newcollection].remove(removedoc);
            #                    db[newcollection].remove(newindexdoc,multi=False);
                    #print "db["+str(newcollection)+"].update("+str(newindexdoc)+","+str({"$set":fulldoc})+",upsert=True);";
                    #sys.stdout.flush();
                    #if (len(lastbasedoc)>0) and (newbasedoc!=lastbasedoc):
                    #templogstream.write("basedocs: "+str(basedocs)+"\n");
                    #templogstream.flush();
            #    else:
                #if (len(basedocs)>1) and (basedocs[-1]!=basedocs[-2]):
            #        with tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempdocstream:
            #            for checkline in docstream:
            #                checklinedoc=json.loads(checkline.rstrip("\n"));
            #                if not all([x in checklinedoc.items() for x in newindexdoc.items()]):
            #                    tempdocstream.write(checkline);
            #                    tempdocstream.flush();
            #            os.rename(tempdocstream.name,docstream.name);
            #        docstream.seek(0);
            #        statsset={};
            #        if markdone!="":
            #            statsset.update({modname+markdone:True});
            #        stats={};
            #        if writestats:
            #            stats.update({"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize});
            #        if writestorage:
            #            stats.update({"BSONSIZE":bsonsize});
            #        if dbpush and len(statsset)>0:
            #            db[basecollection].update(newindexdoc,{"$set":statsset});
            #        bsonsize=0;
            #        if writestats:
            #            templogstream.write("CPUTime: "+str(cputime)+" seconds\n");
            #            templogstream.write("MaxRSS: "+str(maxrss)+" bytes\n");
            #            templogstream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
            #        if writestorage:
            #            templogstream.write("BSONSize: "+str(bsonsize)+" bytes\n");
            #        templogstream.flush();
            #        shutil.copy(templogstream.name,logstream.name);
            #        logstream.seek(0);
            #        currline=line;
            #        line="";
            #        while line!=currline:
            #            line=logstream.readline();
            #        if writestats:
            #            line=logstream.readline();
            #            line=logstream.readline();
            #            line=logstream.readline();
            #        if writestorage:
            #            line=logstream.readline();
                    #line=logstream.readline();
            #    templogstream.write(line);
            #    templogstream.flush();
                        
        #if newindexdoc!="":
        #    statsset={};
        #    if markdone!="":
        #        statsset.update({modname+markdone:True});
        #    stats={};
        #    if writestats:
        #        stats.update({"CPUTIME":cputime,"MAXRSS":maxrss,"MAXVMSIZE":maxvmsize});
        #    if writestorage:
        #        stats.update({"BSONSIZE":bsonsize});
        #    if dbpush and len(statsset)>0:
        #        db[basecollection].update(newindexdoc,{"$set":statsset});
        #    if writestats:
        #        templogstream.write("CPUTime: "+str(cputime)+" seconds\n");
        #        templogstream.write("MaxRSS: "+str(maxrss)+" bytes\n");
        #        templogstream.write("MaxVMSize: "+str(maxvmsize)+" bytes\n");
        #    if writestorage:
        #        templogstream.write("BSONSize: "+str(bsonsize)+" bytes");
        #    templogstream.flush();
        #os.rename(templogstream.name,logstream.name);

        if niters>1:
            docstream.close();
            os.remove(workpath+"/"+jobstepname+".docs");
            #with tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempdocstream:
            #    for checkline in docstream:
            #        checklinedoc=json.loads(checkline.rstrip("\n"));
            #        if not all([x in checklinedoc.items() for x in basedocs[-1].items()]):
            #            tempdocstream.write(checkline);
            #            tempdocstream.flush();
            #    os.rename(tempdocstream.name,docstream.name);
        os.remove(workpath+"/"+jobstepname+".stat");

        #print "j";
        #sys.stdout.flush();
        
        #print "CPUTime: "+str(cputime)+" seconds";
        #print "MaxRSS: "+str(maxrss)+" bytes";
        #print "MaxVMSize: "+str(maxvmsize)+" bytes";
        #print "BSONSize: "+str(bsonsize)+" bytes";
        #logstream.close();
        mongoclient.close();
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