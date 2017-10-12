#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,os,linecache,traceback,subprocess,signal,tempfile,shutil,time,datetime;

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

def lineheader(jobnum,loglinenum):
    return datetime.datetime.now().strftime("%Y.%m.%d:%H.%M.%S")+":"+jobnum+":"+str(loglinenum)+": ";

def filenotempty(filepath,filename):
    try:
        if os.stat(filepath+"/"+filename).st_size>0:
           return True;
        else:
           return False;
    except OSError:
        return False;

def execscript(nstepthreads,batchname,stepmems,memunit,steptime,scriptcommand,scriptflags,scriptpath,scriptfile,args,outfile=subprocess.PIPE):
    scriptcommandflags=scriptcommand;
    if len(scriptflags)>0:
        scriptcommandflags+=" "+scriptflags;
    jobstring="mpirun -srun -n \""+nstepthreads+"\" -J \""+batchname+"\" --mem-per-cpu=\""+stepmems+memunit+"\" --time=\""+steptime+"\" "+scriptcommandflags+" \""+scriptpath+"/"+scriptfile+"\" "+" ".join(["\""+x+"\"" for x in args]);
    job=subprocess.Popen(jobstring,shell=True,stdout=outfile,preexec_fn=default_sigpipe);
    return job;

def getstats(fields,slurmjobid,stepid):
    statsstring="sacct -n -o '"+",".join(fields)+"' -j \""+slurmjobid+"."+str(stepid)+"\" | sed 's/G/MK/g' | sed 's/M/KK/g' | sed 's/K/000/g' | sed 's/\s\s*/ /g' | cut -d' ' -f1 --complement | head -c -2";
    stats=subprocess.Popen(statsstring,shell=True,stdout=subprocess.PIPE).communicate()[0].split(" ");
    return stats;

def chophead(docnum,filepath,origfile,headfile):
    with open(filepath+"/"+origfile,"r") as origstream, open(filepath+"/"+headfile,"w") as headstream, tempfile.NamedTemporaryFile(dir=filepath,delete=False) as tempstream:
        docscount=0;
        rmorig=False;
        docsline=origstream.readline();
        while docsline!="":
            docsstring="";
            while (docsline!="") and ("@" not in docsline):
                docsstring+=docsline;
                docsline=origstream.readline();
            docsstring+=docsline;
            docsline=origstream.readline();
            while (docsline!="") and any([docsline[:len(x)]==x for x in ["CPUTime","MaxRSS","MaxVMSize","BSONSize"]]):
                docsstring+=docsline;
                docsline=origstream.readline();
            if docscount<docnum:
                headstream.write(docsstring);
                headstream.flush();
                if docsline=="":
                    rmorig=True;
                    break;
            else:
                tempstream.write(docsstring);
                tempstream.flush();
            docscount+=1;
        os.rename(tempstream.name,origstream.name);
        if rmorig:
            os.remove(origstream.name);

#Main body
try:
    #Job info
    modname=sys.argv[1];
    controllername=sys.argv[2];
    scriptlanguage=sys.argv[3];
    scriptcommand=sys.argv[4];
    scriptflags=sys.argv[5];
    scriptext=sys.argv[6];
    outputlinemarkers=eval(sys.argv[7]);
    slurmjobid=sys.argv[8];
    jobnum=sys.argv[9];
    memunit=sys.argv[10];
    totmem=eval(sys.argv[11]);
    steptime=sys.argv[12];
    nbatch=eval(sys.argv[13]);
    nbatcheswrite=eval(sys.argv[14]);
    #Option info
    dbpush=sys.argv[15];
    markdone=sys.argv[16];
    writestats=sys.argv[17];
    #File system info
    mainpath=sys.argv[18];
    #Database info
    #dbindexes=sys.argv[19];
    #MPI info
    nstepthreads=sys.argv[19:];

    nsteps=len(nstepthreads);

    jobname=modname+"_"+controllername+"_job_"+jobnum;

    statepath=mainpath+"/state";
    scriptpath=mainpath+"/scripts";
    modulescriptpath=scriptpath+"/modules";
    workpath=mainpath+"/modules/"+modname+"/"+controllername+"/jobs";

    if filenotempty(statepath+"/ignorestrings",scriptlanguage):
        with open(statepath+"/ignorestrings/"+scriptlanguage,"r") as ignorestringsstream:
            ignorestrings=[x.rstrip("\n") for x in ignorestringsstream.readlines()];

    loglinenum=1
    serialid=0
    mergenum=1
    countmerge=0
    countdone=0
    usedsteps=0
    usedmem=0
    mergenums=[];
    batchnums=[];
    mergestepbatchnumslist=[];
    mergestepbatchnums=[];
    stepmems=[];
    stepjobs=[];
    stepstatuses=[];
    #batchstepids=[];
    mergestepids=[];
    rangesteps=range(nsteps);

    #Initiate job steps
    for stepind in rangesteps:
        stepnum=stepind+1;
        stepname=jobname+"_step_"+str(stepnum);
        if filenotempty(workpath,stepname+".docs"):
            batchnums+=[1];
            batchname=stepname+".batch."+str(batchnums[stepind]);
            mergenums+=[0];
            mergestepbatchnums+=[[]]
            chophead(nbatch,workpath,stepname+".docs",batchname+".docs");
            shutil.copy2(workpath+"/"+batchname+".docs",workpath+"/"+batchname+".temp");
            stepmems+=[0];
            stepjobs+=[1];
            #usedsteps+=1;
            #usedmem+=stepmems[stepind];
            stepstatuses+=["COLLECT"];
            #batchstepids+=[serialid];
            mergestepids+=[0];
            #serialid+=1;
            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Batch "+str(batchnums[stepind])+" Started");
            sys.stdout.flush();
            loglinenum+=1;
        else:
            if os.path.exists(workpath+"/"+stepname+".docs"):
                os.remove(workpath+"/"+stepname+".docs");
            stepstatuses+=["DONE"];
            countdone+=1;
            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Complete");
            sys.stdout.flush();
            loglinenum+=1;
    rangesteps=[i for i in rangesteps if stepstatuses[i]!="DONE"];
    #Loop while any step has unprocessed documents
    while len(rangesteps)>0:
        #print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Outer Loop");
        #sys.stdout.flush();
        #loglinenum+=1;
        jobpolls=all([type(stepjobs[i])==subprocess.Popen and stepjobs[i].poll()==None for i in rangesteps]);
        while jobpolls:
            time.sleep(0.1);
            jobpolls=all([type(stepjobs[i])==subprocess.Popen and stepjobs[i].poll()==None for i in rangesteps]);
        #Loop over all steps
        for stepind in rangesteps:
            #print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepind+1)+" Inner Loop");
            #sys.stdout.flush();
            #loglinenum+=1;
            #Check if a batch process has finished
            if (stepjobs[stepind]==1) or (stepjobs[stepind].poll()!=None):
                #print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepind+1)+" STATUS 1: "+stepstatuses[stepind]);
                #sys.stdout.flush();
                #loglinenum+=1;
                if stepstatuses[stepind]!="DONE":
                    stepnum=stepind+1;
                    stepname=jobname+"_step_"+str(stepnum);
                    batchname=stepname+".batch."+str(batchnums[stepind]);
                    mergename=jobname+".merge."+str(mergenum);
                    #print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" STATUS 2: "+stepstatuses[stepind]);
                    #sys.stdout.flush();
                    #loglinenum+=1;
                    if stepstatuses[stepind]=="COLLECT":
                        #Remove problem lines from module script output if necessary
                        if len(ignorestrings)>0:
                            with open(workpath+"/"+batchname+".temp","r") as batchtempstream, tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempstream:
                                for line in batchtempstream:
                                    if not any([x in line for x in ignorestrings]):
                                        tempstream.write(line);
                                        tempstream.flush();
                                os.rename(tempstream.name,batchtempstream.name);
                        #Check that batch process completed with no errors
                        skipped=False;
                        if filenotempty(workpath,batchname+".temp"):
                            with open(workpath+"/"+batchname+".temp","r") as batchtempstream, open(workpath+"/"+batchname+".out","w") as batchoutstream:
                                for line in batchtempstream:
                                    if all([line[:len(x)]!=x for x in outputlinemarkers]):
                                        skipped=True;
                                        os.remove(batchoutstream.name);
                                        break;
                                    else:
                                        batchoutstream.write(line);
                                    batchoutstream.flush();
                        else:
                            skipped=True;
                        if skipped:
                            #Log the exit code and traceback in a .batch.err.out file and remove .batch.temp file
                            with open(workpath+"/"+batchname+".err.out","w") as batcherrstream:
                                batcherrstream.write("Merge #: "+str(mergenum)+"\n");
                                batcherrstream.write("ExitCode: "+exitcode+"\n");
                                batcherrstream.flush();
                                if os.path.exists(workpath+"/"+batchname+".temp"):
                                    with open(workpath+"/"+batchname+".temp","r") as batchtempstream:
                                        for line in batchtempstream:
                                            batcherrstream.write(line);
                                            batcherrstream.flush();
                                        batcherrstream.write("\n");
                                        batcherrstream.flush();
                                        os.remove(batchtempstream.name);
                            #Put failed input documents from .batch.in into .batch.err.docs file
                            #if not os.path.exists(workpath+"/"+batchname+".in"):
                            #    print(workpath+"/"+batchname+".in");
                            #    print("batchnums[stepind]="+str(batchnums[stepind]));
                            #    print("mergenums[stepind]="+str(mergenums[stepind]));
                            #    print("stepstatuses[stepind]="+str(stepstatuses[stepind]));
                            #    sys.stdout.flush();
                            os.rename(workpath+"/"+batchname+".docs",workpath+"/"+batchname+".err.docs");
                            ##Restore failed documents to .docs file and remove .docs.in file
                            #with open(workpath+"/"+batchname+".in","r") as batchinstream, open(workpath+"/"+stepname+".docs","r") as docsstream, tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempstream:
                            #    for line in batchinstream:
                            #        tempstream.write(line);
                            #        tempstream.flush();
                            #    for line in docsstream:
                            #        tempstream.write(line);
                            #        tempstream.flush();
                            #    os.rename(tempstream.name,docsstream.name);
                            #    os.remove(batchinstream.name);
                            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Batch "+str(batchnums[stepind])+" Failed");
                            sys.stdout.flush();
                            loglinenum+=1;
                        else:
                            mergenums[stepind]=mergenum;
                            mergestepbatchnumslist+=[[stepind,batchnums[stepind]]];
                            os.remove(workpath+"/"+batchname+".temp");
                            #Append the .merge.temp file to the .merge.out file and rename .batch.in file to .batch.docs
                            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Batch "+str(batchnums[stepind])+" Completed Successfully");
                            sys.stdout.flush();
                            loglinenum+=1;
                            with open(workpath+"/"+batchname+".out","r") as batchoutstream, open(workpath+"/"+mergename+".in","a") as mergeinstream:
                                for line in batchoutstream:
                                    mergeinstream.write(line);
                                    mergeinstream.flush();
                                os.remove(batchoutstream.name);
                            ##os.rename(workpath+"/"+batchname+".in",workpath+"/"+batchname+".docs");
                            #Check if enough documents are completed in order to merge
                            countmerge+=1;
                            if (countmerge==nbatcheswrite) or (countdone==nsteps-1):
                                #Submit stats process on completed documents
                                stepmems[stepind]=(totmem-usedmem)/(nsteps-countdone-usedsteps);
                                stepjobs[stepind]=execscript(nstepthreads[stepind],mergename+"-"+str(serialid),str(stepmems[stepind]),memunit,steptime,"python","",scriptpath,"stats.py",[mainpath,modname,controllername,mergename+".in",dbpush,markdone,writestats],outfile=open(workpath+"/"+mergename+".temp","a"));
                                usedsteps+=1;
                                usedmem+=stepmems[stepind];
                                mergenum+=1;
                                mergestepbatchnums[stepind]=mergestepbatchnumslist;
                                mergestepbatchnumslist=[];
                                mergestepids[stepind]=serialid;
                                serialid+=1;
                                stepstatuses[stepind]="MERGE";
                                countmerge=0;
                                print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Merge "+str(mergenums[stepind])+" Started");
                                sys.stdout.flush();
                                loglinenum+=1;
                            else:
                                stepstatuses[stepind]="NEXT";
                    elif stepstatuses[stepind]=="MERGE":
                        usedsteps-=1;
                        usedmem-=stepmems[stepind];
                        tempmergenum=mergenums[stepind];
                        tempmergestepbatchnums=mergestepbatchnums[stepind];
                        tempmergename=jobname+".merge."+str(tempmergenum);
                        #Get merge process exit code
                        stats=getstats(["ExitCode"],slurmjobid,str(mergestepids[stepind]));
                        while any([x=="" for x in stats]):
                            time.sleep(0.1);
                            stats=getstats(["ExitCode"],slurmjobid,str(mergestepids[stepind]));
                        exitcode=stats[0];
                        #Check that merge process completed with no errors
                        skipped=False;
                        if (exitcode=="0:0") and filenotempty(workpath,tempmergename+".temp"):
                            with open(workpath+"/"+tempmergename+".temp","r") as mergetempstream:
                                for line in mergetempstream:
                                    if all([line[:len(x)]!=x for x in outputlinemarkers]):
                                        skipped=True;
                                        break;
                        else:
                            skipped=True;
                        if skipped:
                            #Log the exit code and traceback in a .merge.err.out file and remove .merge.temp file
                            with open(workpath+"/"+tempmergename+".err.out","w") as mergeerrstream:
                                mergeerrstream.write("ExitCode: "+exitcode+"\n");
                                mergeerrstream.flush();
                                if os.path.exists(workpath+"/"+tempmergename+".temp"):
                                    with open(workpath+"/"+tempmergename+".temp","r") as mergetempstream:
                                        for line in mergetempstream:
                                            mergeerrstream.write(line);
                                            mergeerrstream.flush();
                                        mergeerrstream.write("\n");
                                        mergeerrstream.flush();
                                        os.remove(mergetempstream.name);
                            #Put failed input documents from .batch.docs into .merge.err.docs file and failed output from .merge.in into .merge.err.in
                            os.rename(workpath+"/"+tempmergename+".in",workpath+"/"+tempmergename+".err.in");
                            for tempstepind,tempbatchnum in tempmergestepbatchnums:
                                tempstepnum=tempstepind+1;
                                tempstepname=jobname+"_step_"+str(tempstepnum);
                                tempbatchname=tempstepname+".batch."+str(tempbatchnum);
                                if filenotempty(workpath,tempbatchname+".docs"):
                                    with open(workpath+"/"+tempbatchname+".docs","r") as tempbatchdocsstream, open(workpath+"/"+tempmergename+".err.docs","w") as mergeerrdocsstream:
                                        for line in tempbatchdocsstream:
                                            mergeerrdocsstream.write(line);
                                            mergeerrdocsstream.flush();
                                        os.remove(tempbatchdocsstream.name);
                                #mergenums[tempstepind]=0;
                                #mergestepbatchnums[tempstepind]=[];
                                #if (stepstatuses[tempstepind]!="DONE") and (tempstepind!=stepind):
                                #    stepstatuses[tempstepind]="COLLECT";
                            ##Restore failed documents to .docs file and remove all .batch.docs and .merge.in files from failed steps
                            #os.remove(workpath+"/"+tempmergename+".in");
                            #for tempstepind,tempbatchnum in tempmergestepbatchnums:
                            #    tempstepnum=tempstepind+1;
                            #    tempstepname=jobname+"_step_"+str(tempstepnum);
                            #    tempbatchname=tempstepname+".batch."+str(tempbatchnum);
                            #    if filenotempty(workpath,tempbatchname+".docs"):
                            #        with open(workpath+"/"+tempbatchname+".docs","r") as tempbatchdocsstream, open(workpath+"/"+tempstepname+".docs","r") as docsstream, tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempstream:
                            #            for line in tempbatchdocsstream:
                            #                tempstream.write(line);
                            #                tempstream.flush();
                            #            for line in docsstream:
                            #                tempstream.write(line);
                            #                tempstream.flush();
                            #            os.rename(tempstream.name,docsstream.name);
                            #            os.remove(tempbatchdocsstream.name);
                            #    mergenums[tempstepind]=0;
                            #    mergestepbatchnums[tempstepind]=[];
                            #    if (stepstatuses[tempstepind]!="DONE") and (tempstepind!=stepind):
                            #        stepstatuses[tempstepind]="COLLECT";
                            stepstatuses[stepind]="NEXT";
                            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Merge "+str(tempmergenum)+" Failed");
                            sys.stdout.flush();
                            loglinenum+=1;
                        else:
                            #Remove all .batch.docs and .merge.in files from completed steps
                            os.remove(workpath+"/"+tempmergename+".in");
                            #print("Clearing "+str(["Step "+str(x[0]+1)+" Batch "+str(x[1]) for x in tempmergestepbatchnums]));
                            for tempstepind,tempbatchnum in tempmergestepbatchnums:
                                tempstepnum=tempstepind+1;
                                tempstepname=jobname+"_step_"+str(tempstepnum);
                                tempbatchname=tempstepname+".batch."+str(tempbatchnum);
                                #print(workpath+"/"+tempbatchname+".docs");
                                #sys.stdout.flush();
                                if filenotempty(workpath,tempbatchname+".docs"):
                                    os.remove(workpath+"/"+tempbatchname+".docs");
                                #mergenums[tempstepind]=0;
                                #mergestepbatchnums[tempstepind]=[];
                                #if (stepstatuses[tempstepind]!="DONE") and (tempstepind!=stepind):
                                #    stepstatuses[tempstepind]="COLLECT";
                            stepstatuses[stepind]="NEXT";
                            os.rename(workpath+"/"+tempmergename+".temp",workpath+"/"+tempmergename+".out");
                            mergestepids[stepind]=0;
                            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Merge "+str(tempmergenum)+" Completed Successfully");
                            sys.stdout.flush();
                            loglinenum+=1;
                    #else:
                    #    print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Not MERGE or COLLECT");
                    #    sys.stdout.flush();
                    #    loglinenum+=1;
                    #print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" STATUS 3: "+stepstatuses[stepind]);
                    #sys.stdout.flush();
                    #loglinenum+=1;
                    if stepstatuses[stepind]=="NEXT":
                        #Check if any documents are left in queue
                        if filenotempty(workpath,stepname+".docs"):
                            #Submit next batch process on current step
                            batchnums[stepind]+=1;
                            batchname=stepname+".batch."+str(batchnums[stepind]);
                            chophead(nbatch,workpath,stepname+".docs",batchname+".docs");
                            shutil.copy2(workpath+"/"+batchname+".docs",workpath+"/"+batchname+".temp");
                            stepmems[stepind]=0;
                            stepjobs[stepind]=1;
                            #usedsteps+=1;
                            #usedmem+=stepmems[stepind];
                            stepstatuses[stepind]="COLLECT";
                            #batchstepids[stepind]=serialid;
                            #serialid+=1;
                            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Batch "+str(batchnums[stepind])+" Started");
                            sys.stdout.flush();
                            loglinenum+=1;
                        else:
                            #Remove .docs file, mark current step as done, and reallocate node memory among remaining steps
                            print(lineheader(jobnum,loglinenum)+"Job "+jobnum+" Step "+str(stepnum)+" Complete");
                            sys.stdout.flush();
                            loglinenum+=1;
                            if os.path.exists(workpath+"/"+stepname+".docs"):
                                os.remove(workpath+"/"+stepname+".docs");
                            stepstatuses[stepind]="DONE";
                            countdone+=1;
        rangesteps=[i for i in rangesteps if stepstatuses[i]!="DONE"];
except Exception as e:
    PrintException();