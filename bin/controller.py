#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

#    DBCrunch: controller.py
#    Copyright (C) 2017 Ross Altman
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful, 
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import time
#Timer initialization
starttime = time.time()

import sys, os, glob, errno, re, linecache, fcntl, traceback, operator, functools, datetime, tempfile, json, yaml
from signal import signal, SIGPIPE, SIG_DFL
from subprocess import Popen, PIPE
from contextlib import contextmanager
#from pymongo import MongoClient

#Misc. function definitions
def PrintException():
    "If an exception is raised, print traceback of it to output log."
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
    print "More info: ", traceback.format_exc()

def default_sigpipe():
    signal(SIGPIPE, SIG_DFL)

def deldup(lst):
    "Delete duplicate elements in lst."
    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]]

class TimeoutException(Exception): pass

@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)

def updatereloadstate(reloadstatefilepath, reloadstatefilename, docbatch, endofdocs, filereadform = lambda x: x, filewriteform = lambda x: x, docwriteform = lambda x: x):
    #Compress docbatch to top tier that has completed
    endofdocsdone = []
    i = len(docbatch)-1
    while i >= 0:
        if endofdocs[i][1]:
            endofdocsdone += [endofdocs[i][0]]
            with open(reloadstatefilepath + "/" + reloadstatefilename + "FILE", "a") as reloadfilesstream:
                reloadfilesstream.write(filewriteform(endofdocs[i][0]) + "\n")
                reloadfilesstream.flush()
        else:
            if endofdocs[i][0] not in endofdocsdone:
                docline = "{" + docbatch[i].split("@")[1].split("\n")[0].split(".{")[1].split("<")[0]
                doc = json.loads(docline)
                with open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "a") as reloaddocsstream:
                    reloaddocsstream.write(filewriteform(endofdocs[i][0]) + "~" + docwriteform(doc) + "\n")
                    reloaddocsstream.flush()
        i -= 1
    #Update reloadstate files by removing the subdocument records of completed documents
    try:
        with open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "r") as reloaddocsstream, tempfile.NamedTemporaryFile(dir = reloadstatefilepath, delete = False) as tempstream:
            for reloaddocline in reloaddocsstream:
                fileline = filereadform(reloaddocline.rstrip("\n").split("~")[0])
                #If not a subdocument of any document in the list, keep it
                if fileline not in endofdocsdone:
                    tempstream.write(reloaddocline)
                    tempstream.flush()
            os.rename(tempstream.name, reloaddocsstream.name)
    except IOError:
        reloaddocsstream = open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "w")
        reloaddocsstream.close()

def printasfunc(*args):
    docbatch = list(args)[-1]
    for doc in docbatch:
        print(doc)
    sys.stdout.flush()
    return len(docbatch)

def writeasfunc(*args):
    arglist = list(args)
    docbatch = arglist[-1]
    with open(arglist[0], "a") as writestream:
        for doc in docbatch:
            writestream.write(doc)
            writestream.flush()
        writestream.write("\n")
        writestream.flush()
    return len(docbatch)

def reloadcrawl(reloadpath, reloadpattern, reloadstatefilepath, reloadstatefilename = "reloadstate", inputfunc = lambda x: {"nsteps": 1}, inputdoc = {"nsteps": 1}, action = printasfunc, filereadform = lambda x: x, filewriteform = lambda x: x, docwriteform = lambda x: x, timeleft = lambda: 1, counters = [1, 1], counterupdate = lambda x: None, resetstatefile = False, limit = None):
    docbatch = []
    endofdocs = []
    if resetstatefile:
        for x in ["FILE", "DOC"]:
            loadstatestream = open(reloadstatefilepath + "/" + reloadstatefilename + x, "w")
            loadstatestream.close()
    prevfiles = []
    try:
        reloadfilesstream = open(reloadstatefilepath + "/" + reloadstatefilename + "FILE", "r")
        for fileline in reloadfilesstream:
            file = filereadform(fileline.rstrip("\n"))
            prevfiles += [file]
    except IOError:
        reloadfilesstream = open(reloadstatefilepath + "/" + reloadstatefilename + "FILE", "w")
        pass
    reloadfilesstream.close()
    prevfiledocs = []
    try:
        reloaddocsstream = open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "r")
        for docline in reloaddocsstream:
            file = filereadform(docline.rstrip("\n").split("~")[0])
            doc = docline.rstrip("\n").split("~")[1]
            prevfiledocs += [[file, doc]]
    except IOError:
        reloaddocsstream = open(reloadstatefilepath + "/" + reloadstatefilename + "DOC", "w")
        pass
    reloaddocsstream.close()
    if (limit == None) or (counters[1] <= limit):
        if timeleft() > 0:
            filescurs = glob.iglob(reloadpath + "/" + reloadpattern)
        else:
            try:
                with time_limit(int(timeleft())):
                    filescurs = glob.iglob(reloadpath + "/" + reloadpattern)
            except TimeoutException(msg):
                filescurs = iter(())
                pass
    else:
        return counters
    filepath = next(filescurs, None)
    while (filepath != None) and (timeleft() > 0) and ((limit == None) or (counters[1] <= limit)):
        file = filepath.split("/")[-1]
        #print(file)
        #sys.stdout.flush()
        if file not in prevfiles:
            docsstream = open(filepath, "r")
            docsline = docsstream.readline()
            while (docsline != "") and (timeleft() > 0) and ((limit == None) or (counters[1] <= limit)):
                docsstring = ""
                while (docsline != "") and ("@" not in docsline):
                    docsstring += docsline
                    docsline = docsstream.readline()
                #docsline = docsstream.readline()
                doc = docwriteform(json.loads("{" + docsline.rstrip("\n").split(".{")[1].split("<")[0]))
                while [file, doc] in prevfiledocs:
                    #print([file, doc])
                    #sys.stdout.flush()
                    docsline = docsstream.readline()
                    while (docsline != "") and any([docsline[:len(x)] == x for x in ["CPUTime", "MaxRSS", "MaxVMSize", "BSONSize"]]):
                        docsline = docsstream.readline()
                    docsstring = ""
                    while (docsline != "") and ("@" not in docsline):
                        docsstring += docsline
                        docsline = docsstream.readline()
                    if docsline == "":
                        break
                    doc = docwriteform(json.loads("{" + docsline.rstrip("\n").split(".{")[1].split("<")[0]))
                if docsline != "":
                    #docslinehead = docsline.rstrip("\n").split(".")[0]
                    #doc = json.loads(".".join(docsline.rstrip("\n").split(".")[1:]))
                    docsstring += docsline.rstrip("\n") + "<" + file + "\n"
                    #print(file + "~" + docsline.rstrip("\n"))
                    #sys.stdout.flush()
                    docsline = docsstream.readline()
                while (docsline != "") and any([docsline[:len(x)] == x for x in ["CPUTime", "MaxRSS", "MaxVMSize", "BSONSize"]]):
                    docsstring += docsline
                    docsline = docsstream.readline()
                docbatch += [docsstring]
                if docsline == "":
                    endofdocs += [[file, True]]
                else:
                    endofdocs += [[file, False]]
                if (len(docbatch) == inputdoc["nsteps"]) or not (timeleft() > 0):
                    if (limit != None) and (counters[1] + len(docbatch) > limit):
                        docbatch = docbatch[:limit-counters[1] + 1]
                    while len(docbatch) > 0:
                        nextdocind = action(counters, inputdoc, docbatch)
                        if nextdocind == None:
                            break
                        docbatchpass = docbatch[nextdocind:]
                        endofdocspass = endofdocs[nextdocind:]
                        docbatchwrite = docbatch[:nextdocind]
                        endofdocswrite = endofdocs[:nextdocind]
                        updatereloadstate(reloadstatefilepath, reloadstatefilename, docbatchwrite, endofdocswrite, filereadform = filereadform, filewriteform = filewriteform, docwriteform = docwriteform)
                        counters[0] += 1
                        counters[1] += nextdocind
                        counterupdate(counters)
                        docbatch = docbatchpass
                        endofdocs = endofdocspass
                        inputfuncresult = inputfunc(docbatchpass)
                        if inputfuncresult == None:
                            break
                        inputdoc.update(inputfuncresult)
        filepath = next(filescurs, None)
    while len(docbatch) > 0:
        if (limit != None) and (counters[1] + len(docbatch) > limit):
            docbatch = docbatch[:limit-counters[1] + 1]
        nextdocind = action(counters, inputdoc, docbatch)
        if nextdocind == None:
            break
        docbatchpass = docbatch[nextdocind:]
        endofdocspass = endofdocs[nextdocind:]
        docbatchwrite = docbatch[:nextdocind]
        endofdocswrite = endofdocs[:nextdocind]
        updatereloadstate(reloadstatefilepath, reloadstatefilename, docbatchwrite, endofdocswrite, filereadform = filereadform, filewriteform = filewriteform, docwriteform = docwriteform)
        counters[0] += 1
        counters[1] += nextdocind
        counterupdate(counters)
        docbatch = docbatchpass
        endofdocs = endofdocspass
    return counters

'''
def py2mat(lst):
    "Converts a Python list to a string depicting a list in Mathematica format."
    return str(lst).replace(" ", "").replace("[", "{").replace("]", "}")

def mat2py(lst):
    "Converts a string depicting a list in Mathematica format to a Python list."
    return eval(str(lst).replace(" ", "").replace("{", "[").replace("}", "]"))

def deldup(lst):
    "Delete duplicate elements in lst."
    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]]

def transpose_list(lst):
    "Get the transpose of a list of lists."
    return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))]

#Module-specific function definitions
def collectionfind(db, collection, query, projection):
    if projection == "Count":
        result = db[collection].find(query).count()
    else:
        result = list(db[collection].find(query, projection))
    #return [dict(zip(y.keys(), [mat2py(y[x]) for x in y.keys()])) for y in result]
    return result

def collectionfieldexists(db, collection, field):
    result = db[collection].find({}, {"_id": 0, field: 1}).limit(1).next() != {}
    return result

def listindexes(db, collection, filters, indexes = ["POLYID", "GEOMN", "TRIANGN", "INVOLN"]):
    trueindexes = [x for x in indexes if collectionfieldexists(db, collection, x)]
    if len(trueindexes) == 0:
        return []
    indexlist = deldup([dict([(x, z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters])
    return indexlist

def sameindexes(filter1, filter2, indexes = ["POLYID", "GEOMN", "TRIANGN", "INVOLN"]):
    return all([filter1[x] == filter2[x] for x in filter1 if (x in indexes) and (x in filter2)])

def querydatabase(db, controllerconfigdoc["db"]["query"], tiers = ["POLY", "GEOM", "TRIANG", "INVOL"]):
    sortedprojcontrollerconfigdoc["db"]["query"] = sorted([y for y in controllerconfigdoc["db"]["query"] if y[2] != "Count"], key = lambda x: (len(x[1]), tiers.index(x[0])), reverse = True)
    maxcountquery = [] if len(controllerconfigdoc["db"]["query"]) == len(sortedprojcontrollerconfigdoc["db"]["query"]) else [max([y for y in controllerconfigdoc["db"]["query"] if y not in sortedprojcontrollerconfigdoc["db"]["query"]], key = lambda x: len(x[1]))]
    sortedcontrollerconfigdoc["db"]["query"] = sortedprojcontrollerconfigdoc["db"]["query"] + maxcountquery
    totalresult = collectionfind(db, *sortedcontrollerconfigdoc["db"]["query"][0])
    if sortedcontrollerconfigdoc["db"]["query"][0][2] == "Count":
        return totalresult
    for i in range(1, len(sortedcontrollerconfigdoc["db"]["query"])):
        indexlist = listindexes(db, sortedcontrollerconfigdoc["db"]["query"][i][0], totalresult)
        if len(indexlist) == 0:
            orgroup = sortedcontrollerconfigdoc["db"]["query"][i][1]
        else:
            orgroup = dict(sortedcontrollerconfigdoc["db"]["query"][i][1].items() + {"$or": indexlist}.items())
        nextresult = collectionfind(db, sortedcontrollerconfigdoc["db"]["query"][i][0], orgroup, sortedcontrollerconfigdoc["db"]["query"][i][2])
        if sortedcontrollerconfigdoc["db"]["query"][i][2] == "Count":
            return nextresult
        totalresult = [dict(x.items() + y.items()) for x in totalresult for y in nextresult if sameindexes(x, y)]
    return totalresult

def querytofile(db, controllerconfigdoc["db"]["query"], inputpath, inputfile, tiers = ["POLY", "GEOM", "TRIANG", "INVOL"]):
    results = querydatabase(db, controllerconfigdoc["db"]["query"], tiers)
    with open(inputpath + "/" + inputfile, "a") as inputstream:
        for doc in results:
            json.dump(doc, inputstream, separators = (', ', ':'))
            inputstream.write("\n")
            inputstream.flush()

def py2matdict(dic):
    return str(dic).replace("u'", "'").replace(" ", "").replace("'", "\\\"").replace(":", "->")
'''

def timestamp2unit(timestamp, unit = "seconds"):
    if timestamp == "infinite":
        return timestamp
    else:
        days = 0
        if "-" in timestamp:
            daysstr, timestamp = timestamp.split("-")
            days = int(daysstr)
        hours, minutes, seconds = [int(x) for x in timestamp.split(":")]
        hours += days * 24
        minutes += hours * 60
        seconds += minutes * 60
        if unit == "seconds":
            return seconds
        elif unit == "minutes":
            return float(seconds) / 60.
        elif unit == "hours":
            return float(seconds) / (60. * 60.)
        elif unit == "days":
            return float(seconds) / (60. * 60. * 24.)
        else:
            return 0

def seconds2timestamp(seconds):
    timestamp = ""
    days = str(seconds / (60 * 60 * 24))
    remainder = seconds % (60 * 60 * 24)
    hours = str(remainder / (60 * 60)).zfill(2)
    remainder = remainder % (60 * 60)
    minutes = str(remainder / 60).zfill(2)
    remainder = remainder % 60
    seconds = str(remainder).zfill(2)
    if days != "0":
        timestamp += days + "-"
    timestamp += hours + ":" + minutes + ":" + seconds
    return timestamp

#def contractededjobname2jobdocs(jobname, dbindexes):
#    indexsplit = [[eval(y) for y in x.split("_")] for x in jobname.lstrip("[").rstrip("]").split(",")]
#    return [dict([(dbindexes[i], x[i]) for i in range(len(dbindexes))]) for x in indexsplit]

#def jobstepname2indexdoc(jobstepname, dbindexes):
#    indexsplit = jobstepname.split("_")
#    nindexes = min(len(indexsplit)-2, len(dbindexes))
#    #return dict([(dbindexes[i], eval(indexsplit[i + 2])) for i in range(nindexes)])
#    return dict([(dbindexes[i], eval(indexsplit[i + 2]) if indexsplit[i + 2].isdigit() else indexsplit[i + 2]) for i in range(nindexes)])

#def indexdoc2jobstepname(doc, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], dbindexes):
#    return controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_" + "_".join([str(doc[x]) for x in dbindexes if x in doc.keys()])

def indexsplit2indexdoc(indexsplit, dbindexes):
    nindexes = min(len(indexsplit), len(dbindexes))
    return dict([(dbindexes[i], eval(indexsplit[i]) if indexsplit[i].isdigit() else indexsplit[i]) for i in range(nindexes)])

def indexdoc2indexsplit(doc, dbindexes):
    return [str(doc[x]) for x in dbindexes if x in doc.keys()]

#def doc2jobjson(doc, dbindexes):
#    return dict([(y, doc[y]) for y in dbindexes])

#def jobnameexpand(jobname):
#    bracketexpanded = jobname.rstrip("]").split("[")
#    return [bracketexpanded[0] + x for x in bracketexpanded[1].split(",")]

#def jobstepnamescontract(jobstepnames):
#    "3 because controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"] are first two."
#    bracketcontracted = [x.split("_") for x in jobstepnames]
#    return '_'.join(bracketcontracted[0][:-3] + ["["]) + ', '.join(['_'.join(x[-3:]) for x in bracketcontracted]) + "]"

#def formatinput(doc):#, controllerconfigdoc["module"]["language"]):
#    #if controllerconfigdoc["module"]["language"] == "python":
#    #    formatteddoc = doc
#    #elif controllerconfigdoc["module"]["language"] == "sage":
#    #    formatteddoc = doc
#    #elif controllerconfigdoc["module"]["language"] == "mathematica":
#    #    formatteddoc = mongojoin.pythondictionary2mathematicarules(doc)
#    #return str(formatteddoc).replace(" ", "")
#    return json.dumps(doc, separators = (',',':')).replace("\"", "\\\"")

def dir_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.stat(fp).st_blocks * 512
            except OSError:
                pass
    return total_size

def storageleft(path, controllerstoragelimit):
    if controllerstoragelimit == None:
        return True
    else:
        return dir_size(path) < controllerstoragelimit

def getpartitiontimelimit(partition, moduletimelimit, modulebuffertime):
    maxtimelimit = get_maxtimelimit(partition)
    #print "getpartitiontimelimit"
    #print "sinfo -h -o '%l %P' | grep -E '" + partition + "\*?\s*$' | sed 's/\s\s*/ /g' | sed 's/*//g' | cut -d' ' -f1 | head -c -1"
    #print ""
    #sys.stdout.flush()
    if moduletimelimit in [None, "", "infinite"]:
        partitiontimelimit = maxtimelimit
    else:
        if maxtimelimit == "infinite":
            partitiontimelimit = moduletimelimit
        else:
            partitiontimelimit = min(maxtimelimit, moduletimelimit, key = timestamp2unit)
    if partitiontimelimit == "infinite":
        buffertimelimit = partitiontimelimit
    else:
        buffertimelimit = seconds2timestamp(timestamp2unit(partitiontimelimit) - timestamp2unit(modulebuffertime))
    return [partitiontimelimit, buffertimelimit]

def timeleft(starttime, buffertimelimit):
    "Determine if runtime limit has been reached."
    #print str(time.time()-starttime) + " " + str(timestamp2unit(buffertimelimit))
    #sys.stdout.flush()
    if buffertimelimit == "infinite":
        return 1
    else:
        return timestamp2unit(buffertimelimit)-(time.time()-starttime)

#def timeleftq(controllerjobid, buffertimelimit):
#    "Determine if runtime limit has been reached."
#    if buffertimelimit == "infinite":
#        return True
#    else:
#        timestats = Popen("sacct -n -j \"" + controllerjobid + "\" -o 'Elapsed,Timelimit' | head -n1 | sed 's/^\s*//g' | sed 's/\s\s*/ /g' | tr ' ' ',' | tr '\n' ',' | head -c -2", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
#        elapsedtime, timelimit = timestats.split(",")
#        return timestamp2unit(elapsedtime) < timestamp2unit(buffertimelimit)

#def clusterjobslotsleft(globalmaxjobcount):
#    njobs = eval(Popen("squeue -h -r | wc -l", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
#    return njobs < globalmaxjobcount

def availlicensecount(localbinpath, licensescript, sublicensescript):
    if licensescript != None and os.path.isfile(licensescript):
        navaillicenses = [eval(Popen(licensescript, shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])]
    else:
        raise IOError(errno.ENOENT, 'No license script available.', licensescript)
    if sublicensescript != None and os.path.isfile(sublicensescript):
        navaillicenses += [eval(Popen(sublicensescript, shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])]
    #print "licensecount"
    #print binpath + "/" + controllerconfigdoc["module"]["language"] + "licensecount.bash"
    #print ""
    #sys.stdout.flush()
    return navaillicenses

def pendlicensecount(username, needslicense):
    npendjobsteps = 0
    npendjobthreads = 0
    #grepmods = "|".join(modlist)
    #pendjobnamespaths = Popen("squeue -h -u " + username + " -o '%T %j %.130Z' | grep 'PENDING' | cut -d' ' -f2,3 | grep -E \"(" + grepmods + ")\" | grep -v \"controller\" | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
    pendjobnamespaths = get_pendjobnamespaths(username)
    if len(pendjobnamespaths) > 0:
        for pendjobname, pendjobpath in pendjobnamespaths:
            pendjobnamesplit = pendjobname.split("_")
            modname = pendjobnamesplit[0]
            controllername = pendjobnamesplit[1]
            if os.path.exists(pendjobpath + "/../crunch_" + modname + "_" + controllername + "_controller.job"):
                nsteps = 1-eval(pendjobnamesplit[5])
                njobthreads = eval(Popen("echo \"$(cat " + pendjobpath + "/" + pendjobname + ".job | grep -E \"njobstepthreads\[[0-9]+\]=\" | cut -d'=' -f2 | tr '\n' '+' | head -c -1)\" | bc | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
                if needslicense:
                    npendjobsteps += nsteps
                    npendjobthreads += njobthreads
    return [npendjobsteps, npendjobthreads]

def licensecount(username, needslicense, localbinpath, licensescript, sublicensescript):
    navaillicensesplit = availlicensecount(localbinpath, licensescript, sublicensescript)
    npendlicensesplit = pendlicensecount(username, needslicense)
    try:
        nlicensesplit = [navaillicensesplit[i] - npendlicensesplit[i] for i in range(len(navaillicensesplit))]
    except IndexError:
        raise
    return nlicensesplit

def clusterjobslotsleft(username, modname, controllername, globalmaxjobcount, localmaxjobcount):
    nglobaljobs = get_nglobaljobs(username)
    globaljobsleft = (nglobaljobs < globalmaxjobcount)
    nlocaljobs = get_nlocaljobs(username, modname, controllername)
    localjobsleft = (nlocaljobs < localmaxjobcount)
    return (globaljobsleft and localjobsleft)

def clusterlicensesleft(nlicensesplit, minthreads):#, minnsteps = 1):
    nlicenses = nlicensesplit[0]
    licensesleft = (nlicenses > 0);#(navaillicenses >= minnsteps))
    if len(nlicensesplit) > 1:
        nsublicenses = nlicensesplit[1]
        licensesleft = (licensesleft and (nsublicenses >= minthreads))
    return licensesleft

#def islimitreached(controllerpath, querylimit):
#    if querylimit == None:
#        return False
#    else:
#        #if niters == 1:
#        #    ntot = eval(Popen("echo \"$(cat " + controllerpath + "/jobs/*.error 2>/dev/null | wc -l)+$(cat " + controllerpath + "/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
#        #elif niters > 1:
#        ntot = eval(Popen("echo \"$(cat $(find " + controllerpath + "/jobs/ -type f -name '*.docs' -o -name '*.docs.pend' 2>/dev/null) 2>/dev/null | wc -l)+$(cat " + controllerpath + "/jobs/*.job.log 2>/dev/null | grep 'CPUTime' | wc -l)\" | bc | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0])
#        return ntot >= querylimit

def submitjob(controllerpath, jobname, jobinfo, nnodes, ncores, niters, nbatch, resubmit = False):
    jobspath = controllerpath + "/jobs"
    jobid = get_submitjob(jobspath, jobname)
    partitions = [x[1] for x in jobinfo]
    partitions = sorted([partitions[i] for i in range(len(partitions)) if partitions[i] not in partitions[:i]])
    totmem = sum([x[3] for x in jobinfo])
    #Print information about controller job submission
    if resubmit:
        #jobid = submitcomm.split(' ')[-1]
        #maketop = Popen("scontrol top " + jobid, shell = True, stdout = PIPE, preexec_fn = default_sigpipe)
        print ""
        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
        print "Resubmitted batch job " + jobid + " as " + jobname + " on partition(s) " + ','.join(partitions) + " with " + str(nnodes) + " nodes, " + str(ncores) + " CPU(s), and " + str(totmem) + "MB RAM allocated."
        for jobstepnum in range(len(jobinfo)):
            #with open(jobspath + "/" + jobstepnames[i] + ".error", "a") as statstream:
            #    statstream.write(jobstepnames[i] + ", -1:0, False\n")
            #    statstream.flush()
            print "....With job step " + jobid + "." + str(jobstepnum) + " as " + jobinfo[jobstepnum][0] + " in batches of " + str(nbatch) + "/" + str(niters) + " iteration(s) on partition " + jobinfo[jobstepnum][1] + " with " + str(jobinfo[jobstepnum][2]) + " CPU(s) and " + str(jobinfo[jobstepnum][3]) + "MB RAM allocated."
        print ""
        print ""
    else:
        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
        print "Submitted batch job " + jobid + " as " + jobname + " on partition(s) " + ','.join(partitions) + " with " + str(nnodes) + " nodes, " + str(ncores) + " CPU(s), and " + str(totmem) + "MB RAM allocated."
        for jobstepnum in range(len(jobinfo)):
            #with open(jobspath + "/" + jobstepnames[i] + ".error", "a") as statstream:
            #    statstream.write(jobstepnames[i] + ", -1:0, False\n")
            #    statstream.flush()
            print "....With job step " + jobid + "." + str(jobstepnum) + " as " + jobinfo[jobstepnum][0] + " in batches of " + str(nbatch) + "/" + str(niters) + " iteration(s) on partition " + jobinfo[jobstepnum][1] + " with " + str(jobinfo[jobstepnum][2]) + " CPU(s) and " + str(jobinfo[jobstepnum][3]) + "MB RAM allocated."
        print ""
    sys.stdout.flush()

def submitcontrollerjob(controllerpath, jobname, controllernnodes, controllerncores, partition, maxmemorypernode, resubmit = False):
    jobid = get_submitjob(controllerpath, jobname)
    #Print information about controller job submission
    if resubmit:
        #jobid = submitcomm.split(' ')[-1]
        #maketop = Popen("scontrol top " + jobid, shell = True, stdout = PIPE, preexec_fn = default_sigpipe)
        print ""
        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
        print "Resubmitted batch job " + jobid + " as " + jobname + " on partition " + partition + " with " + controllernnodes + " nodes, " + controllerncores + " CPU(s), and " + str(maxmemorypernode / 1000000) + "MB RAM allocated."
        print ""
        print ""
    else:
        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
        print "Submitted batch job " + jobid + " as " + jobname + " on partition " + partition + " with " + controllernnodes + " nodes, " + controllerncores + " CPU(s), and " + str(maxmemorypernode / 1000000) + "MB RAM allocated."
        print ""
    sys.stdout.flush()

#def skippedjobslist(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], workpath):
#    jobsrunning = userjobsrunninglist(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
#    blankfilesstring = Popen("find '" + workpath + "' -maxdepth 1 -type f -name '*.out' -empty | rev | cut -d'/' -f1 | rev | cut -d'.' -f1 | cut -d'_' -f1,2 --complement | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0]
#    if blankfilesstring == '':
#        return []
#    else:
#        blankfiles = blankfilesstring.split(",")
#        skippedjobs = [controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_" + x for x in blankfiles if x not in jobsrunning]
#        return skippedjobs

def requeueskippedqueryjobs(controllerconfigdoc, controllerpath, querystatefilename, counters, counterstatefile, counterheader, dbindexes):
    try:
        skippeddoccount = 0
        skippedjobfiles = []
        skippedjobnums = []
        skippedjobdocs = []
        with open(controllerpath + "/skipped", "r") as skippedstream:#, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream:
            skippedheader = skippedstream.readline()
            #tempstream.write(skippedheader)
            #tempstream.flush()
            for line in skippedstream:
                skippedjobfile, exitcode, resubmitq = line.rstrip("\n").split(",")
                if eval(resubmitq):
                    skippedjobfilesplit = skippedjobfile.split("_")
                    if skippedjobfilesplit[:2] == [controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]]:
                        #if niters == 1:
                        skippedjobnums += [re.sub(".*_job_([0-9] + )[_\.].*", r"\1", skippedjobfile)]
                        skippedjobfiles += [skippedjobfile]
                        #elif niters > 1:
                        skippedjobfiledocs = []
                        with open(controllerpath + "/jobs/" + skippedjobfile, "r") as skippeddocstream:
                            for docsline in skippeddocstream:
                                doc = json.loads(docsline.rstrip("\n"))
                                skippedjobfiledocs += [controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_" + "_".join(indexdoc2indexsplit(doc, dbindexes))]
                                skippeddoccount += 1
                        skippedjobdocs += [skippedjobfiledocs]
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs")
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs.in")
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".error")
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".batch.log")
                    #else:
                    #    tempstream.write(line)
                    #    tempstream.flush()
                #else:
                #    tempstream.write(line)
                #    tempstream.flush()
            #os.rename(tempstream.name, skippedstream.name)
        if skippeddoccount > 0:
            querystatetierfilenames = Popen("find " + controllerpath + "/ -maxdepth 1 -type f -name '" + querystatefilename + "*' 2>/dev/null | rev | cut -d'/' -f1 | rev | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(",")
            for querystatetierfilename in querystatetierfilenames:
                try:
                    if querystatetierfilename == querystatefilename + controllerconfigdoc["db"]["basecollection"]:
                        #with open(controllerpath + "/" + querystatetierfilename, "a") as querystatefilestream:
                        #    for i in range(len(skippedjobs)):
                        #        line = skippedjobs[i]
                        #        if i < len(skippedjobs)-1:
                        #            line += "\n"
                        #        querystatefilestream.write(line)
                        #        querystatefilestream.flush()
                        with open(controllerpath + "/" + querystatetierfilename, "r") as querystatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                            #print(skippedjobdocs)
                            #print("")
                            #sys.stdout.flush()
                            for querystateline in querystatefilestream:
                                querystatelinestrip = querystateline.rstrip("\n")
                                skipped = False
                                i = 0
                                #print(linestrip)
                                #sys.stdout.flush()
                                while i < len(skippedjobdocs):
                                    if querystatelinestrip in skippedjobdocs[i]:
                                        skippedjobdocs[i].remove(querystatelinestrip)
                                        if len(skippedjobdocs[i]) == 0:
                                            skippedjobfile = skippedjobfiles[i]
                                            skippedjobnum = skippedjobnums[i]
                                            if not os.path.isdir(controllerpath + "/jobs/reloaded"):
                                                os.mkdir(controllerpath + "/jobs/reloaded")
                                            os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
                                            del skippedjobdocs[i]
                                            del skippedjobfiles[i]
                                            del skippedjobnums[i]
                                            try:
                                                skippedjobfilein = skippedjobfile.replace(".docs", ".in")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                            except:
                                                pass
                                            try:
                                                skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                            except:
                                                pass
                                            try:
                                                skippedjobfileout = skippedjobfile.replace(".docs", ".out")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                            except:
                                                pass
                                            if skippedjobnums.count(skippedjobnum) == 0:
                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
                                                    os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                                    if not ((".merge." in file) and (".out" in file)):
                                                        os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                            with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
                                                skippedheader = skippedstream.readline()
                                                tempstream2.write(skippedheader)
                                                tempstream2.flush()
                                                for skippedline in skippedstream:
                                                    if not skippedjobfile in skippedline:
                                                        tempstream2.write(skippedline)
                                                        tempstream2.flush()
                                                os.rename(tempstream2.name, skippedstream.name)
                                            i -= 1
                                        skipped = True
                                    i += 1
                                if not skipped:
                                    tempstream1.write(querystateline)
                                    tempstream1.flush();    
                            os.rename(tempstream1.name, querystatefilestream.name)
                    else:
                    #if querystatetierfilename != querystatefilename + controllerconfigdoc["db"]["basecollection"]:
                        with open(controllerpath + "/" + querystatetierfilename, "r") as querystatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                            for querystateline in querystatefilestream:
                                querystatelinestrip = querystateline.rstrip("\n")
                                skipped = False
                                i = 0
                                while i < len(skippedjobdocs):
                                    if any([querystatelinestrip + "_" in x for x in skippedjobdocs[i]]):
                                        for x in skippedjobdocs[i]:
                                            if querystatelinestrip + "_" in x:
                                                skippedjobdocs[i].remove(x)
                                        if len(skippedjobdocs[i]) == 0:
                                            skippedjobfile = skippedjobfiles[i]
                                            skippedjobnum = skippedjobnums[i]
                                            if not os.path.isdir(controllerpath + "/jobs/reloaded"):
                                                os.mkdir(controllerpath + "/jobs/reloaded")
                                            os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
                                            del skippedjobdocs[i]
                                            del skippedjobfiles[i]
                                            del skippedjobnums[i]
                                            try:
                                                skippedjobfilein = skippedjobfile.replace(".docs", ".in")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                            except:
                                                pass
                                            try:
                                                skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                            except:
                                                pass
                                            try:
                                                skippedjobfileout = skippedjobfile.replace(".docs", ".out")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                            except:
                                                pass
                                            if skippedjobnums.count(skippedjobnum) == 0:
                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
                                                    os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                                for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                                    if not ((".merge." in file) and (".out" in file)):
                                                        os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                            with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
                                                skippedheader = skippedstream.readline()
                                                tempstream2.write(skippedheader)
                                                tempstream2.flush()
                                                for skippedline in skippedstream:
                                                    if not skippedjobfile in skippedline:
                                                        tempstream2.write(skippedline)
                                                        tempstream2.flush()
                                                os.rename(tempstream2.name, skippedstream.name)
                                            i -= 1
                                        skipped = True
                                    i += 1
                                if not skipped:
                                    tempstream1.write(querystateline)
                                    tempstream1.flush()
                            os.rename(tempstream1.name, querystatefilestream.name)
                except IOError:
                    print "File path \"" + controllerpath + "/" + querystatetierfilename + "\" does not exist."
                    sys.stdout.flush()
            counters[1] -= skippeddoccount
            docounterupdate(counters, counterstatefile, counterheader)
    except IOError:
        print "File path \"" + controllerpath + "/skipped\" does not exist."
        sys.stdout.flush()

def requeueskippedreloadjobs(controllerconfigdoc, controllerpath, reloadstatefilename, reloadpath, counters, counterstatefile, counterheader, dbindexes):
    try:
        skippeddoccount = 0
        skippedjobfiles = []
        skippedjobnums = []
        skippeddocs = []
        skippedreloadfiles = []
        with open(controllerpath + "/skipped", "r") as skippedstream:#, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream:
            skippedheader = skippedstream.readline()
            #tempstream.write(skippedheader)
            #tempstream.flush()
            for line in skippedstream:
                skippedjobfile, exitcode, resubmitq = line.rstrip("\n").split(",")
                if eval(resubmitq):
                    skippedjobfilesplit = skippedjobfile.split("_")
                    if skippedjobfilesplit[:2] == [controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]]:
                        #if niters == 1:
                        #elif niters > 1:
                        with open(controllerpath + "/jobs/" + skippedjobfile, "r") as skippeddocstream:
                            #skippeddocsline = skippeddocstream.readline()
                            for skippeddocsline in skippeddocstream:
                                if "@" in skippeddocsline:
                                    #print skippeddocsline
                                    #sys.stdout.flush()
                                    skippedinfoline = ("{" + skippeddocsline.rstrip("\n").split(".{")[1]).split("<")
                                    #print skippedinfoline
                                    #sys.stdout.flush()
                                    skippedjobfiles += [skippedjobfile]
                                    skippedjobnums += [re.sub(".*_job_([0-9] + )[_\.].*", r"\1", skippedjobfile)]
                                    skippeddocs += ["_".join(indexdoc2indexsplit(json.loads(skippedinfoline[0]), dbindexes))]
                                    skippedreloadfiles += [skippedinfoline[1]]
                                    skippeddoccount += 1
                                    #if skippedfileposdoc[reloadstatefilename + "FILE"] in reloadfiles.keys():
                                    #    reloadpos += [skippedfileposdoc[reloadstatefilename + "POS"]]
                                    #skippeddoc = json.loads(skippedjobids)
                                    #skippedjobfiles += [skippedjobfile]
                                    #skippedjobpos += ["_".join(indexdoc2indexsplit(skippeddoc, dbindexes))]
                                    ##print(skippeddoc)
                                    ##sys.stdout.flush()
                                    #skippeddocsline = skippeddocstream.readline()
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs")
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".docs.in")
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".error")
                        #os.remove(controllerpath + "/jobs/" + skippedjob + ".batch.log")
                    #else:
                    #    tempstream.write(line)
                    #    tempstream.flush()
                #else:
                #    tempstream.write(line)
                #    tempstream.flush()
            #os.rename(tempstream.name, skippedstream.name)
            #if len(skippeddocs) > 0:
            #    reloadfilescurs = glob.iglob(controllerpath + "/jobs/" + reloadpattern)
            #    skippedjobfilescollect = []
            #    skippedjobposcollect = []
            #    for reloadfile in reloadfilescurs:
            #        #print(reloadfile)
            #        #sys.stdout.flush()
            #        with open(reloadfile, "r") as reloaddocstream:
            #            reloaddocsline = reloaddocstream.readline()
            #            while (reloaddocsline != "") and (len(skippedjobpos) > 0):
            #                while (reloaddocsline != "") and ("@" not in reloaddocsline):
            #                    reloaddocsline = reloaddocstream.readline()
            #                if "@" in reloaddocsline:
            #                    reloaddoc = json.loads("{" + reloaddocsline.rstrip("\n").split(".{")[1])
            #                    reloadid = "_".join(indexdoc2indexsplit(reloaddoc, dbindexes))
            #                    if reloadid in skippedjobpos:
            #                        skippedjobindex = skippedjobpos.index(reloadid)
            #                        reloadfiles += [reloadfile.split("/")[-1]]
            #                        reloadids += [reloadid]
            #                        skippeddoccount += 1
            #                        #skippedjobdocs.remove(reloaddocsline)
            #                        skippedjobfilescollect += [skippedjobfiles[skippedjobindex]]
            #                        skippedjobposcollect += [skippedjobpos[skippedjobindex]]
            #                        del skippedjobfiles[skippedjobindex]
            #                        del skippedjobpos[skippedjobindex]
            #                    reloaddocsline = reloaddocstream.readline()
            #        if len(skippedjobpos) == 0:
            #            break
            #        #if len(reloadfilesplits) > 0:
            #        #    reloadids += [reloadfilesplits]
            #        #    reloadfiles += [reloadfile.split("/")[-1]]
            #    skippedjobfiles += skippedjobfilescollect
            #    skippedjobpos += skippedjobposcollect
            #os.rename(tempstream.name, skippedstream.name)
        if len(skippeddocs) > 0:
            try:
                with open(controllerpath + "/" + reloadstatefilename + "FILE", "r") as reloadstatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                    for reloadstateline in reloadstatefilestream:
                        reloadstatelinefile = reloadstateline.rstrip("\n")
                        if reloadstatelinefile in skippedreloadfiles:
                            with open(controllerpath + "/" + reloadpath + "/" + reloadstatelinefile, "r") as reloaddocsstream, open(controllerpath + "/" + reloadstatefilename + "DOC", "a") as reloadstatedocsstream:
                                for reloaddocsline in reloaddocsstream:
                                    if "@" in reloaddocsline:
                                        reloaddoc = json.loads("{" + reloaddocsline.rstrip("\n").split(".{")[1])
                                        reloaddocform = "_".join(indexdoc2indexsplit(reloaddoc, dbindexes))
                                        if reloaddocform not in skippeddocs:
                                            reloadstatedocsstream.write(reloadstatelinefile + "~" + reloaddocform + "\n")
                                            reloadstatedocsstream.flush()
                                        else:
                                            skippeddocindex = skippeddocs.index(reloaddocform)
                                            skippedjobfile = skippedjobfiles[skippeddocindex]
                                            skippedjobnum = skippedjobnums[skippeddocindex]
                                            del skippeddocs[skippeddocindex]
                                            del skippedjobfiles[skippeddocindex]
                                            del skippedjobnums[skippeddocindex]
                                            del skippedreloadfiles[skippeddocindex]
                                            if skippedjobfiles.count(skippedjobfile) == 0:
                                                if not os.path.isdir(controllerpath + "/jobs/reloaded"):
                                                    os.mkdir(controllerpath + "/jobs/reloaded")
                                                os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
                                                try:
                                                    skippedjobfilein = skippedjobfile.replace(".docs", ".in")
                                                    os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                                except:
                                                    pass
                                                try:
                                                    skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
                                                    os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                                except:
                                                    pass
                                                try:
                                                    skippedjobfileout = skippedjobfile.replace(".docs", ".out")
                                                    os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                                except:
                                                    pass
                                                if skippedjobnums.count(skippedjobnum) == 0:
                                                    for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
                                                        os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                                    for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                                        if not ((".merge." in file) and (".out" in file)):
                                                            os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                                with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
                                                    skippedheader = skippedstream.readline()
                                                    tempstream2.write(skippedheader)
                                                    tempstream2.flush()
                                                    for skippedline in skippedstream:
                                                        if not skippedjobfile in skippedline:
                                                            tempstream2.write(skippedline)
                                                            tempstream2.flush()
                                                    #print("a")
                                                    #sys.stdout.flush()
                                                    os.rename(tempstream2.name, skippedstream.name)
                                                    #print("b")
                                                    #sys.stdout.flush()
                        else:
                            tempstream1.write(reloadstateline)
                            tempstream1.flush()
                    os.rename(tempstream1.name, reloadstatefilestream.name)
            except IOError:
                print "File path \"" + controllerpath + "/" + reloadstatefilename + "FILE" + "\" does not exist."
                sys.stdout.flush()
            try:
                with open(controllerpath + "/" + reloadstatefilename + "DOC", "r") as reloadstatefilestream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream1:
                    for reloadstateline in reloadstatefilestream:
                        reloadstatelinefile, reloadstatelinedoc = reloadstateline.rstrip("\n").split("~")
                        if reloadstatelinedoc in skippeddocs:
                            skippedindex = skippeddocs.index(reloadstatelinedoc)
                            if reloadstatelinefile == skippedreloadfiles[skippedindex]:
                                skippedjobfile = skippedjobfiles[skippedindex]
                                skippedjobnum = skippedjobnums[skippedindex]
                                del skippeddocs[skippeddocindex]
                                del skippedjobfiles[skippeddocindex]
                                del skippedjobnums[skippeddocindex]
                                del skippedreloadfiles[skippeddocindex]
                                if skippedjobfiles.count(skippedjobfile) == 0:
                                    if not os.path.isdir(controllerpath + "/jobs/reloaded"):
                                        os.mkdir(controllerpath + "/jobs/reloaded")
                                    os.rename(controllerpath + "/jobs/" + skippedjobfile, controllerpath + "/jobs/reloaded/" + skippedjobfile)
                                    try:
                                        skippedjobfilein = skippedjobfile.replace(".docs", ".in")
                                        os.rename(controllerpath + "/jobs/" + skippedjobfilein, controllerpath + "/jobs/reloaded/" + skippedjobfilein)
                                    except:
                                        pass
                                    try:
                                        skippedjobfiletemp = skippedjobfile.replace(".docs", ".temp")
                                        os.rename(controllerpath + "/jobs/" + skippedjobfiletemp, controllerpath + "/jobs/reloaded/" + skippedjobfiletemp)
                                    except:
                                        pass
                                    try:
                                        skippedjobfileout = skippedjobfile.replace(".docs", ".out")
                                        os.rename(controllerpath + "/jobs/" + skippedjobfileout, controllerpath + "/jobs/reloaded/" + skippedjobfileout)
                                    except:
                                        pass
                                    if skippedjobnums.count(skippedjobnum) == 0:
                                        for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + "_*"):
                                            os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                        for file in glob.iglob(controllerpath + "/jobs/*_job_" + skippedjobnum + ".*"):
                                            if not ((".merge." in file) and (".out" in file)):
                                                os.rename(file, file.replace("/jobs/", "/jobs/reloaded/"))
                                    with open(controllerpath + "/skipped", "r") as skippedstream, tempfile.NamedTemporaryFile(dir = controllerpath, delete = False) as tempstream2:
                                        skippedheader = skippedstream.readline()
                                        tempstream2.write(skippedheader)
                                        tempstream2.flush()
                                        for skippedline in skippedstream:
                                            if not skippedjobfile in skippedline:
                                                tempstream2.write(skippedline)
                                                tempstream2.flush()
                                        #print("c")
                                        #sys.stdout.flush()
                                        os.rename(tempstream2.name, skippedstream.name)
                                        #print("d")
                                        #sys.stdout.flush()
                        else:
                            tempstream1.write(reloadstateline)
                            tempstream1.flush()
                    os.rename(tempstream1.name, reloadstatefilestream.name)
            except IOError:
                print "File path \"" + controllerpath + "/" + reloadstatefilename + "DOC" + "\" does not exist."
                sys.stdout.flush()
            counters[1] -= skippeddoccount
            docounterupdate(counters, counterstatefile, counterheader)
    except IOError:
        print "File path \"" + controllerpath + "/skipped\" does not exist."
        sys.stdout.flush()

#def orderpartitions(partitions):
#    partitionsidle = get_partitionsidle(partitions)
#    #partsmix = Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'mix' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
#    #partsalloc = Popen("sinfo -o '%t %P' | grep 'ser-par-10g' | grep 'alloc' | cut -d' ' -f2 | sed 's/\*//g' | tr '\n' ',' | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(',')
#    #partslist = ', '.join(partsidle + [x for x in partsmix if x not in partsidle] + [x for x in partsalloc if x not in partsidle + partsmix])
#    partitionscomp = get_partitionscomp(partitions)
#    partitionsrun = get_partitionsrun(partitions)
#    partitionspend = get_partitionspend(partitions)
#    #print "orderpartitions"
#    #print "for rawpart in $(sinfo -h -o '%t %c %D %P' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'idle' | cut -d' ' -f2, 3,4 | sed 's/\*//g' | sed 's/\s/, /g'); do echo $(echo $rawpart | cut -d', ' -f1, 2 | sed 's/, /*/g' | bc), $(echo $rawpart | cut -d', ' -f3); done | tr ' ' '\n' | sort -t', ' -k1 -n | cut -d', ' -f2 | tr '\n' ', ' | head -c -1"
#    #print "squeue -h -o '%P %T %L' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'COMPLETING' | sort -k2, 2 -k3, 3n | cut -d' ' -f1 | tr '\n' ', ' | head -c -1"
#    #print "squeue -h -o '%P %T %L' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'RUNNING' | sort -k2, 2 -k3, 3n | cut -d' ' -f1 | tr '\n' ', ' | head -c -1"
#    #print "squeue -h -o '%P %T %L' | grep -E '(" + greppartitions + ")\*?\s*$' | grep 'PENDING' | sort -k2, 2 -k3, 3n | cut -d' ' -f1 | tr '\n' ', ' | head -c -1"
#    #print ""
#    #sys.stdout.flush()
#    orderedpartitions = [x for x in deldup(partitionsidle + partitionscomp + partitionsrun + partitionspend + partitions) if x != ""]
#    return orderedpartitions

#def orderfreepartitions(partitions):
#    partitionsidle = get_partitionsidle(partitions)
#    partitionscomp = get_partitionscomp(partitions)
#    orderedfreepartitions = [x for x in deldup(partitionsidle + partitionscomp) if x != ""]
#    return orderedfreepartitions

#def getmaxmemorypernode(resourcesstatefile, partition):
#    maxmemorypernode = 0
#    try:
#        with open(resourcesstatefile, "r") as resourcesstream:
#            resourcesheader = resourcesstream.readline()
#            for resourcesstring in resourcesstream:
#                resources = resourcesstring.rstrip("\n").split(",")
#                if resources[0] == partition:
#                    maxmemorypernode = eval(resources[1])
#                    break
#    except IOError:
#        print "File path \"" + resourcesstatefile + "\" does not exist."
#        sys.stdout.flush()
#    return maxmemorypernode

#def get_freenodes(partitions):
    #a = get_idlenodeCPUs(partitions)
    #b = get_mixnodeCPUs(partitions)
    #c = get_compnodeCPUs(partitions)
    #print("IDLE: "+str(a))
    #print("MIX: "+str(b))
    #print("COMP: "+str(c))
    #print("")
    #return sorted(a + b + c, key = lambda x: x[3], reverse = True)
#    return sorted(get_idlenodeCPUs(partitions) + get_mixnodeCPUs(partitions) + get_compnodeCPUs(partitions), key = lambda x: x[3], reverse = True)

def crunchconfig(rootpath):
    with open(rootpath + "/crunch.config", "r") as crunchconfigstream:
        crunchconfigdoc = yaml.load(crunchconfigstream)
    return crunchconfigdoc

def controllerconfig(controllername, controllerpath):
    globalmaxjobcount = get_maxjobcount()
    localmaxstepcount = get_maxstepcount()

    with open(controllerpath + "/" + modname + "_" + controllername + ".config", "r") as controllerconfigstream:
        controllerconfigdoc = yaml.load(controllerconfigstream)

    if isinstance(controllerconfigdoc["controller"]["storagelimit"], str):
        if controllerconfigdoc["controller"]["storagelimit"] == "":
            controllerconfigdoc["controller"]["storagelimit"] = None
        else:
            controllerstoragelimit_num = ""
            controllerstoragelimit_unit = ""
            for x in controllerconfigdoc["controller"]["storagelimit"]:
                if x.isdigit() or x == ".":
                    controllerstoragelimit_num += x
                else:
                    controllerstoragelimit_unit += x
            if controllerstoragelimit_unit.lower() == "" or controllerstoragelimit_unit.lower() == "b":
                controllerconfigdoc["controller"]["storagelimit"] = float(controllerstoragelimit_num)
            elif controllerstoragelimit_unit.lower() in ["k", "kb"]:
                controllerconfigdoc["controller"]["storagelimit"] = float(controllerstoragelimit_num) * 1024
            elif controllerstoragelimit_unit.lower() in ["m", "mb"]:
                controllerconfigdoc["controller"]["storagelimit"] = float(controllerstoragelimit_num) * 1000 * 1024
            elif controllerstoragelimit_unit.lower() in ["g", "gb"]:
                controllerconfigdoc["controller"]["storagelimit"] = float(controllerstoragelimit_num) * 1000 * 1000 * 1024

    if isinstance(controllerconfigdoc["module"]["memorylimit"], str):
        if controllerconfigdoc["module"]["memorylimit"] == "":
            controllerconfigdoc["module"]["memorylimit"] = None
        else:
            modulememorylimit_num = ""
            modulememorylimit_unit = ""
            for x in controllerconfigdoc["module"]["memorylimit"]:
                if x.isdigit() or x == ".":
                    modulememorylimit_num += x
                else:
                    modulememorylimit_unit += x
        if modulememorylimit_unit.lower() == "" or modulememorylimit_unit.lower() == "b":
            controllerconfigdoc["module"]["memorylimit"] = float(modulememorylimit_num)
        elif modulememorylimit_unit.lower() in ["k", "kb"]:
            controllerconfigdoc["module"]["memorylimit"] = float(modulememorylimit_num) * 1000
        elif modulememorylimit_unit.lower() in ["m", "mb"]:
            controllerconfigdoc["module"]["memorylimit"] = float(modulememorylimit_num) * 1000 * 1000
        elif modulememorylimit_unit.lower() in ["g", "gb"]:
            controllerconfigdoc["module"]["memorylimit"] = float(modulememorylimit_num) * 1000 * 1000 * 1000

    assert isinstance(crunchconfigdoc["max-jobs"], int) or crunchconfigdoc["max-jobs"] is None
    if isinstance(crunchconfigdoc["max-jobs"], int):
        globalmaxjobcount = min(crunchconfigdoc["max-jobs"], globalmaxjobcount)
    assert isinstance(crunchconfigdoc["max-steps"], int) or crunchconfigdoc["max-steps"] is None
    if isinstance(crunchconfigdoc["max-steps"], int):
        localmaxstepcount = min(crunchconfigdoc["max-steps"], localmaxstepcount)

    if controllerconfigdoc["job"]["joblimit"] == "":
        localmaxjobcount = globalmaxjobcount
    else:
        localmaxjobcount = min(int(controllerconfigdoc["job"]["joblimit"]), globalmaxjobcount)

    if controllerconfigdoc["module"]["buffertime"] == "":
        controllerconfigdoc["module"]["buffertime"] = "00:00:00"

    assert "resources" in crunchconfigdoc.keys()
    partitionsmaxmemory = crunchconfigdoc["resources"]

    controllerconfigdoc["db"]["query"] = eval(controllerconfigdoc["db"]["query"])

    return controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory

#def distributeovernodes(partition, maxmemorypernode, controllerconfigdoc["module"]["memorylimit"], nnodes, localmaxstepcount, niters):#, maxthreads):
def distributeovernodes(freenodes, partitionsmaxmemory, modulememorylimit, nnodes, localmaxstepcount, niters):
    #print partitions
    #sys.stdout.flush()
    #partition = partitions[0]
    #partitionncorespernode = get_partitionncorespernode(partition)
    #ncores = nnodes * partitionncorespernode
    #maxnnodes = get_maxnnodes(partition)
    #print "distributeovernodes"
    #print "sinfo -h -p '" + partition + "' -o '%c' | head -n1 | head -c -1"
    #print "scontrol show partition '" + partition + "' | grep 'MaxNodes = ' | sed 's/^.*\sMaxNodes = \([0-9]*\)\s.*$/\\1/g' | head -c -1"
    #print ""
    #sys.stdout.flush()
    #maxmemorypernode = getmaxmemorypernode(resourcesstatefile, partition)
    #tempnnodes = nnodes
    #if controllerconfigdoc["module"]["memorylimit"] == "":
    #    nstepsdistribmempernode = float(maxsteps / tempnnodes)
    #    while (tempnnodes > 1) and (nstepsdistribmempernode < 1):
    #        tempnnodes -= 1
    #        nstepsdistribmempernode = float(maxsteps / tempnnodes)
    #else:
    freenodesmem = [x + [partitionsmaxmemory[x[0]] * x[3] / x[2]] for x in freenodes if modulememorylimit == None or modulememorylimit <= partitionsmaxmemory[x[0]] * x[3] / x[2]]

    if (modulememorylimit == None):# or (eval(controllerconfigdoc["module"]["memorylimit"]) == maxmemorypernode):
        nsteps = min(nnodes, localmaxstepcount) * niters
        #memoryperstep = maxmemorypernode
    #elif eval(controllerconfigdoc["module"]["memorylimit"]) > maxmemorypernode:
    #    return None
    else:
        nstepsdistribmem = sum([x[4] / modulememorylimit for x in freenodesmem[:nnodes]])
        #print "a: " + str(nstepsdistribmem)
        nsteps = min(localmaxstepcount, nstepsdistribmem) * niters
        #print "b: " + str(nsteps)
        #memoryperstep = nnodes * maxmemorypernode / nsteps
        #print "c: " + str(memoryperstep)

    #nstepsfloat = min(float(ndocsleft), float(partitionncorespernode), nstepsdistribmempernode)
    #nnodes = int(min(maxnnodes, math.ceil(1./nstepsfloat)))
    #ncores = nnodes * partitionncorespernode
    #nsteps = int(max(1, nstepsfloat))
    #nnodes = 1
    #if nstepsdistribmempernode < 1:
    #    if len(partitions) > 1:
    #        return distributeovernodes(resourcesstatefile, controllerconfigdoc["job"]["partitions"][1:], controllerconfigdoc["module"]["memorylimit"], nnodes, maxsteps, maxthreads)
    #    else:
    #        print "Memory requirement is too large for this cluster."
    #        sys.stdout.flush()
    #nnodes = tempnnodes
    #nsteps = min(ncores, nstepsdistribmem, maxsteps)
    #nsteps = int(nstepsfloat)
    #if nsteps > 0:
    #    memoryperstep = nnodes * maxmemorypernode / nsteps
    #else:
    #    memoryperstep = maxmemorypernode
    return freenodesmem[:nnodes], nsteps

def writejobfile(controllerconfigdoc, reloadjob, jobname, jobstepnames, controllerpath, partitiontimelimits, freenodesmem, counters, scriptcommand, scriptflags, scriptext, dbindexes, nbatch, nworkers, docbatches):
    ndocbatches = len(docbatches)
    outputlinemarkers = ["-", " + ", "&", "@", "CPUTime:", "MaxRSS:", "MaxVMSize:", "BSONSize:", "None"]
    jobstring = "#!/bin/bash\n"
    jobstring += "\n"
    jobstring += "# Created " + str(datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")) + "\n"
    jobstring += "\n"
    jobstring += "#Job name\n"
    jobstring += "#SBATCH -J \"" + jobname + "\"\n"
    jobstring += "#################\n"
    jobstring += "#Working directory\n"
    jobstring += "#SBATCH -D \"" + controllerpath + "/logs\"\n"
    jobstring += "#################\n"
    jobstring += "#Job output file\n"
    jobstring += "#SBATCH -o \"" + jobname + ".debug\"\n"
    jobstring += "#################\n"
    jobstring += "#Job error file\n"
    jobstring += "#SBATCH -e \"" + jobname + ".err\"\n"
    jobstring += "#################\n"
    jobstring += "#Job file write mode\n"
    jobstring += "#SBATCH --open-mode=\"" + controllerconfigdoc["job"]["writemode"] + "\"\n"
    jobstring += "#################\n"
    jobstring += "#Job max time\n"
    jobstring += "#SBATCH --time=\"" + max([partitiontimelimits[i][0] for i in range(len(partitiontimelimits))]) + "\"\n"
    jobstring += "#################\n"
    jobstring += "#Partition(s) to use for job\n"
    jobstring += "#SBATCH --partition=\"" + ','.join([x[0] for x in freenodesmem]) + "\"\n"
    jobstring += "#################\n"
    #jobstring += "#Number of tasks (CPUs) allocated for job\n"
    #jobstring += "#SBATCH -n " + str(ndocbatches) + "\n"
    #jobstring += "#################\n"
    #jobstring += "#Number of nodes to distribute n tasks across\n"
    #jobstring += "#SBATCH -N " + str(len(freenodesmem)) + "\n"
    #jobstring += "#################\n"
    jobstring += "#List of nodes to distribute n tasks across\n"
    jobstring += "#SBATCH -w \"" + ','.join([x[1] for x in freenodesmem]) + "\"\n"
    jobstring += "#################\n"
    #jobstring += "#Lock down N nodes for job\n"
    #jobstring += "#SBATCH --exclusive\n"
    #jobstring += "#################\n"
    jobstring += "#Requeue job on node failure\n"
    jobstring += "#SBATCH --requeue\n"
    jobstring += "#################\n"
    jobstring += "\n"
    jobstring += "# Job info\n"
    jobstring += "modname=\"" + controllerconfigdoc["controller"]["modname"] + "\"\n"
    jobstring += "controllername=\"" + controllerconfigdoc["controller"]["controllername"] + "\"\n"
    #jobstring += "outputlinemarkers=\"" + str(outputlinemarkers).replace(" ", "") + "\"\n"
    #jobstring += "jobnum=" + str(counters[0]) + "\n"
    #jobstring += "nsteps=" + str(ndocbatches) + "\n"
    jobstring += "memunit=\"M\"\n"
    #jobstring += "totmem=" + str(totmem / 1000000) + "\n"
    #jobstring += "stepmem=$((${totmem}/${nsteps}))\n"
    #jobstring += "steptime=\"" + buffertimelimit + "\"\n"
    if controllerconfigdoc["module"]["language"] == None:
        jobstring += "modlang=\"\"\n"
    else:
        jobstring += "modlang=\"" + controllerconfigdoc["module"]["language"] + "\"\n"
    jobstring += "nbatch=" + str(nbatch) + "\n"
    jobstring += "nworkers=" + str(min(nworkers, ndocbatches)) + "\n"
    jobstring += "\n"
    jobstring += "# Option info\n"
    #jobstring += "controllerconfigdoc["options"]["intermedlog"]=\"" + str(controllerconfigdoc["options"]["intermedlog"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["outlog"]=\"" + str(controllerconfigdoc["options"]["outlog"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["cleanup"]=\"" + str(controllerconfigdoc["options"]["cleanup"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["intermedlocal"]=\"" + str(controllerconfigdoc["options"]["intermedlocal"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["outlocal"]=\"" + str(controllerconfigdoc["options"]["outlocal"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["outdb"]=\"" + str(controllerconfigdoc["options"]["outdb"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["statslocal"]=\"" + str(controllerconfigdoc["options"]["statslocal"]) + "\"\n"
    #jobstring += "controllerconfigdoc["options"]["statsdb"]=\"" + str(controllerconfigdoc["options"]["statsdb"]) + "\"\n"
    jobstring += "markdone=\"" + controllerconfigdoc["options"]["markdone"] + "\"\n"
    jobstring += "cleanup=\"" + str(controllerconfigdoc["options"]["cleanup"]) + "\"\n"
    jobstring += "\n"
    jobstring += "# File system info\n"
    jobstring += "rootpath=\"${CRUNCH_ROOT}\"\n"
    jobstring += "binpath=\"${rootpath}/bin\"\n"
    jobstring += "modpath=\"${rootpath}/modules/modules/${modname}\"\n"
    jobstring += "\n"
    #jobstring += "#Script info\n"
    #jobstring += "controllerconfigdoc["module"]["language"]=\"" + controllerconfigdoc["module"]["language"] + "\"\n"
    #jobstring += "scriptcommand=\"" + scriptcommand + "\"\n"
    #jobstring += "scriptflags=\"" + scriptflags + "\"\n"
    #jobstring += "scriptext=\"" + scriptext + "\"\n"
    #jobstring += "\n"
    #if not reloadjob:
    #    jobstring += "#Database info\n"
    #    jobstring += "controllerconfigdoc["db"]["basecollection"]=\"" + base + "\"\n"
    #    jobstring += "dbindexes=\"" + str([str(x) for x in dbindexes]).replace(" ", "") + "\"\n"
    #    jobstring += "\n"
    jobstring += "# MPI info\n"
    jobinfo = []
    j = 0
    k = 0
    for i in range(ndocbatches):
        with open(controllerpath + "/docs/" + jobstepnames[i] + ".docs", "w") as docstream:
            for n in range(len(docbatches[i])):
                #if n > 0:
                #    docstream.write("\n")
                if reloadjob:
                    docstream.write(docbatches[i][n])
                else:
                    docstream.write(json.dumps(docbatches[i][n], separators = (',',':')) + "\n")
                docstream.flush()
        if controllerconfigdoc["db"]["nthreadsfield"] != None:
            nstepthreads = max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatches[i]])
        else:
            nstepthreads = 1
        jobstring += "nstepthreads=" + str(nstepthreads) + "\n"
        stepmem = nstepthreads * freenodesmem[j][4] / (1000000 * ndocbatches)#freenodesmem[j][3])
        jobstring += "stepmem=" + str(stepmem) + "\n"
        if not reloadjob:
            jobstring += "mpirun -srun -w \"" + freenodesmem[j][1] + "\" -n \"${nstepthreads}\" -J \"" + jobstepnames[i] + "\" --mem-per-cpu=\"${stepmem}${memunit}\" "
            if partitiontimelimits[j][1] != "infinite":
                jobstring += "--time=\"" + partitiontimelimits[j][1] + "\" "
            jobstring += "python ${binpath}/wrapper.py --mod \"${modname}\" --controller \"${controllername}\" --stepid \"${SLURM_JOBID}." + str(i) + "\" --delay \"0.1\" --stats \"TotalCPUTime\" \"Rss\" \"Size\" "
            if partitiontimelimits[j][1] != "infinite":
                jobstring += "--time-limit \"" + partitiontimelimits[j][1] + "\" "
            jobstring += "--cleanup-after \"${cleanup}\" --nbatch \"${nbatch}\" --nworkers \"${nworkers}\" --dbindexes " + " ".join(["\"" + x + "\"" for x in dbindexes]) + " --file \"" + jobstepnames[i] + ".docs\" "#--random-nbatch "
            if controllerconfigdoc["options"]["intermedlog"]:
                jobstring += "--intermed-log "
            if controllerconfigdoc["options"]["intermedlocal"]:
                jobstring += "--intermed-local "
            if controllerconfigdoc["options"]["outlog"]:
                jobstring += "--out-log "
            if controllerconfigdoc["options"]["outlocal"]:
                jobstring += "--out-local "
            if controllerconfigdoc["options"]["statslocal"]:
                jobstring += "--stats-local "
            if controllerconfigdoc["options"]["outdb"]:
                jobstring += "--out-db "
            if controllerconfigdoc["options"]["statsdb"]:
                jobstring += "--stats-db "
            jobstring += "--module-language \"${modlang}\" --module " + scriptcommand + scriptflags + "${modpath}/${modname}" + scriptext + " "
            if controllerconfigdoc["module"]["args"] != None:
                jobstring += "--args " + controllerconfigdoc["module"]["args"] + " "
            jobstring += "&"
        jobstring += "\n"
        jobinfo += [[jobstepnames[i], freenodesmem[j][0], nstepthreads, stepmem]]
        k += nstepthreads
        if k == freenodesmem[j][3]:
            j += 1
            k = 0
    jobstring += "wait"
    #if reloadjob:
    #    jobstring += "python \"${binpath}/reloadjobmanager.py\" \"${controllerconfigdoc["controller"]["modname"]}\" \"${controllerconfigdoc["controller"]["controllername"]}\" \"${controllerconfigdoc["module"]["language"]}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nworkers}\" \"${controllerconfigdoc["options"]["intermedlog"]}\" \"${controllerconfigdoc["options"]["outlog"]}\" \"${controllerconfigdoc["options"]["cleanup"]}\" \"${controllerconfigdoc["options"]["intermedlocal"]}\" \"${controllerconfigdoc["options"]["outlocal"]}\" \"${controllerconfigdoc["options"]["outdb"]}\" \"${controllerconfigdoc["options"]["statslocal"]}\" \"${controllerconfigdoc["options"]["statsdb"]}\" \"${controllerconfigdoc["options"]["markdone"]}\" \"${rootpath}\" \"${nstepthreads[@]}\""
    #else:
    #    jobstring += "python \"${binpath}/queryjobmanager.py\" \"${controllerconfigdoc["controller"]["modname"]}\" \"${controllerconfigdoc["controller"]["controllername"]}\" \"${controllerconfigdoc["module"]["language"]}\" \"${scriptcommand}\" \"${scriptflags}\" \"${scriptext}\" \"${outputlinemarkers}\" \"${SLURM_JOBID}\" \"${jobnum}\" \"${memunit}\" \"${totmem}\" \"${steptime}\" \"${nbatch}\" \"${nworkers}\" \"${controllerconfigdoc["options"]["intermedlog"]}\" \"${controllerconfigdoc["options"]["outlog"]}\" \"${controllerconfigdoc["options"]["cleanup"]}\" \"${controllerconfigdoc["options"]["intermedlocal"]}\" \"${controllerconfigdoc["options"]["outlocal"]}\" \"${controllerconfigdoc["options"]["outdb"]}\"\"${controllerconfigdoc["options"]["statslocal"]}\" \"${controllerconfigdoc["options"]["statsdb"]}\" \"${controllerconfigdoc["options"]["markdone"]}\" \"${rootpath}\" \"${controllerconfigdoc["db"]["basecollection"]}\" \"${dbindexes}\" \"${nstepthreads[@]}\""
    with open(controllerpath + "/jobs/" + jobname + ".job", "w") as jobstream:
        jobstream.write(jobstring)
        jobstream.flush()

    return jobinfo

def waitforslots(controllerconfigdoc, reloadjob, needslicense, username, controllerpath, querystatefilename, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, dbindexes):
    #print("dir_size/controllerconfigdoc["controller"]["storagelimit"]: " + str(dir_size(controllerpath)) + "/" + str(controllerconfigdoc["controller"]["storagelimit"]))
    #print("storageleft: " + str(storageleft(controllerpath, controllerconfigdoc["controller"]["storagelimit"])))
    #sys.stdout.flush()
    #needslicense = (licensestream != None)
    with open(statusstatefile, "r+") as statusstream:
        startstatus = statusstream.readline()
        statusstream.truncate(0)
        statusstream.write("Waiting for slots.")
        statusstream.flush()
    if needslicense:
        jobslotsleft = clusterjobslotsleft(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], globalmaxjobcount, localmaxjobcount)
        nlicensesplit = licensecount(username, needslicense, localbinpath, licensescript, sublicensescript)
        licensesleft = clusterlicensesleft(nlicensesplit, maxthreads)
        freenodes = get_freenodes(partitions)
        releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
        if (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and licensesleft and (len(freenodes) > 0) and storageleft(controllerpath, controllerconfigdoc["controller"]["storagelimit"])):
            while (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and licensesleft and (len(freenodes) > 0) and storageleft(controllerpath, controllerconfigdoc["controller"]["storagelimit"])):
                time.sleep(controllerconfigdoc["controller"]["sleeptime"])
                jobslotsleft = clusterjobslotsleft(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], globalmaxjobcount, localmaxjobcount)
                nlicensesplit = licensecount(username, needslicense, localbinpath, licensescript, sublicensescript)
                licensesleft = clusterlicensesleft(nlicensesplit, maxthreads)
                freenodes = get_freenodes(partitions)
                releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
        #fcntl.flock(licensestream, fcntl.LOCK_EX)
        #fcntl.LOCK_EX might only work on files opened for writing. This one is open as "a + ", so instead use bitwise OR with non-controllerconfigdoc["options"]["blocking"] and loop until lock is acquired.
        while (timeleft(starttime, controllerbuffertimelimit) > 0):
            releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
            #try:
            #    fcntl.flock(licensestream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            #    break
            #except IOError as e:
            #    if e.errno != errno.EAGAIN:
            #        raise
            #    else:
            #        time.sleep(0.1)
        if not (timeleft(starttime, controllerbuffertimelimit) > 0):
            #print "hi"
            #sys.stdout.flush()
            return None
        #licensestream.seek(0, 0)
        #licenseheader = licensestream.readline()
        #print licenseheader
        #sys.stdout.flush()
        #licensestream.truncate(0)
        #licensestream.seek(0, 0)
        #licensestream.write(licenseheader)
        #licensestream.write(', '.join([str(x) for x in nlicensesplit]))
        #licensestream.flush()
    else:
        jobslotsleft = clusterjobslotsleft(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], globalmaxjobcount, localmaxjobcount)
        freenodes = get_freenodes(controllerconfigdoc["job"]["partitions"])
        releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
        if (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and (len(freenodes) > 0) and storageleft(controllerpath, controllerconfigdoc["controller"]["storagelimit"])):
            while (timeleft(starttime, controllerbuffertimelimit) > 0) and not (jobslotsleft and (len(freenodes) > 0) and storageleft(controllerpath, controllerconfigdoc["controller"]["storagelimit"])):
                time.sleep(controllerconfigdoc["controller"]["sleeptime"])
                jobslotsleft = clusterjobslotsleft(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], globalmaxjobcount, localmaxjobcount)
                freenodes = get_freenodes(controllerconfigdoc["job"]["partitions"])
                releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
        if not (timeleft(starttime, controllerbuffertimelimit) > 0):
            return None

    #if reloadjob:
    #    requeueskippedreloadjobs(controllerconfigdoc, controllerpath, reloadstatefilename, reloadpath, counters, counterstatefile, counterheader, dbindexes)
    #else:
    #    requeueskippedqueryjobs(controllerconfigdoc, controllerpath, querystatefilename, counters, counterstatefile, counterheader, dbindexes)

    with open(statusstatefile, "w") as statusstream:
        statusstream.write(startstatus)
        statusstream.flush()

    return freenodes

def doinput(docbatch, controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, rootpath, querylimit, counters, reloadjob, needslicense, username, controllerpath, querystatefilename, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes):
    crunchconfigdoc = crunchconfig(rootpath)
    controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory = controllerconfig(controllerconfigdoc["controller"]["controllername"], controllerpath)
    if querylimit == None:
        niters = controllerconfigdoc["options"]["niters"]
    else:
        niters = min(controllerconfigdoc["options"]["niters"], querylimit-counters[1] + 1)
    if len(docbatch) == 1:
        niters = min(niters, len(docbatch[0]))
    if (len(docbatch) > 0) and (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
        maxthreads = max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[0:niters]])
    else:
        maxthreads = 1
    freenodes = waitforslots(controllerconfigdoc, reloadjob, needslicense, username, controllerpath, querystatefilename, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, dbindexes)
    if len(freenodes) == 0:
        return None
    #freenodes = orderpartitions(partitions)
    nnodes = 1
    #i = 0
    #while i < len(freenodes):
    #    partition = freenodes[i]
        #nnodes = 1
    freenodesmem, nsteps = distributeovernodes(freenodes, partitionsmaxmemory, controllerconfigdoc["module"]["memorylimit"], nnodes, localmaxstepcount, niters)
    if len(freenodesmem) == 0:
        raise Exception("Memory requirement is too large for this cluster.")
    ncores = sum([x[3] for x in freenodesmem])
    if len(docbatch) > 0:
        maxthreads = 0
        nextdocind = 0
        if (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
                maxthreads += max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
                nextdocind += niters
            if nextdocind > len(docbatch):
                nextdocind = len(docbatch)
            if maxthreads > ncores:
                nextdocind -= niters
                maxthreads -= max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
        else:
            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
                maxthreads += 1
                nextdocind += niters
            if nextdocind > len(docbatch):
                nextdocind = len(docbatch)
            if maxthreads > ncores:
                nextdocind -= niters
                maxthreads -= 1
        while maxthreads == 0:
            nnodes += 1
            freenodes = waitforslots(controllerconfigdoc, reloadjob, needslicense, username, controllerpath, querystatefilename, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, dbindexes)
            if len(freenodes) == 0:
                return None
            #freenodes = orderpartitions(partitions)
            #i = 0
            #while i < len(freenodes):
            #    partition = freenodes[i]
                #nnodes = 1
            freenodesmem, nsteps = distributeovernodes(freenodes, partitionsmaxmemory, controllerconfigdoc["module"]["memorylimit"], nnodes, localmaxstepcount, niters)
            if len(freenodesmem) == 0:
                raise Exception("Memory requirement is too large for this cluster.")
            ncores = sum([x[3] for x in freenodesmem])
            maxthreads = 0
            nextdocind = 0
            if (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
                while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
                    maxthreads += max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
                    nextdocind += niters
                if nextdocind > len(docbatch):
                    nextdocind = len(docbatch)
                if maxthreads > ncores:
                    nextdocind -= niters
                    maxthreads -= max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
            else:
                while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
                    maxthreads += 1
                    nextdocind += niters
                if nextdocind > len(docbatch):
                    nextdocind = len(docbatch)
                if maxthreads > ncores:
                    nextdocind -= niters
                    maxthreads -= 1
        #nsteps = 0
    
    with open(statusstatefile, "w") as statusstream:
        statusstream.write("Populating job.")
        statusstream.flush()

    #print {"partition": partition, "nnodes": nnodes, "ncores": ncores, "nsteps": nsteps, "maxmemorypernode": maxmemorypernode}
    #sys.stdout.flush()
    #return {"partition": partition, "nnodes": nnodes, "ncores": ncores, "nsteps": nsteps, "maxmemorypernode": maxmemorypernode}
    return {"freenodesmem": freenodesmem, "nsteps": nsteps}

def doaction(counters, inputdoc, docbatch, controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, querylimit, reloadjob, needslicense, username, controllerpath, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes, scriptcommand, scriptflags, scriptext, querystatefilename, counterstatefile, counterheader):
    #partition = inputdoc['partition']
    freenodesmem = inputdoc['freenodesmem']
    nnodes = len(freenodesmem)
    ncores = sum([x[3] for x in freenodesmem])
    nsteps = inputdoc['nsteps']
    #totmem = sum([x[4] for x in freenodesmem])
    #maxmemorypernode = inputdoc["maxmemorypernode"]
    #print(docbatch)
    #sys.stdout.flush()
    if querylimit == None:
        niters = controllerconfigdoc["options"]["niters"]
    else:
        niters = min(controllerconfigdoc["options"]["niters"], querylimit - counters[1] + 1)
    if len(docbatch) < nsteps:
        niters = min(niters, len(docbatch))
    if controllerconfigdoc["options"]["nbatch"] > niters:
        nbatch = niters
    else:
        nbatch = controllerconfigdoc["options"]["nbatch"]
    ndocs = len(docbatch)
    if controllerconfigdoc["options"]["nworkers"] * nbatch > ndocs:
        nworkers = int(ndocs / nbatch) + int(ndocs % nbatch > 0)
    else:
        nworkers = controllerconfigdoc["options"]["nworkers"]
    maxthreads = 0
    nextdocind = 0
    if (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
        while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
            maxthreads += max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
            nextdocind += niters
        if nextdocind > len(docbatch):
            nextdocind = len(docbatch)
        if maxthreads > ncores:
            nextdocind -= niters
            maxthreads -= max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
    else:
        while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
            maxthreads += 1
            nextdocind += niters
        if nextdocind > len(docbatch):
            nextdocind = len(docbatch)
        if maxthreads > ncores:
            nextdocind -= niters
            maxthreads -= 1
    while maxthreads == 0:
        nnodes += 1
        freenodes = waitforslots(controllerconfigdoc, reloadjob, needslicense, username, controllerpath, querystatefilename, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, dbindexes)
        if len(freenodes) == 0:
            return None
        #freenodes = orderpartitions(partitions)
        #i = 0
        #while i < len(freenodes):
        #    partition = freenodes[i]
            #nnodes = 1
        freenodesmem, nsteps = distributeovernodes(freenodes, partitionsmaxmemory, controllerconfigdoc["module"]["memorylimit"], nnodes, localmaxstepcount, niters)
        if len(freenodesmem) == 0:
            raise Exception("Memory requirement is too large for this cluster.")
        ncores = sum([x[3] for x in freenodesmem])
        maxthreads = 0
        nextdocind = 0
        if (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
                maxthreads += max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
                nextdocind += niters
            if nextdocind > len(docbatch):
                nextdocind = len(docbatch)
            if maxthreads > ncores:
                nextdocind -= niters
                maxthreads -= max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
        else:
            while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
                maxthreads += 1
                nextdocind += niters
            if nextdocind > len(docbatch):
                nextdocind = len(docbatch)
            if maxthreads > ncores:
                nextdocind -= niters
                maxthreads -= 1

    freenodes = waitforslots(controllerconfigdoc, reloadjob, needslicense, username, controllerpath, querystatefilename, globalmaxjobcount, localmaxjobcount, localbinpath, licensescript, sublicensescript, maxthreads, starttime, controllerbuffertimelimit, statusstatefile, dbindexes)
    if len(freenodes) == 0:
        return None
    #freenodes = orderpartitions(partitions)
    #i = 0
    #while i < len(freenodes):
    #    partition = freenodes[i]
        #nnodes = 1
    freenodesmem, nsteps = distributeovernodes(freenodes, partitionsmaxmemory, controllerconfigdoc["module"]["memorylimit"], nnodes, localmaxstepcount, niters)
    if len(freenodesmem) == 0:
        raise Exception("Memory requirement is too large for this cluster.")
    ncores = sum([x[3] for x in freenodesmem])
    maxthreads = 0
    nextdocind = 0
    if (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
        while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
            maxthreads += max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
            nextdocind += niters
        if nextdocind > len(docbatch):
            nextdocind = len(docbatch)
        if maxthreads > ncores:
            nextdocind -= niters
            maxthreads -= max([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch[nextdocind:nextdocind + niters]])
    else:
        while (nextdocind < len(docbatch)) and (maxthreads <= ncores):
            maxthreads += 1
            nextdocind += niters
        if nextdocind > len(docbatch):
            nextdocind = len(docbatch)
        if maxthreads > ncores:
            nextdocind -= niters
            maxthreads -= 1
    #docbatches = docbatch[:nextdocind]
    docbatches = [docbatch[i:i + niters] for i in range(0, nextdocind, niters)]
    #docbatchpass = docbatch[nextdocind:]

    #totmem = nnodes * maxmemorypernode
    #ncoresused = len(docbatches)
    #memoryperstep = totmem / ncoresused
    #niters = len(docbatch)
    #if controllerconfigdoc["db"]["nthreadsfield"] == None:
    #    totcontrollerconfigdoc["db"]["nthreadsfield"] = niters
    #else:
    #    totcontrollerconfigdoc["db"]["nthreadsfield"] = sum([x[controllerconfigdoc["db"]["nthreadsfield"]] for x in docbatch])
    #while not clusterjobslotsleft(globalmaxjobcount, scriptext, minnsteps = inputdoc["nsteps"]):
    #    time.sleep(controllerconfigdoc["controller"]["sleeptime"])
    #doc = json.loads(doc.rstrip('\n'))
    #if niters == 1:
    #    jobstepnames = [indexdoc2jobstepname(x, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], dbindexes) for x in docbatches]
    #else:

    with open(statusstatefile, "w") as statusstream:
        statusstream.write("Initializing job.")
        statusstream.flush()

    jobstepnames = ["crunch_" + controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_job_" + str(counters[0]) + "_step_" + str(i + 1) for i in range(len(docbatches))]
    #jobstepnamescontract = jobstepnamescontract(jobstepnames)
    jobname = "crunch_" + controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_job_" + str(counters[0]) + "_steps_" + str((counters[1] + controllerconfigdoc["options"]["niters"] - 1) / controllerconfigdoc["options"]["niters"]) + "-" + str((counters[1] + controllerconfigdoc["options"]["niters"] * len(docbatches) - 1) / controllerconfigdoc["options"]["niters"])
    #if reloadjob:
    #    jobstepnames = ["reload_" + x for x in jobstepnames]
    #    jobname = "reload_" + jobname
    partitiontimelimits = [getpartitiontimelimit(x[0], controllerconfigdoc["module"]["timelimit"], controllerconfigdoc["module"]["buffertime"]) for x in freenodesmem]
    #if len(docbatch) < inputdoc["nsteps"]:
    #    inputdoc["memoryperstep"] = (memoryperstep * inputdoc["nsteps"]) / len(docbatch)
    jobinfo = writejobfile(controllerconfigdoc, reloadjob, jobname, jobstepnames, controllerpath, partitiontimelimits, freenodesmem, counters, scriptcommand, scriptflags, scriptext, dbindexes, nbatch, nworkers, docbatches)
    #Submit job file
    if (controllerconfigdoc["db"]["nthreadsfield"] != None) and (not reloadjob):
        nthreads = [max([y[controllerconfigdoc["db"]["nthreadsfield"]] for y in x]) for x in docbatches]
    else:
        nthreads = [1 for x in docbatches]
    submitjob(controllerpath, jobname, jobinfo, nnodes, ncores, niters, nbatch, resubmit = False)
    #needslicense = (licensestream != None)
    #if needslicense:
    #    fcntl.flock(licensestream, fcntl.LOCK_UN)
        #pendlicensestream.seek(0, 0)
        #pendlicenseheader = pendlicensestream.readline()
        #npendlicensesplit = [eval(x) for x in pendlicensestream.readline().rstrip("\n").split(",")]
        #print npendlicensesplit
        #pendlicensestream.truncate(0)
        #print "hi"
        #pendlicensestream.seek(0, 0)
        #pendlicensestream.write(pendlicenseheader)
        #npendlicenses = npendlicensesplit[0]
        #if len(npendlicensesplit) > 1:
        #    npendsublicenses = npendlicensesplit[1]
        #    pendlicensestream.write(str(npendlicenses + niters) + ", " + str(npendsublicenses + totcontrollerconfigdoc["db"]["nthreadsfield"]))
        #else:
        #    pendlicensestream.write(str(npendlicenses + niters))
    #releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
    #print "End action"
    #sys.stdout.flush()
    #seekstream.write(querystream.tell())
    #seekstream.flush()
    #seekstream.seek(0)
    #doc = querystream.readline()
    releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])

    #with open(statusstatefile, "r+") as statusstream:
    #    startstatus = statusstream.readline()
    #    statusstream.truncate(0)
    #    statusstream.write("Waiting for slots.")
    #    statusstream.flush()
    return nextdocind;#docbatchpass

def docounterupdate(counters, counterstatefile, counterheader):
    with open(counterstatefile, "w") as counterstream:
        counterstream.write(counterheader)
        counterstream.write(str(counters[0]) + " " + str(counters[1]))
        counterstream.flush()

try:
    #print "main"
    #print "scontrol show config | grep 'MaxJobCount\|MaxStepCount' | sed 's/\s//g' | cut -d'=' -f2 | tr '\n' ', ' | head -c -1"
    #print ""
    #sys.stdout.flush()

    #Cluster info
    username = os.environ['USER']
    rootpath = os.environ['CRUNCH_ROOT']
    localbinpath = os.environ['USER_LOCAL']
    #username, rootpath = Popen("echo \"${USER},${CRUNCH_ROOT}\" | head -c -1", shell = True, stdout = PIPE, preexec_fn = default_sigpipe).communicate()[0].split(",")

    controllerjobid = sys.argv[1]
    controllerpath = sys.argv[2]

    modname, controllername = controllerpath.split("/")[-2:]

    crunchconfigdoc = crunchconfig(rootpath)

    if crunchconfigdoc["workload-manager"] == "slurm":
        from crunch_slurm import *

    controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory = controllerconfig(controllername, controllerpath)
    
    #Read seek position from file
    #with open(controllerpath + "/" + seekfile, "r") as seekstream:
    #    seekpos = eval(seekstream.read())

    #Open seek file stream
    #seekstream = open(controllerpath + "/" + seekfile, "w")

    #If first submission, read from database
    #if seekpos == -1:
    #Open connection to remote database
    #sys.stdout.flush()

    controllerstats = get_controllerstats(controllerjobid)
    while len(controllerstats) < 4:
        time.sleep(controllerconfigdoc["controller"]["sleeptime"])
        controllerstats = get_controllerstats(controllerjobid)
    controllerpartition, controllertimelimit, controllernnodes, controllerncores = controllerstats
    controllerpartitiontimelimit, controllerbuffertimelimit = getpartitiontimelimit(controllerpartition, controllertimelimit, controllerconfigdoc["controller"]["buffertime"])

    print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
    print "Starting job crunch_" + controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_controller"
    print ""
    print ""
    sys.stdout.flush()

    #controllerpath = get_controllerpath(controllerjobid)

    #statepath = rootpath + "/state"
    binpath = rootpath + "/bin"
    #workpath = controllerpath + "/jobs"
    if not os.path.isdir(controllerpath + "/jobs"):
        os.mkdir(controllerpath + "/jobs")
    if not os.path.isdir(controllerpath + "/docs"):
        os.mkdir(controllerpath + "/docs")
    if not os.path.isdir(controllerpath + "/logs"):
        os.mkdir(controllerpath + "/logs")
    if not os.path.isdir(controllerpath + "/locks"):
        os.mkdir(controllerpath + "/locks")
    if not os.path.isdir(controllerpath + "/bkps") and (controllerconfigdoc["options"]["intermedlocal"] or controllerconfigdoc["options"]["outlocal"] or controllerconfigdoc["options"]["statslocal"]):
        os.mkdir(controllerpath + "/bkps")
    dependenciesfile = rootpath + "/modules/modules/" + controllerconfigdoc["controller"]["modname"] + "/dependencies"
    #resourcesstatefile = statepath + "/resources"
    #softwarestatefile = statepath + "/software"
    #globalmaxjobsfile = statepath + "/maxjobs"
    counterstatefile = controllerpath + "/batchcounter"
    statusstatefile = controllerpath + "/status"

    with open(statusstatefile, "w") as statusstream:
        statusstream.write("Starting controller.")
        statusstream.flush()
    
    #querystatefile = controllerpath + "/querystate"

    for f in glob.iglob(controllerpath + "/locks/*.lock"):
        os.remove(f)

    try:
        with open(dependenciesfile, "r") as dependenciesstream:
            dependencies = [x.rstrip('\n') for x in dependenciesstream.readlines()]
    except IOError:
        print "File path \"" + dependenciesfile + "\" does not exist."
        sys.stdout.flush()
        raise

    try:
        with open(counterstatefile, "r") as counterstream:
            counterheader = counterstream.readline()
            counterline = counterstream.readline()
            counters = [int(x) for x in counterline.rstrip("\n").split()]
    except IOError:
        counterheader = "Batch Step\n"
        counters = [1, 1]
        with open(counterstatefile, "w") as counterstream:
            counterstream.write(counterheader + ' '.join([str(x) for x in counters]))
            counterstream.flush()

    if controllerconfigdoc["db"]["type"] == "mongodb":
        import mongojoin
        if controllerconfigdoc["db"]["username"] == None:
            dbclient = mongojoin.MongoClient("mongodb://" + str(controllerconfigdoc["db"]["host"]) + ":" + str(controllerconfigdoc["db"]["port"]) + "/" + controllerconfigdoc["db"]["name"])
        else:
            dbclient = mongojoin.MongoClient("mongodb://" + controllerconfigdoc["db"]["username"] + ":" + controllerconfigdoc["db"]["password"] + "@" + str(controllerconfigdoc["db"]["host"]) + ":" + str(controllerconfigdoc["db"]["port"]) + "/" + controllerconfigdoc["db"]["name"] + "?authMechanism=SCRAM-SHA-1")

        #controllerconfigdoc["db"]["name"] = mongouri.split("/")[-1]
        db = dbclient[controllerconfigdoc["db"]["name"]]

        dbindexes = mongojoin.getintersectionindexes(db, controllerconfigdoc["db"]["basecollection"])
        #print dbindexes
        #sys.stdout.flush()

        allindexes = mongojoin.getunionindexes(db)
    else:
        raise Exception("Only \"mongodb\" is currently supported.")

    if controllerconfigdoc["module"]["language"] in crunchconfigdoc["software"].keys():
        scriptext = (crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]]["extension"] if "extension" in crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]].keys() else "")
        scriptcommand = (crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]]["command"] + " " if "command" in crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]].keys() else "")
        scriptflags = (" ".join(crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]]["flags"]) + " " if "flags" in crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]].keys() else "")
        needslicense = ("license-command" in crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]].keys())
        if needslicense:
            licensescript = crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]]["license-command"]
            sublicensescript = (crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]]["sublicense-command"] + " " if "sublicense-command" in crunchconfigdoc["software"][controllerconfigdoc["module"]["language"]].keys() else None)
            licensestatefile = localbinpath + "/" + licensescript
            #licensestream = open(licensestatefile, "a + ")
        else:
            licensescript = None
            sublicensescript = None
            #licensestream = None
    else:
        scriptext = ""
        scriptcommand = ""
        scriptflags = ""
        needslicense = False
        licensescript = None
        sublicensescript = None
        #licensestream = None

    if controllerconfigdoc["options"]["blocking"]:
        while prevcontrollerjobsrunningq(username, dependencies) and (timeleft(starttime, controllerbuffertimelimit) > 0):
            time.sleep(controllerconfigdoc["controller"]["sleeptime"])

    reloadjob = (controllerconfigdoc["db"]["query"][0] == "RELOAD")
    if reloadjob:
        reloadstatefilename = "reloadstate"
        if len(controllerconfigdoc["db"]["query"]) > 1:
            reloadpath = controllerpath + "/reload"
            reloadpattern = controllerconfigdoc["db"]["query"][1]
        if len(controllerconfigdoc["db"]["query"]) > 2:
            querylimit = controllerconfigdoc["db"]["query"][2]
        else:
            querylimit = None
    else:
        querystatefilename = "querystate"
        #basecollpattern = controllerconfigdoc["db"]["basecollection"]
        querylimitlist = [x[3]['LIMIT'] for x in controllerconfigdoc["db"]["query"] if (len(x) > 3) and ("LIMIT" in x[3].keys())]
        if len(querylimitlist) > 0:
            querylimit = functools.reduce(operator.mul, querylimitlist, 1)
        else:
            querylimit = None
    
    #dependencies = modlist[:modlist.index(controllerconfigdoc["controller"]["modname"])]

    #if firstlastrun and needslicense:
    #   fcntl.flock(pendlicensestream, fcntl.LOCK_UN)
    #firstrun = True

    #if querylimit != None:
    #    controllerconfigdoc["options"]["niters"] = min(controllerconfigdoc["options"]["niters"], querylimit)
    
    firstlastrun = (not (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])))
    #counters[0] = 1
    #counters[1] = 1
    while (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]) or firstlastrun) and ((querylimit == None) or (counters[1] <= querylimit + 1)) and (timeleft(starttime, controllerbuffertimelimit) > 0):
        #oldqueryresultinds = [dict([(y, x[y]) for y in dbindexes] + [(newfield, {"$exists": True})]) for x in queryresult]
        #if len(oldqueryresultinds) == 0:
        #    oldqueryresult = []
        #else:
        #    oldqueryresult = mongojoin.collectionfind(db, newcollection, {"$or": oldqueryresultinds}, dict([("_id", 0)] + [(y, 1) for y in dbindexes]))
        #oldqueryresultrunning = [y for x in userjobsrunninglist(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]) for y in contractededjobname2jobdocs(x, dbindexes) if len(x) > 0]
        #with open(querystatefile, "a") as iostream:
        #    for line in oldqueryresult + oldqueryresultrunning:
        #        iostream.write(str(dict([(x, line[x]) for x in allindexes if x in line.keys()])).replace(" ", "") + "\n")
        #        iostream.flush()
        #print "Next Run"
        #print "hi"
        #sys.stdout.flush()
        #print str(starttime) + " " + str(controllerbuffertimelimit)
        #sys.stdout.flush()
        #if not islimitreached(controllerpath, querylimit):
        if reloadjob:
            #requeueskippedreloadjobs(controllerconfigdoc, controllerpath, reloadstatefilename, reloadpath, counters, counterstatefile, counterheader, dbindexes)
            if (querylimit == None) or (counters[1] <= querylimit):
                counters = reloadcrawl(reloadpath, reloadpattern, controllerpath,
                                       reloadstatefilename = reloadstatefilename,
                                       inputfunc = lambda x: doinput(x, controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, rootpath, querylimit, counters, reloadjob, needslicense, username, controllerpath, querystatefilename, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes),
                                       inputdoc = doinput([], controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, rootpath, querylimit, counters, reloadjob, needslicense, username, controllerpath, querystatefilename, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes),
                                       action = lambda x, y, z: doaction(x, y, z, controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, querylimit, reloadjob, needslicense, username, controllerpath, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes, scriptcommand, scriptflags, scriptext, querystatefilename, counterstatefile, counterheader),
                                       filereadform = lambda x: x,
                                       filewriteform = lambda x: x,
                                       docwriteform = lambda x: "_".join(indexdoc2indexsplit(x, dbindexes)),
                                       timeleft = lambda: timeleft(starttime, controllerbuffertimelimit),
                                       counters = counters,
                                       counterupdate = lambda x: docounterupdate(x, counterstatefile, counterheader),
                                       resetstatefile = False,
                                       limit = querylimit)
        else:
            #requeueskippedqueryjobs(controllerconfigdoc, controllerpath, querystatefilename, counters, counterstatefile, counterheader, dbindexes)
            if (querylimit == None) or (counters[1] <= querylimit):
                if controllerconfigdoc["db"]["type"] == "mongodb":
                    counters = mongojoin.dbcrawl(db, controllerconfigdoc["db"]["query"], controllerpath,
                                                 statefilename = querystatefilename,
                                                 inputfunc = lambda x: doinput(x, controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, rootpath, querylimit, counters, reloadjob, needslicense, username, controllerpath, querystatefilename, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes),
                                                 inputdoc = doinput([], controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, rootpath, querylimit, counters, reloadjob, needslicense, username, controllerpath, querystatefilename, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes),
                                                 action = lambda x, y, z: doaction(x, y, z, controllerconfigdoc, globalmaxjobcount, localmaxjobcount, localmaxstepcount, partitionsmaxmemory, querylimit, reloadjob, needslicense, username, controllerpath, localbinpath, licensescript, sublicensescript, starttime, controllerbuffertimelimit, statusstatefile, dbindexes, scriptcommand, scriptflags, scriptext, querystatefilename, counterstatefile, counterheader),
                                                 readform = lambda x: indexsplit2indexdoc(x.split("_")[2:], dbindexes),
                                                 writeform = lambda x: controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_" + "_".join(indexdoc2indexsplit(x, dbindexes)),
                                                 timeleft = lambda: timeleft(starttime, controllerbuffertimelimit),
                                                 counters = counters,
                                                 counterupdate = lambda x: docounterupdate(x, counterstatefile, counterheader),
                                                 resetstatefile = False,
                                                 limit = querylimit,
                                                 limittries = 10,
                                                 toplevel = True)
        #print "bye"
        #firstrun = False
        releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
        if (timeleft(starttime, controllerbuffertimelimit) > 0):
            firstlastrun = (not (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]) or firstlastrun))
    #while controllerjobsrunningq(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]) and (timeleft(starttime, controllerbuffertimelimit) > 0):
    #    releaseheldjobs(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"])
    #    skippedjobs = skippedjobslist(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"], workpath)
    #    for x in skippedjobs:
    #        maxmemorypernode = getmaxmemorypernode(resourcesstatefile, controllerpartition)
    #        submitjob(workpath, x,controllerpartition, maxmemorypernode, maxmemorypernode, resubmit = True)
    #    time.sleep(controllerconfigdoc["controller"]["sleeptime"])

    if (prevcontrollerjobsrunningq(username, dependencies) or controllerjobsrunningq(username, controllerconfigdoc["controller"]["modname"], controllerconfigdoc["controller"]["controllername"]) or firstlastrun) and not (timeleft(starttime, controllerbuffertimelimit) > 0):
        #Resubmit controller job
        #maxmemorypernode = getmaxmemorypernode(resourcesstatefile, controllerpartition)
        assert isinstance(crunchconfigdoc["resources"][controllerpartition], int)
        maxmemorypernode = crunchconfigdoc["resources"][controllerpartition]

        loadpathnames = glob.iglob(controllerpath + "/docs/*.docs")
        #with open(controllerpath + "/skipped", "a") as skippedstream:
        #    for loadpathname in loadpathnames:
        #        loadfilename = loadpathname.split("/")[-1]
        #        errloadpathname = loadpathname.replace(".docs", ".err")
        #        errcode = "-1:0"
        #        if os.path.exists(errloadpathname):
        #            with open(errloadpathname, "r") as errstream:
        #                for errline in errstream:
        #                    if "ExitCode: " in errline:
        #                        errcode = errline.rstrip("\n").replace("ExitCode: ", "")
        #                        break
        #        skippedstream.write(loadfilename + ", " + errcode + ", True\n")
        #        skippedstream.flush()

        submitcontrollerjob(controllerpath, "crunch_" + controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_controller", controllernnodes, controllerncores, controllerpartition, maxmemorypernode, resubmit = True)
        with open(statusstatefile, "w") as statusstream:
            statusstream.write("Resubmitting controller.")
            statusstream.flush()

    else:
        #if pdffile != "":
        #    plotjobgraph(controllerconfigdoc["controller"]["modname"], controllerpath, controllerconfigdoc["controller"]["controllername"], workpath, pdffile)
        for f in glob.iglob(controllerpath + "/locks/*.lock"):
            os.remove(f)
        os.rmdir(controllerpath + "/locks")
        with open(statusstatefile, "w") as statusstream:
            statusstream.write("Completing controller.")
            statusstream.flush()
        print ""
        print datetime.datetime.utcnow().strftime("%Y %m %d %H:%M:%S UTC")
        print "Completing job crunch_" + controllerconfigdoc["controller"]["modname"] + "_" + controllerconfigdoc["controller"]["controllername"] + "_controller\n"
        sys.stdout.flush()

    #querystream.close()
    #seekstream.close()
    #if needslicense:
        #fcntl.flock(pendlicensestream, fcntl.LOCK_UN)
    #    licensestream.close()
    if controllerconfigdoc["db"]["type"] == "mongodb":
        dbclient.close()
except Exception as e:
    PrintException()