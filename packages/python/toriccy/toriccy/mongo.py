import sys,os,tempfile;
from pymongo import MongoClient;
from math import ceil;
from . import tools;
from . import parse;

def collectionfind(db,collection,query,projection,formatresult="string"):
    "Query specific collection in database."
    if projection=="count":
        result=db[collection].find(query).count();
    else:
        stringresult=list(db[collection].find(query,projection));
        if formatresult=="string":
            result=stringresult;
        elif formatresult=="expression":
            result=parse.string2expression(stringresult);
        else:
            result=None;
    #return [dict(zip(y.keys(),[mat2py(y[x]) for x in y.keys()])) for y in result];
    return result;

def gettiers(db):
    "Return all tiers (i.e. collections) of database."
    return [x["TIER"] for x in collectionfind(db,"TIERS",{},{"_id":0,"TIER":1})];

#def getindexes(db,collection="all"):
#    "Return all indexes for a collection."
#    if collection=="all":
#        query={};
#    else:
#        query={"TIER":collection};
#    result=tools.deldup([x["INDEX"] for x in collectionfind(db,"INDEXES",query,{"_id":0,"INDEX":1})]);
#    return result;

def getunionindexes(db,*collections):
    if len(collections)==0:
        tierquery={};
    else:
        tierquery={"TIER":{"$in":collections}};
    sortedindexdocs=sorted(collectionfind(db,"INDEXES",tierquery,{"_id":0,"TIERID":1,"TIER":1,"INDEXID":1,"INDEX":1}),key=lambda x:(x["TIERID"],x["INDEXID"]));
    unionindexes=tools.deldup([x["INDEX"] for x in sortedindexdocs]);
    return unionindexes;

def getintersectionindexes(db,*collections):
    if len(collections)==0:
        tierquery={};
    else:
        tierquery={"TIER":{"$in":collections}};
    sortedindexdocs=sorted(collectionfind(db,"INDEXES",tierquery,{"_id":0,"TIERID":1,"TIER":1,"INDEXID":1,"INDEX":1}),key=lambda x:(x["TIERID"],x["INDEXID"]));
    unionindexes=tools.deldup([x["INDEX"] for x in sortedindexdocs]);
    indexgroups=[[x for x in sortedindexdocs if x["INDEX"]==y] for y in unionindexes];
    intersectionindexes=[x[0]["INDEX"] for x in indexgroups if all([y["TIER"] in collections for y in x]) and all([z in [y["TIER"] for y in x] for z in collections])];
    return intersectionindexes;

def getcomplementindexes(db,*collections):
    return [x for x in getunionindexes(db,*collections) if x not in getintersectionindexes(db,*collections)];

def gettierfromdoc(db,doc):
    tiers=gettiers(db);
    indexes=getintersectionindexes(db);
    dbindexes=[x for x in doc.keys() if x in indexes];
    i=0;
    tierindexes=getintersectionindexes(db,tiers[i]);
    while (i<len(tiers)) and not (all([x in tierindexes for x in dbindexes]) and all([x in dbindexes for x in tierindexes])):
        i+=1;
        tierindexes=getintersectionindexes(db,tiers[i]);
    if i<len(tiers):
        return tiers[i];
    else:
        return "";

def collectionfieldexists(db,collection,field):
    "Check if specific field exists in the collection."
    result=db[collection].find({},{"_id":0,field:1}).limit(1).next()!={};
    return result;

#def listindexes(db,commonindexes,distribfilter,filters):
#    "List the minimal sets of indexes from one collection's query required to specify documents in the next collection's query."
#    if len(commonindexes)==0:
#        indexlist=distribfilter;
#    else:
#        #indexlist=tools.deldup([dict([(x,z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters]);
#        indexlist=[{"$and":[dict([z]) for z in distribfilter.items()]+[{x:y[x]} for x in commonindexes]} for y in filters];
#    return indexlist;

def listindexes(db,distribfilter,commonindexes,filters):
    "List the minimal sets of indexes from one collection's query required to specify documents in the next collection's query."
    if len(commonindexes)==0:
        indexlist=distribfilter;
    else:
        #indexlist=tools.deldup([dict([(x,z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters]);
        indexlist={"$or":[{"$and":[dict([z]) for z in distribfilter.items()]+[{x:y[x]} for x in commonindexes]} for y in filters]};
    return indexlist;

#def sameindexes(filter1,filter2,indexes):
#    "Check whether documents from two different collection's queries share the same minimal indexes and should be concatenated."
#    return all([filter1[x]==filter2[x] for x in filter1 if (x in indexes) and (x in filter2)]);

def mergenextquery(db,commonindexes,nextquery,prevresult,chunk=100,formatresult="string"):
    n=int(ceil(float(len(prevresult))/float(chunk)));
    totalresult=[];
    for k in range(n):
        chunkprevresult=prevresult[k*chunk:(k+1)*chunk];
        chunkindexlist=listindexes(db,nextquery[1],commonindexes,chunkprevresult);
        chunknextresult=collectionfind(db,nextquery[0],chunkindexlist,nextquery[2],formatresult=formatresult);
        chunktotalresult=[dict(x.items()+y.items()) for x in chunkprevresult for y in chunknextresult if all([x[z]==y[z] for z in commonindexes])];
        #print str(k+1)+" of "+str(n);
        totalresult+=chunktotalresult;
    return totalresult;

#def querydatabase(db,queries,formatresult="string"):
#    "Query all collections in the database and concatenate the documents of each that refer to the same object."
#    tiersord=dict([(x["TIER"],x["TIERID"]) for x in collectionfind(db,"TIERS",{},{"_id":0,"TIER":1,"TIERID":1})]);
#    indexes=getintersectionindexes(db);
#    maxquerylen=max([len(x[1]) for x in queries]);
#    sortedprojqueries=sorted([y for y in queries if y[2]!="count"],key=lambda x: (maxquerylen-len(x[1]),tiersord[x[0]]));
#    maxcountquery=[] if len(queries)==len(sortedprojqueries) else [max([y for y in queries if y not in sortedprojqueries],key=lambda x: len(x[1]))];
#    sortedqueries=sortedprojqueries+maxcountquery;
#    totalresult=collectionfind(db,*sortedqueries[0],formatresult=formatresult);
#    if sortedqueries[0][2]=="count":
#        return totalresult;
#    for i in range(1,len(sortedqueries)):
#        indexlist=listindexes(db,[x[0] for x in sortedqueries[:i+1]],totalresult,indexes);
#        if len(indexlist)==0:
#            orgroup=sortedqueries[i][1];
#        else:
#            orgroup=dict(sortedqueries[i][1].items()+{"$or":indexlist}.items());
#        nextresult=collectionfind(db,sortedqueries[i][0],orgroup,sortedqueries[i][2],formatresult=formatresult);
#        if sortedqueries[i][2]=="count":
#            return nextresult;
#        totalresult=[dict(x.items()+y.items()) for x in totalresult for y in nextresult if sameindexes(x,y,indexes)];
#    return totalresult;

def querydatabase(db,queries,chunk=100,formatresult="string"):
    "Query all collections in the database and concatenate the documents of each that refer to the same object."
    tiersord=dict([(x["TIER"],x["TIERID"]) for x in collectionfind(db,"TIERS",{},{"_id":0,"TIER":1,"TIERID":1})]);
    maxquerylen=max([len(x[1]) for x in queries]);
    sortedprojqueries=sorted([y for y in queries if y[2]!="count"],key=lambda x: (maxquerylen-len(x[1]),tiersord[x[0]]));
    maxcountquery=[] if len(queries)==len(sortedprojqueries) else [max([y for y in queries if y not in sortedprojqueries],key=lambda x: len(x[1]))];
    sortedqueries=sortedprojqueries+maxcountquery;
    totalresult=collectionfind(db,*sortedqueries[0],formatresult=formatresult);
    if sortedqueries[0][2]=="count":
        return len(totalresult);
    for i in range(1,len(sortedqueries)):
        prevcollections=[x[0] for x in sortedqueries[:i+1]];
        commonindexes=getintersectionindexes(db,*prevcollections);
        totalresult=mergenextquery(db,commonindexes,sortedqueries[i],totalresult,chunk=chunk,formatresult=formatresult);
        if sortedqueries[i][2]=="count":
            return len(totalresult);
    return totalresult;

def updatequerystate(queries,statefilepath,statefilename,allcollindexes,docbatch,endofdocs,readform=lambda x:eval(x),writeform=lambda x:x):
    #Compress docbatch to top tier that has completed
    i=len(docbatch)-1;
    while i>=0:
        if True in endofdocs[i]:
            j=endofdocs[i].index(True);
            if j<len(endofdocs[i])-1: 
                k=0;
                while k<i:
                    if all([docbatch[i][x]==docbatch[k][x] for x in allcollindexes[j]]):
                        docbatch=docbatch[:k]+docbatch[k+1:];
                        endofdocs=endofdocs[:k]+endofdocs[k+1:];
                        k-=1;
                        i-=1;
                    k+=1;
            docbatch[i]=dict([(x,docbatch[i][x]) for x in allcollindexes[j]]);
            with open(statefilepath+"/"+statefilename+queries[j][0],"a") as querystatestream:
                #alliostreams[j].seek(0,2);
                querystatestream.write(str(writeform(docbatch[i])).replace(" ","")+"\n");
                querystatestream.flush();
        i-=1;
    #Update querystate files by removing the subdocument records of completed documents
    minupdatetier=min([x.index(True) for x in endofdocs])+1;
    for i in range(minupdatetier,len(queries)):
        try:
            with open(statefilepath+"/"+statefilename+queries[i][0],"r") as querystatestream, tempfile.NamedTemporaryFile(dir=statefilepath,delete=False) as tempstream:
                for line in querystatestream:
                    linesubdoc=readform(line.rstrip("\n"));
                    #If not a subdocument of any document in the list, keep it
                    if not any([all([x in linesubdoc.items() for x in docbatch[j].items()]) for j in range(len(endofdocs)) if endofdocs[j].index(True)<i]):
                        tempstream.write(line);
                        tempstream.flush();
                os.rename(tempstream.name,querystatestream.name);
        except IOError:
            querystatestream=open(statefilepath+"/"+statefilename+queries[i][0],"w");
            querystatestream.close();

def printasfunc(*args):
    print list(args)[-1];
    sys.stdout.flush();

def dbcrawl(db,queries,statefilepath,statefilename="querystate",inputfunc=lambda x:{"nsteps":1},inputdoc={"nsteps":1},action=printasfunc,readform=lambda x:eval(x),writeform=lambda x:x,stopat=lambda:False,batchcounter=1,stepcounter=1,resetstatefile=False,toplevel=True):
    docbatch=[];
    endofdocs=[];
    if toplevel:
        if resetstatefile:
            for x in queries:
                querystatestream=open(statefilepath+"/"+statefilename+x[0],"w");
                querystatestream.close();
        allprojfields=[y[0] for x in queries for y in x[2].items() if y[1]==1];
        allcollindexes=[getintersectionindexes(db,x[0]) for x in queries];
        thiscollindexes=allcollindexes[0];
    else:
        thiscollindexes=getintersectionindexes(db,queries[0][0]);
    prevfilters=[];
    try:
        thisiostream=open(statefilepath+"/"+statefilename+queries[0][0],"r");
        for line in thisiostream:
            linedoc=readform(line.rstrip("\n"));
            if all([linedoc[x]==queries[0][1][x] for x in thiscollindexes if x in queries[0][1].keys()]):
                linefilter={"$or":[{x:{"$ne":linedoc[x]}} for x in thiscollindexes if x not in queries[0][1].keys()]};
                prevfilters+=[linefilter];
    except IOError:
        thisiostream=open(statefilepath+"/"+statefilename+queries[0][0],"w");
    thisiostream.close();
    newquerydoc={"$and":[dict([x]) for x in queries[0][1].items()]+prevfilters};
    newprojdoc=dict(queries[0][2].items()+[(x,1) for x in thiscollindexes]+[("_id",0)]);
    thisquery=[queries[0][0],newquerydoc,newprojdoc];
    docs=collectionfind(db,*thisquery);
    i=0;
    while (i<len(docs)) and not stopat():
        doc=docs[i];
        if len(queries)>1:
            commonindexes=getintersectionindexes(db,queries[0][0],queries[1][0]);
            nextqueries=[[queries[1][0],dict(queries[1][1].items()+[(x,doc[x]) for x in commonindexes]),queries[1][2]]]+queries[2:];
            newinputdoc=inputdoc.copy();
            newinputdoc.update({"nsteps":inputdoc["nsteps"]-len(docbatch)});
            [endofsubdocs,subdocbatch]=dbcrawl(db,nextqueries,statefilepath,statefilename=statefilename,inputfunc=inputfunc,inputdoc=newinputdoc,action=action,readform=readform,writeform=writeform,stopat=stopat,batchcounter=batchcounter,stepcounter=stepcounter,resetstatefile=resetstatefile,toplevel=False);
        else:
            [endofsubdocs,subdocbatch]=[[[True]],[{}]];

        docbatch+=[dict(doc.items()+x.items()) for x in subdocbatch];

        if toplevel:
            endofdocs+=endofsubdocs;
            if len(docbatch)==inputdoc["nsteps"]:
                docbatchprojfields=[dict([y for y in x.items() if y[0] in allprojfields]) for x in docbatch];
                action(batchcounter,stepcounter,inputdoc,docbatchprojfields);
                inputdoc.update(inputfunc(docbatchprojfields));
                updatequerystate(queries,statefilepath,statefilename,allcollindexes,docbatch,endofdocs,readform=readform,writeform=writeform);
                batchcounter+=1;
                stepcounter+=len(docbatch);
                #docbatchtier=[];
                #for j in range(len(docbatch)):
                #    linedoc=docbatch[j];
                #    k=endofdocs[j].index(True);
                #    linedoctrim=dict([(x,linedoc[x]) for x in allcollindexes[k]]);
                #    if linedoctrim not in docbatchtier:
                #        alliostreams[k].seek(0,2);
                #        alliostreams[k].write(str(writeform(linedoctrim)).replace(" ","")+"\n");
                #        alliostreams[k].flush();
                #        docbatchtier+=[linedoctrim];
                #        for l in range(k+1,len(endofdocs[j])):
                #            alliostreams[l].seek(0,0);
                #            with tempfile.NamedTemporaryFile(dir=statefilepath,delete=False) as tempstream:
                #                for line in alliostreams[l]:
                #                    linesubdoc=readform(line.rstrip("\n"));
                #                    if not (all([x in linesubdoc.items() for x in linedoctrim.items()]) or all([x in linedoctrim.items() for x in linesubdoc.items()])):
                #                        tempstream.write(line);
                #                        tempstream.flush();
                #                alliostreams[l].close();
                #                os.rename(tempstream.name,statefilepath+"/"+statefilename+queries[l][0]);
                #                alliostreams[l]=open(statefilepath+"/"+statefilename+queries[l][0],"a+");
                #    stepcounter+=1;
                #batchcounter+=1;
                docbatch=[];
                if not endofdocs[-1][0]:
                    i-=1;
                endofdocs=[];
        else:
            endofdocs+=[[all(x) and (i==len(docs)-1)]+x for x in endofsubdocs];
            if len(docbatch)==inputdoc["nsteps"]:
                return [endofdocs,docbatch];
        i+=1;
    if toplevel:
        if len(docbatch)>0:
            docbatchprojfields=[dict([y for y in x.items() if y[0] in allprojfields]) for x in docbatch];
            action(batchcounter,stepcounter,inputdoc,docbatchprojfields);
            updatequerystate(queries,statefilepath,statefilename,allcollindexes,docbatch,endofdocs,readform=readform,writeform=writeform);
            batchcounter+=1;
            stepcounter+=len(docbatch);
        return [batchcounter,stepcounter];
    else:
        return [endofdocs,docbatch];

'''
username="frontend";
password="password";
server="129.10.135.170";
port="27017";
dbname="ToricCY";

client=MongoClient("mongodb://"+username+":"+password+"@"+server+":"+port+"/"+dbname);
db=client[dbname];

statepath="/gss_gpfs_scratch/altman.ro/ToricCY/packages/state/";

tiersstream=open(statepath+"tiers","r");
ToricCYTiers=[x.rstrip("\n") for x in tiersstream.readlines()];
tiersstream.close();

indexesstream=open(statepath+"indexes","r");
ToricCYIndexes=[x.rstrip("\n") for x in indexesstream.readlines()];
indexesstream.close();
'''