#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

from pymongo import MongoClient;
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

def getindexes(db,collection="all"):
    "Return all indexes for a collection."
    if collection=="all":
        indexes=tools.deldup([y["INDEX"] for x in collectionfind(db,"TIERS",{},{"_id":0,"INDEXES.INDEX":1}) for y in x["INDEXES"]]);
    else:
        indexes=[y["INDEX"] for x in collectionfind(db,"TIERS",{"TIER":collection},{"_id":0,"INDEXES.INDEX":1}) for y in x["INDEXES"]];
    return indexes;

def collectionfieldexists(db,collection,field):
    "Check if specific field exists in the collection."
    result=db[collection].find({},{"_id":0,field:1}).limit(1).next()!={};
    return result;

def listindexes(db,collection,filters):
    "List the minimal sets of indexes from one collection's query required to specify documents in the next collection's query."
    indexes=getindexes(db);
    trueindexes=[x for x in indexes if collectionfieldexists(db,collection,x)]
    if len(trueindexes)==0:
        return [];
    indexlist=tools.deldup([dict([(x,z[x]) for x in trueindexes if all([x in y.keys() for y in filters])]) for z in filters]);
    return indexlist;

def sameindexes(db,filter1,filter2):
    "Check whether documents from two different collection's queries share the same minimal indexes and should be concatenated."
    indexes=getindexes(db);
    return all([filter1[x]==filter2[x] for x in filter1 if (x in indexes) and (x in filter2)]);

def querydatabase(db,queries,formatresult="string"):
    "Query all collections in the database and concatenate the documents of each that refer to the same object."
    tiers=gettiers(db);
    sortedprojqueries=sorted([y for y in queries if y[2]!="count"],key=lambda x: (len(x[1]),tiers.index(x[0])),reverse=True);
    maxcountquery=[] if len(queries)==len(sortedprojqueries) else [max([y for y in queries if y not in sortedprojqueries],key=lambda x: len(x[1]))];
    sortedqueries=sortedprojqueries+maxcountquery;
    totalresult=collectionfind(db,*sortedqueries[0],formatresult=formatresult);
    if sortedqueries[0][2]=="count":
        return totalresult;
    for i in range(1,len(sortedqueries)):
        indexlist=listindexes(db,sortedqueries[i][0],totalresult);
        if len(indexlist)==0:
            orgroup=sortedqueries[i][1];
        else:
            orgroup=dict(sortedqueries[i][1].items()+{"$or":indexlist}.items());
        nextresult=collectionfind(db,sortedqueries[i][0],orgroup,sortedqueries[i][2],formatresult=formatresult);
        if sortedqueries[i][2]=="count":
            return nextresult;
        totalresult=[dict(x.items()+y.items()) for x in totalresult for y in nextresult if sameindexes(db,x,y)];
    return totalresult;

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