#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,linecache,traceback,operator,itertools,json,mongolink;
from copy import deepcopy;
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;

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

def disjointNIDs(NIDpairs,swaplist=[]):
    if len(NIDpairs)==0:
        return swaplist;
    fullswaplist=[];
    firstflag=True;
    for NIDpair in NIDpairs:
        newNIDpairs=[x for x in NIDpairs if (x[0]>min(NIDpair)) and (x[1]>min(NIDpair)) and all([i not in x for i in NIDpair])];
        if len(swaplist)==0:
            newswaplist=[[NIDpair]];
        else:
            newswaplist=[swaplist[-1]+[NIDpair]];
        if firstflag:
            newswaplist=swaplist+newswaplist;
        fullswaplist+=disjointNIDs(newNIDpairs,newswaplist);
        firstflag=False;
    if len(swaplist)==0:
        fullswaplist=sorted(fullswaplist,key=lambda x:(len(x),x[0]));
    return fullswaplist;

try:
    docsfile=sys.argv[1];
    dbindexes=sys.argv[2:];
    with open(docsfile,"r") as docstream:
        for line in docstream:
            triangdoc=json.loads(line.rstrip("\n"));

            rescws=mat2py(triangdoc['RESCWS']);
            divcohom=mat2py(triangdoc['DIVCOHOM']);
            itensXD=mat2py(triangdoc['ITENSXD']);
            SRideal=triangdoc['SRIDEAL'];
            SRsets=sorted([[y-1 for y in eval(("["+x+"]").replace("D","").replace("*",","))] for x in SRideal.lstrip("{").rstrip("}").split(",")],key=lambda x:(len(x),operator.itemgetter(*range(len(x)))(x)));
            itensXDsets=[];
            for i in range(len(rescws)):
                for j in range(i,len(rescws)):
                    for k in range(j,len(rescws)):
                        itensXDsets+=[[[i,j,k],itensXD[i][j][k]]];
            itensXDsets=sorted(itensXDsets,key=lambda x:(len(x[0]),operator.itemgetter(*range(len(x[0])))(x[0])));

            newdivcohom=[py2mat(x) for x in divcohom];

            NIDpairs=[];
            for i in range(len(rescws)):
                for j in range(i+1,len(rescws)):
                    if (divcohom[i]==divcohom[j]) and (rescws[i]!=rescws[j]):
                        NIDpairs+=[[i,j]];

            #disjointsets=[];
            #for i in range(len(NIDpairs)):
            #    combs=list(itertools.combinations(NIDpairs,i+1));
            #    for comb in combs:
            #        if all([not any([k in y for k in comb[j] for y in comb[:j]+comb[j+1:]]) for j in range(len(comb))]):
            #            disjointsets+=[list(comb)];

            disjointsets=disjointNIDs(NIDpairs);

            allowedinvols=[];
            involn=1;
            for invol in disjointsets:
                newSRsets=[];
                for SRset in SRsets:
                    newSRset=SRset;
                    for x in invol:
                        newSRset=[x[1] if y==x[0] else x[0] if y==x[1] else y for y in newSRset];
                    newSRset=sorted(newSRset);
                    newSRsets+=[newSRset];
                newSRsets=sorted(newSRsets,key=lambda x:(len(x),operator.itemgetter(*range(len(x)))(x)));
                
                newitensXDsets=[];
                for itensXDset in itensXDsets:
                    newitensXDset=itensXDset[0];
                    for x in invol:
                        newitensXDset=[x[1] if y==x[0] else x[0] if y==x[1] else y for y in newitensXDset];
                    newitensXDset=[sorted(newitensXDset),itensXDset[1]];
                    newitensXDsets+=[newitensXDset];
                newitensXDsets=sorted(newitensXDsets,key=lambda x:(len(x[0]),operator.itemgetter(*range(len(x[0])))(x[0])));
                
                if (newSRsets==SRsets) or (newitensXDsets==itensXDsets):
                    matinvol="{"+",".join([",".join(["D"+str(x[0]+1)+"->D"+str(x[1]+1),"D"+str(x[1]+1)+"->D"+str(x[0]+1)]) for x in invol])+"}";
                    SRinvol=(newSRsets==SRsets);
                    itensXDinvol=(newitensXDsets==itensXDsets);
                    involquery={"POLYID":triangdoc['POLYID'],"GEOMN":triangdoc['GEOMN'],"TRIANGN":triangdoc['TRIANGN'],"INVOLN":involn};
                    newinvolout={"H11":triangdoc['H11'],"POLYID":triangdoc['POLYID'],"GEOMN":triangdoc['GEOMN'],"TRIANGN":triangdoc['TRIANGN'],"INVOLN":involn,"INVOL":matinvol,"INVOLDIVCOHOM":[py2mat(divcohom[x[0]]) for x in invol],"SRINVOL":SRinvol,"ITENSXDINVOL":itensXDinvol};
                    print("+INVOL."+json.dumps(involquery,separators=(',',':'))+">"+json.dumps(newinvolout,separators=(',',':')));
                    sys.stdout.flush();
                    involn+=1;
            involn-=1;

            triangquery={"POLYID":triangdoc['POLYID'],"GEOMN":triangdoc['GEOMN'],"TRIANGN":triangdoc['TRIANGN']};
            newtriangoutput={"DIVCOHOM1":newdivcohom,"NINVOL":involn};

            print("+TRIANG."+json.dumps(triangquery,separators=(',',':'))+">"+json.dumps(newtriangoutput,separators=(',',':')));
            print("@TRIANG."+json.dumps(dict([(x,triangdoc[x]) for x in dbindexes]),separators=(',',':')));
            sys.stdout.flush();
except Exception as e:
    PrintException();