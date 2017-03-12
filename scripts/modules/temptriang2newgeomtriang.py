#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,subprocess,signal,json,mongolink;
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;

#################################################################################
#Misc. function definitions
def PrintException():
    "If an exception is raised, print traceback of it to output log."
    exc_type,exc_obj,tb=sys.exc_info();
    f=tb.tb_frame;
    lineno=tb.tb_lineno;
    filename=f.f_code.co_filename;
    linecache.checkcache(filename);
    line=linecache.getline(filename,lineno,f.f_globals);
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename,lineno,line.strip(),exc_obj);
    print "More info: ",traceback.format_exc();

def default_sigpipe():
    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

#################################################################################
#Module-specific function definitions

def match(itensXD_pair,c2Xnums_pair,eX_pair,mori_rows_pair):
    "Determine if the above pairs of values match."
    #inter=Cone([vector(x) for x in mori_rows_pair[0]]).intersection(Cone([-vector(x) for x in mori_rows_pair[1]])).rays().column_matrix().columns();
    potentialflop=False;
    flop=True;
    i=0;
    while (i<len(mori_rows_pair[0])) and flop:
        row0=mori_rows_pair[0][i];
        j=0;
        while (j<len(mori_rows_pair[1])) and flop:
            row1=mori_rows_pair[1][j];
            flopinds=[k for k in range(len(row0)) if (row0[k]==-row1[k]) and (row0[k]!=0)];
            #Toric divisor indices whose intersection defines a singularity in the wall
            if all([row0[k]==row1[k] for k in range(len(row0))]) or all([row0[k]==-row1[k] for k in range(len(row0))]):
                #Two Kahler cones share a wall (they either overlap or are adjacent)
                potentialflop=True;
                if len(flopinds)>0:
                    #There is a singularity in the wall
                    range0=[k for k in flopinds if row0[k]<0];
                    range1=[k for k in flopinds if row1[k]<0];
                    if (len(range0)>0) and (len(range1)>0):
                        #The singularity is contained within some intersection of divisors in both Kahler cones
                        set0=Set(range0).subsets(min(len(range0),3)).list();
                        set1=Set(range1).subsets(min(len(range1),3)).list();
                        inums0=[mongolink.nestind(itensXD_pair[0],list(y)) for y in set0];
                        inums1=[mongolink.nestind(itensXD_pair[1],list(y)) for y in set1];
                        flop=flop and all([y==0 for y in flatten(inums0+inums1)]);
                    else:
                        #The singularity is not contained in any intersection of divisors for at least one Kahler cone, so it cannot be checked the the CY avoids it
                        flop=False;
            j+=1;
        i+=1;
    flop=flop and potentialflop;
    return (itensXD_pair[0]==itensXD_pair[1]) and (Set(c2Xnums_pair[0])==Set(c2Xnums_pair[1])) and (eX_pair[0]==eX_pair[1]) and flop;

def glue_groups(itensXD_L,c2Xnums_L,eX_L,mori_rows_L):
    "Determine the sets of triangulations that correspond to a unique Calabi-Yau threefold and should be glued according to both Wall's theorem and the flop-tracing method."
    groups=[inds for inds in Set(range(len(itensXD_L))).subsets(2).list() if match([itensXD_L[i] for i in inds],[c2Xnums_L[i] for i in inds],[eX_L[i] for i in inds],[mori_rows_L[i] for i in inds])];
    i=0;
    while i<len(groups):
        origgroup=groups[i];
        j=i+1;
        while j<len(groups):
            if len(groups[i].intersection(groups[j]))>0:
                groups[i]=groups[i].union(groups[j]);
                groups=groups[:j]+groups[j+1:];
                j-=1;
            j+=1;
        if groups[i]!=origgroup:
            i-=1;
        i+=1;
    groups_list=[list(x) for x in groups];
    return groups_list+[[i] for i in range(len(itensXD_L)) if not any([i in x for x in groups_list])];

def glue_mori(DtoJmat,mori_rows_group):
    "Compute Mori and Kahler cones for glued geometries."
    g_mori=Cone(mori_rows_group[0]);
    for x in mori_rows_group[1:]:
        g_mori=g_mori.intersection(Cone(x));
    g_mori_rows=[list(x) for x in g_mori.rays().column_matrix().columns()];
    g_mori_cols=mongolink.transpose_list(g_mori_rows);
    g_kahler_cols=[sum([DtoJmat[k][j]*vector(g_mori_cols[j]) for j in range(len(g_mori_cols))]) for k in range(len(DtoJmat))];
    g_kahler_rows=mongolink.transpose_list(g_kahler_cols);
    return [g_mori_rows,g_kahler_rows];

#################################################################################
#Main body
try:
    #IO Definitions
    polydoc=json.loads(sys.argv[1]);
    #Read in pertinent fields from JSON
    polyid=polydoc['POLYID'];
    h11=polydoc['H11'];
    DtoJmat=mat2py(polydoc['DTOJ']);

    packagepath=subprocess.Popen("echo \"${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
    statepath=packagepath+"/state";
    mongourifile=statepath+"/mongouri";
    with open(mongourifile,"r") as mongouristream:
        mongouri=mongouristream.readline().rstrip("\n");

    mongoclient=mongolink.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
    dbname=mongouri.split("/")[-1];
    db=mongoclient[dbname];
    triangdocs=mongolink.collectionfind(db,'TRIANGtemp',{'H11':h11,'POLYID':polyid},{'_id':0,'GEOMN':1,'TRIANGN':1,'ALLTRIANGN':1,'BASIS':1,'EULER':1,'JTOD':1,'INVBASIS':1,'CHERN2XJ':1,'CHERN2XNUMS':1,'IPOLYXJ':1,'ITENSXJ':1,'TRIANG':1,'SRIDEAL':1,'CHERN2XD':1,'IPOLYAD':1,'ITENSAD':1,'IPOLYXD':1,'ITENSXD':1,'IPOLYAJ':1,'ITENSAJ':1,'CHERNAD':1,'CHERNAJ':1,'CHERN3XD':1,'CHERN3XJ':1,'MORIMATP':1,'KAHLERMATP':1},formatresult='string');
    mongoclient.close();
    #print triangdocs[0];
    #Create lists of properties to check for consistency when gluing
    itensXD_L=[];
    c2Xnums_L=[];
    eX_L=[];
    mori_rows_L=[];
    for x in triangdocs:
        itensXD_L+=[mat2py(x['ITENSXD'])];
        c2Xnums_L+=[mat2py(x['CHERN2XNUMS'])];
        eX_L+=[x['EULER']];
        mori_rows_L+=[mat2py(x['MORIMATP'])];
    #Determine which triangulations to glue together
    to_glue_L=glue_groups(itensXD_L,c2Xnums_L,eX_L,mori_rows_L);
    #print to_glue_L;
    #sys.stdout.flush();
    #Compress properties that should remain the same across the whole polytope into single variables
    basis=triangdocs[0]['BASIS'];
    JtoDmat=triangdocs[0]['JTOD'];
    invbasis=triangdocs[0]['INVBASIS'];
    #Add new properties to base tier of JSON
    #print "+POLY."+json.dumps({'POLYID':polyid},separators=(',',':'))+">"+json.dumps({'BASIS':basis,'EULER':int(eX_L[0]),'NGEOMS':len(to_glue_L),'JTOD':JtoDmat,'INVBASIS':invbasis},separators=(',',':'));
    #sys.stdout.flush();
    #Glue triangulations into their compositie geometries
    g_mori_rows_L=[];
    g_kahler_rows_L=[];
    for i in range(len(to_glue_L)):
        #Compute glued Mori and Kahler cone matrices
        mori_rows_group=[mori_rows_L[j] for j in to_glue_L[i]];
        g_mori_rows,g_kahler_rows=glue_mori(DtoJmat,mori_rows_group);
        g_mori_rows_L+=[g_mori_rows];
        g_kahler_rows_L+=[g_kahler_rows];
        #Add new properties to GEOMDATA tier of JSON
        j=to_glue_L[i][0];
        print "+GEOM."+json.dumps({'POLYID':polyid,'GEOMN':i+1},separators=(',',':'))+">"+json.dumps({'POLYID':polyid,'GEOMN':i+1,'H11':h11,'NTRIANGS':len(to_glue_L[i]),'CHERN2XJ':triangdocs[j]['CHERN2XJ'],'CHERN2XNUMS':triangdocs[j]['CHERN2XNUMS'],'IPOLYXJ':triangdocs[j]['IPOLYXJ'],'ITENSXJ':triangdocs[j]['ITENSXJ'],'MORIMAT':py2mat(g_mori_rows),'KAHLERMAT':py2mat(g_kahler_rows)},separators=(',',':'));
        sys.stdout.flush();
        #Add new properties to triangdocs tier of JSON
        m=0;
        for k in to_glue_L[i]:
            print "+TRIANG."+json.dumps({'POLYID':polyid,'GEOMN':i+1,'TRIANGN':m+1},separators=(',',':'))+">"+json.dumps({'POLYID':polyid,'GEOMN':i+1,'TRIANGN':m+1,'ALLTRIANGN':triangdocs[k]['ALLTRIANGN'],'OLDGEOMN':triangdocs[k]['GEOMN'],'OLDTRIANGN':triangdocs[k]['TRIANGN'],'H11':h11,'TRIANG':triangdocs[k]['TRIANG'],'SRIDEAL':triangdocs[k]['SRIDEAL'],'CHERN2XD':triangdocs[k]['CHERN2XD'],'IPOLYAD':triangdocs[k]['IPOLYAD'],'ITENSAD':triangdocs[k]['ITENSAD'],'IPOLYXD':triangdocs[k]['IPOLYXD'],'ITENSXD':triangdocs[k]['ITENSXD'],'IPOLYAJ':triangdocs[k]['IPOLYAJ'],'ITENSAJ':triangdocs[k]['ITENSAJ'],'CHERNAD':triangdocs[k]['CHERNAD'],'CHERNAJ':triangdocs[k]['CHERNAJ'],'CHERN3XD':triangdocs[k]['CHERN3XD'],'CHERN3XJ':triangdocs[k]['CHERN3XJ'],'MORIMATP':triangdocs[k]['MORIMATP'],'KAHLERMATP':triangdocs[k]['KAHLERMATP']},separators=(',',':'));
            #print "-TRIANGtemp."+json.dumps({'POLYID':polyid,'GEOMN':triangdocs[k]['GEOMN'],'TRIANGN':triangdocs[k]['TRIANGN']},separators=(',',':'))+">"+json.dumps({},separators=(',',':'));
            sys.stdout.flush();
            m+=1;
except Exception as e:
    PrintException();