#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,subprocess,signal,json;
import mongolink.tools as tools;
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
def mori(dresverts,triang):
    "Compute the Mori cone of the phase corresponding to triang."
    orig_dresverts=dresverts+[vector([0,0,0,0])];
    aug_dresverts=[vector([1]+list(x)) for x in orig_dresverts];
    conelist=[Cone([aug_dresverts[i] for i in x]) for x in triang];
    cone_combs=[x for x in Set([y for y in conelist if y.dim()==4]).subsets(2).list() if x[0].intersection(x[1]).dim()==3];
    cone_intersects=[x[0].intersection(x[1]).rays().matrix().rows()+[aug_dresverts[-1]] for x in cone_combs];
    cone_unions=[tools.deldup(x[0].rays().matrix().rows()+x[1].rays().matrix().rows()+[aug_dresverts[-1]]) for x in cone_combs];
    compl_pos=[[i for i in range(len(cone_unions[j])) if cone_unions[j][i] not in cone_intersects[j]] for j in range(len(cone_unions))];
    kers=[matrix(x).left_kernel().matrix().rows() for x in cone_unions];
    sel_kers=[[x if all([x[i]>0 for i in compl_pos[j]]) else -x if all([x[i]<0 for i in compl_pos[j]]) else None for x in kers[j]] for j in range(len(kers))];
    redund_mori=[[vector([x[cone_unions[i].index(y)] if y in cone_unions[i] else 0 for y in aug_dresverts]) for x in sel_kers[i] if x!=None] for i in range(len(sel_kers))];
    pre_mori=Cone([x for y in redund_mori for x in y]).rays().matrix();
    mori=pre_mori.delete_columns([pre_mori.ncols()-1]);
    mori_rows=[list(x) for x in mori.rows()];
    return mori_rows;

def SR_ideal(DD,triang):
    "Compute the Stanley-Reisner ideal corresponding to the polytope given by the vertices vert. Implements the fastest algorithm, while still sorting the ideal to give it a unique, searchable form."
    V=Set(range(len(DD)));
    v_subsets=V.subsets();
    triang_sets=[Set(y) for y in triang];
    nonfaces=[x for x in v_subsets if not any([x.issubset(y) for y in triang_sets])];
    srtd_min_nonfaces=sorted([sorted(nonfaces[i]) for i in range(len(nonfaces)) if not any([nonfaces[j].issubset(nonfaces[i]) for j in range(i)])],key=lambda x:(len(x),)+operator.itemgetter(*range(len(x)))(x) if len(x)>1 else (1,x[0]));
    return srtd_min_nonfaces;

def simpvol(verts):
    "Compute the volume of a simplex specified by a set of vertices."
    return abs(matrix(verts).det());

def chowAmb(C,DD,JJ,dresverts,triang,Ichow):
    "Compute the intersection numbers and Chern classes on the ambient space."
    ndivsD=len(DD);
    triangind=0;
    norm=0;
    while (triangind<len(triang)) and (norm==0):
        vol=simpvol([dresverts[i] for i in triang[triangind]]);
        norm=vol*prod([DD[i] for i in triang[triangind]]).reduce(Ichow);
        triangind+=1;
    imonomsAD=(sum(DD)**4).monomials();
    inumsAD=[x.reduce(Ichow)/norm for x in imonomsAD];
    ipolyAD=sum([inumsAD[i]*imonomsAD[i] for i in range(len(imonomsAD))]);
    imonomADexps=[x.exponents()[0][1:ndivsD+1] for x in imonomsAD];
    imonomADinds=[[i for i in range(len(x)) for j in range(x[i])] for x in imonomADexps];
    itensAD=[[[[0 for l in range(ndivsD)] for k in range(ndivsD)] for j in range(ndivsD)] for i in range(ndivsD)];
    for i in range(ndivsD):
        for j in range(i+1):
            for k in range(j+1):
                for l in range(k+1):
                    if [l,k,j,i] in imonomADinds:
                        internumD=inumsAD[imonomADinds.index([l,k,j,i])];
                        itensAD[i][j][k][l]=internumD;
                        itensAD[i][k][j][l]=internumD;
                        itensAD[j][i][k][l]=internumD;
                        itensAD[j][k][i][l]=internumD;
                        itensAD[k][i][j][l]=internumD;
                        itensAD[k][j][i][l]=internumD;
                        itensAD[i][j][l][k]=internumD;
                        itensAD[i][k][l][j]=internumD;
                        itensAD[j][i][l][k]=internumD;
                        itensAD[j][k][l][i]=internumD;
                        itensAD[k][i][l][j]=internumD;
                        itensAD[k][j][l][i]=internumD;
                        itensAD[i][l][j][k]=internumD;
                        itensAD[i][l][k][j]=internumD;
                        itensAD[j][l][i][k]=internumD;
                        itensAD[j][l][k][i]=internumD;
                        itensAD[k][l][i][j]=internumD;
                        itensAD[k][l][j][i]=internumD;
                        itensAD[l][i][j][k]=internumD;
                        itensAD[l][i][k][j]=internumD;
                        itensAD[l][j][i][k]=internumD;
                        itensAD[l][j][k][i]=internumD;
                        itensAD[l][k][i][j]=internumD;
                        itensAD[l][k][j][i]=internumD;
    itensXD=[[[sum([itensAD[i][j][k][l] for i in range(ndivsD)]) for j in range(ndivsD)] for k in range(ndivsD)] for l in range(ndivsD)];
    imonomsXD=(sum(DD)**3).monomials();
    imonomsXDexps=[x.exponents()[0][1:ndivsD+1] for x in imonomsXD];
    imonomsXDinds=[[i for i in range(len(x)) for j in range(x[i])] for x in imonomsXDexps];
    inumsXD=[itensXD[x[0]][x[1]][x[2]] for x in imonomsXDinds];
    ipolyXD=sum([inumsXD[i]*imonomsXD[i] for i in range(len(imonomsXD))]);
    hysurf=sum(DD);
    hyideal=Ichow.quotient(C.ideal(hysurf));
    Dinbasis=[x.reduce(hyideal) for x in DD];
    JtoDmat=[[y.coefficient(x) for x in JJ] for y in Dinbasis];
    invbasis=[[Dinbasis[i],DD[i]] for i in range(ndivsD)];
    cAD=prod([(1+C.gen(0)*d) for d in DD]);
    cnAD=[cAD.coefficient({C.gen(0):i}) for i in range(cAD.degree(C.gen(0))+1)];
    cAJ=cAD.reduce(Ichow);
    cnAJ=[cAJ.coefficient({C.gen(0):i}) for i in range(cAJ.degree(C.gen(0))+1)];
    return [ipolyAD,itensAD,ipolyXD,itensXD,invbasis,JtoDmat,cnAD,cnAJ];

def chowHysurf(C,DD,JJ,DtoJmat,itensAD,itensXD,cnAD,cnAJ):
    "Compute the intersection numbers and Chern classes on the Calabi-Yau hypersurface."
    ndivsD=len(DD);
    ndivsJ=len(JJ);
    itensAJ=[[[[sum([DtoJmat[l][s]*sum([DtoJmat[k][r]*sum([DtoJmat[j][q]*sum([DtoJmat[i][p]*itensAD[p][q][r][s] for p in range(ndivsD)]) for q in range(ndivsD)]) for r in range(ndivsD)]) for s in range(ndivsD)]) for i in range(ndivsJ)] for j in range(ndivsJ)] for k in range(ndivsJ)] for l in range(ndivsJ)];
    imonomsAJ=(sum(JJ)**4).monomials();
    imonomsAJexps=[list(x.exponents()[0][-ndivsJ:]) for x in imonomsAJ];
    imonomsAJinds=[[i for i in range(len(x)) for j in range(x[i])] for x in imonomsAJexps];
    inumsAJ=[itensAJ[x[0]][x[1]][x[2]][x[3]] for x in imonomsAJinds];
    ipolyAJ=sum([inumsAJ[i]*imonomsAJ[i] for i in range(len(imonomsAJ))]);
    itensXJ=[[[sum([DtoJmat[k][r]*sum([DtoJmat[j][q]*sum([DtoJmat[i][p]*itensXD[p][q][r] for p in range(ndivsD)]) for q in range(ndivsD)]) for r in range(ndivsD)]) for i in range(ndivsJ)] for j in range(ndivsJ)] for k in range(ndivsJ)];
    imonomsXJ=(sum(JJ)**3).monomials();
    imonomsXJexps=[list(x.exponents()[0][-ndivsJ:]) for x in imonomsXJ];
    imonomsXJinds=[[i for i in range(len(x)) for j in range(x[i])] for x in imonomsXJexps];
    inumsXJ=[itensXJ[x[0]][x[1]][x[2]] for x in imonomsXJinds];
    ipolyXJ=sum([inumsXJ[i]*imonomsXJ[i] for i in range(len(imonomsXJ))]);
    c2XD=cnAD[2];
    c3XD=cnAD[3]-cnAD[2]*cnAD[1];
    cnXD=[1,0,c2XD,c3XD];
    c2XJ=cnAJ[2];
    c3XJ=cnAJ[3]-cnAJ[2]*cnAJ[1];
    cnXJ=[1,0,c2XJ,c3XJ];
    str_c3XJ=str(c3XJ);
    str_c2Xnums=[str(c2XJ*j) for j in JJ];
    for i in range(len(imonomsXJ)):
        str_c2Xnums=[x.replace(str(imonomsXJ[i]),str(inumsXJ[i])) for x in str_c2Xnums];
        str_c3XJ=str_c3XJ.replace(str(imonomsXJ[i]),str(inumsXJ[i]));
    c2Xnums=[sage_eval(x) for x in str_c2Xnums];
    eX=sage_eval(str_c3XJ);
    return [ipolyAJ,itensAJ,ipolyXJ,itensXJ,c2XD,c3XD,c2XJ,c3XJ,c2Xnums,eX];

#################################################################################
#Main body
try:
    #IO Definitions
    polytriangdoc=json.loads(sys.argv[1]);
    #Read in pertinent fields from JSON
    polyid=polytriangdoc['POLYID'];
    alltriangn=allpolytriangdoc['ALLTRIANGN'];
    h11=polytriangdoc['H11'];
    dresverts=mat2py(polytriangdoc['DRESVERTS']);
    rescws=mat2py(polytriangdoc['RESCWS']);
    DtoJmat=mat2py(polytriangdoc['DTOJ']);
    triang=mat2py(polytriangdoc['TRIANG']);
    #Set the number of basis divisors
    ndivsJ=matrix(rescws).rank();
    #Create the pseudo-Chow polynomial ring
    C=PolynomialRing(QQ,names=['t']+['D'+str(i+1) for i in range(len(dresverts))]+['J'+str(i+1) for i in range(ndivsJ)]);
    DD=list(C.gens()[1:-ndivsJ]);
    JJ=list(C.gens()[-ndivsJ:]);
    #Define the divisor basis
    basis=[[JJ[j],sum([DtoJmat[j][i]*DD[i] for i in range(len(DD))])] for j in range(len(JJ))];
    #Define the linear and basis change parts of the Chow ideal
    linideal=[sum([dresverts[i][j]*DD[i] for i in range(len(dresverts))]) for j in range(len(dresverts[0]))];
    basechangeideal=[x[0]-x[1] for x in basis];
    #Obtain Mori and Kahler matrices
    mori_rows=mori(dresverts,triang);
    mori_cols=tools.transpose_list(mori_rows);
    kahler_cols=[sum([DtoJmat[k][j]*vector(mori_cols[j]) for j in range(len(mori_cols))]) for k in range(len(DtoJmat))];
    kahler_rows=tools.transpose_list(kahler_cols);
    #Obtain Stanley-Reisner ideal and Chow ideal
    SR=SR_ideal(dresverts,triang);
    SRideal=[prod([DD[j] for j in x]) for x in SR];
    Ichow=C.ideal(linideal+basechangeideal+SRideal)
    #Obtain information about the Chow ring
    ipolyAD,itensAD,ipolyXD,itensXD,invbasis,JtoDmat,cnAD,cnAJ=chowAmb(C,DD,JJ,dresverts,triang,Ichow);
    ipolyAJ,itensAJ,ipolyXJ,itensXJ,c2XD,c3XD,c2XJ,c3XJ,c2Xnums,eX=chowHysurf(C,DD,JJ,DtoJmat,itensAD,itensXD,cnAD,cnAJ);
    #Add new properties to base tier of JSON
    print "+TRIANGtemp."+json.dumps({'POLYID':polyid,'ALLTRIANGN':alltriangn},separators=(',',':'))+">"+json.dumps({'BASIS':py2mat(basis),'EULER':int(eX),'JTOD':py2mat(JtoDmat),'INVBASIS':py2mat(invbasis),'CHERN2XJ':py2mat(c2XJ),'CHERN2XNUMS':py2mat(c2Xnums),'IPOLYXJ':py2mat(ipolyXJ),'ITENSXJ':py2mat(itensXJ),'SRIDEAL':py2mat(SRideal),'CHERN2XD':py2mat(c2XD),'IPOLYAD':py2mat(ipolyAD),'ITENSAD':py2mat(itensAD),'IPOLYXD':py2mat(ipolyXD),'ITENSXD':py2mat(itensXD),'IPOLYAJ':py2mat(ipolyAJ),'ITENSAJ':py2mat(itensAJ),'CHERNAD':py2mat(cnAD),'CHERNAJ':py2mat(cnAJ),'CHERN3XD':py2mat(c3XD),'CHERN3XJ':py2mat(c3XJ),'MORIMATP':py2mat(mori_rows),'KAHLERMATP':py2mat(kahler_rows)},separators=(',',':'));
    sys.stdout.flush();
except Exception as e:
    PrintException();