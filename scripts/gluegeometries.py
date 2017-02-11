#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,json,toriccy;
from toriccy.parse import pythonlist2mathematicalist as py2mat;
from toriccy.parse import mathematicalist2pythonlist as mat2py;
from mpi4py import MPI;

comm=MPI.COMM_WORLD;
size=comm.Get_size();
rank=comm.Get_rank();

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

def distribcores(lst,size):
    "Distribute information in lst into chunks of size size in order to scatter to various cores."
    L=len(lst);
    mod=L%size;
    split=[];
    j=0;
    for i in range(size):
        increm=((L-mod)/size);
        if i<mod:
            split+=[lst[j:j+increm+1]];
            j+=increm+1;
        else:
            split+=[lst[j:j+increm]];
            j+=increm;
    return split;

#################################################################################
#Module-specific function definitions
def mori(dresverts,triang):
    "Compute the Mori cone of the phase corresponding to triang."
    orig_dresverts=dresverts+[vector([0,0,0,0])];
    aug_dresverts=[vector([1]+list(x)) for x in orig_dresverts];
    conelist=[Cone([aug_dresverts[i] for i in x]) for x in triang];
    cone_combs=[x for x in Set([y for y in conelist if y.dim()==4]).subsets(2).list() if x[0].intersection(x[1]).dim()==3];
    cone_intersects=[x[0].intersection(x[1]).rays().matrix().rows()+[aug_dresverts[-1]] for x in cone_combs];
    cone_unions=[toriccy.deldup(x[0].rays().matrix().rows()+x[1].rays().matrix().rows()+[aug_dresverts[-1]]) for x in cone_combs];
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

def match(itensXD_pair,c2Xnums_pair,eX_pair,mori_rows_pair):
    "Determine if the above pairs of values match."
    inter=Cone([vector(x) for x in mori_rows_pair[0]]).intersection(Cone([-vector(x) for x in mori_rows_pair[1]])).rays().column_matrix().columns();
    flop=True;
    for x in inter:
        range0=[k for k in range(len(x)) if x[k]<0];
        range1=[k for k in range(len(x)) if x[k]>0];
        set0=Set(range0).subsets(min(len(range0),3)).list();
        set1=Set(range1).subsets(min(len(range1),3)).list();
        inums0=[toriccy.nestind(itensXD_pair[0],y) for y in set0];
        inums1=[toriccy.nestind(itensXD_pair[1],y) for y in set1];
        flop=flop and all([y==0 for y in flatten(inums0+inums1)]);
    return (itensXD_pair[0]==itensXD_pair[1]) and (Set(c2Xnums_pair[0])==Set(c2Xnums_pair[1])) and (eX_pair[0]==eX_pair[1]) and flop;

def glue_groups(itensXD_L,c2Xnums_L,eX_L,mori_rows_L):
    "Determine the sets of triangulations that correspond to a unique Calabi-Yau threefold and should be glued according to both Wall's theorem and the flop-tracing method."
    pairs=[inds for inds in Set(range(len(itensXD_L))).subsets(2).list() if match([itensXD_L[i] for i in inds],[c2Xnums_L[i] for i in inds],[eX_L[i] for i in inds],[mori_rows_L[i] for i in inds])];
    groups=[];
    for i in range(len(pairs)):
        glue=pairs[i];
        for j in range(i+1,len(pairs)):
            if len(glue.intersection(pairs[j]))>0:
                glue=glue.union(pairs[j]);
        if not any([glue.issubset(x) for x in groups]):
            groups+=[glue];
    groups_list=[list(x) for x in groups];
    return groups_list+[[i] for i in range(len(itensXD_L)) if not any([i in x for x in groups_list])];

def glue_mori(DtoJmat,mori_rows_group):
    "Compute Mori and Kahler cones for glued geometries."
    g_mori=Cone(mori_rows_group[0]);
    for x in mori_rows_group[1:]:
        g_mori=g_mori.intersection(Cone(x));
    g_mori_rows=[list(x) for x in g_mori.rays().column_matrix().columns()];
    g_mori_cols=toriccy.transpose_list(g_mori_rows);
    g_kahler_cols=[sum([DtoJmat[k][j]*vector(g_mori_cols[j]) for j in range(len(g_mori_cols))]) for k in range(len(DtoJmat))];
    g_kahler_rows=toriccy.transpose_list(g_kahler_cols);
    return [g_mori_rows,g_kahler_rows];

#################################################################################
#Main body
if rank==0:
    try:
        #IO Definitions
        mongouri=sys.argv[1];
        polydoc=json.loads(sys.argv[4]);
        #Read in pertinent fields from JSON
        polyid=polydoc['POLYID'];
        h11=polydoc['H11'];
        dresverts=toriccy.mathematicalist2pythonlist(polydoc['DRESVERTS']);
        rescws=toriccy.mathematicalist2pythonlist(polydoc['RESCWS']);
        DtoJmat=toriccy.mathematicalist2pythonlist(polydoc['DTOJ']);

        mongoclient=toriccy.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
        dbname=mongouri.split("/")[-1];
        db=mongoclient[dbname];
        triangs=toriccy.collectionfind(db,'TRIANG1',{'H11':h11,'POLYID':polyid},{'_id':0,'GEOMN':1,'TRIANG':1},formatresult='expression');
        mongoclient.close();
        #Set the number of basis divisors
        ndivsJ=matrix(rescws).rank();
        #Create the pseudo-Chow polynomial ring
        C=PolynomialRing(QQ,names=['t']+['D'+str(i+1) for i in range(len(dresverts))]+['J'+str(i+1) for i in range(ndivsJ)]);
        DD=list(C.gens()[1:-ndivsJ]);
        JJ=list(C.gens()[-ndivsJ:]);
        #Define the divisor basis
        basis=[[JJ[j],sum([DtoJmat[j][i]*DD[i] for i in range(len(DD))])] for j in range(len(JJ))];
        #Define the linear and basis change parts of the Chow ideal
        Ilin=[sum([dresverts[i][j]*DD[i] for i in range(len(dresverts))]) for j in range(len(dresverts[0]))];
        Ibasechange=[x[0]-x[1] for x in basis];
        Iprechow=C.ideal(Ilin+Ibasechange);
        ######################## Begin parallel MPI scatter/gather of geometrical information ###############################
        scatt=[[C,DD,JJ,dresverts,DtoJmat,Iprechow,x] for x in distribcores(triangs,size)];
        #If fewer cores are required than are available, pass extraneous cores no information
        if len(scatt)<size:
            scatt+=[-2 for x in range(len(scatt),size)];
        #Scatter and define rank-independent input variables
        prechow=comm.scatter(scatt,root=0);
        C_chunk,DD_chunk,JJ_chunk,dresverts_chunk,DtoJmat_chunk,Iprechow_chunk,triangs_chunk=prechow;
        #For each chunk of information assigned to this rank, do the following
        gath=[];
        for t in triangs_chunk:
            #Obtain Mori and Kahler matrices
            mori_rows=mori(dresverts_chunk,t['TRIANG']);
            mori_cols=toriccy.transpose_list(mori_rows);
            kahler_cols=[sum([DtoJmat_chunk[k][j]*vector(mori_cols[j]) for j in range(len(mori_cols))]) for k in range(len(DtoJmat_chunk))];
            kahler_rows=toriccy.transpose_list(kahler_cols);
            #Obtain Stanley-Reisner ideal and Chow ideal
            SR=SR_ideal(dresverts_chunk,t['TRIANG']);
            SRid=[prod([DD_chunk[j] for j in x]) for x in SR];
            ISR=C_chunk.ideal(SRid);
            Ichow=Iprechow_chunk+ISR;
            #Obtain information about the Chow ring
            ipolyAD,itensAD,ipolyXD,itensXD,invbasis,JtoDmat,cnAD,cnAJ=chowAmb(C_chunk,DD_chunk,JJ_chunk,dresverts_chunk,t['TRIANG'],Ichow);
            ipolyAJ,itensAJ,ipolyXJ,itensXJ,c2XD,c3XD,c2XJ,c3XJ,c2Xnums,eX=chowHysurf(C_chunk,DD_chunk,JJ_chunk,DtoJmat_chunk,itensAD,itensXD,cnAD,cnAJ);
            #Gather information into a list and pass it back to main rank
            gath+=[{'SRIDEAL':SRid,'JTOD':JtoDmat,'INVBASIS':invbasis,'IPOLYAD':ipolyAD,'ITENSAD':itensAD,'IPOLYXD':ipolyXD,'ITENSXD':itensXD,'IPOLYAJ':ipolyAJ,'ITENSAJ':itensAJ,'IPOLYXJ':ipolyXJ,'ITENSXJ':itensXJ,'CHERNAD':cnAD,'CHERNAJ':cnAJ,'CHERN2XD':c2XD,'CHERN2XJ':c2XJ,'CHERN3XD':c3XD,'CHERN3XJ':c3XJ,'CHERN2XNUMS':c2Xnums,'EULERX':eX,'MORIMATP':mori_rows,'KAHLERMATP':kahler_rows}];
        postchow_group=comm.gather(gath,root=0);
        #Signal ranks to exit current process (if there are no other processes, then exit other ranks)
        scatt=[-1 for j in range(size)];
        comm.scatter(scatt,root=0);
        #Reorganize gathered information into a serial form
        postchow=[x for y in postchow_group for x in y];
        #######################################################################################################################
        #Create lists of properties to check for consistency when gluing
        itensXD_L=[];
        c2Xnums_L=[];
        eX_L=[];
        mori_rows_L=[];
        for x in postchow:
            itensXD_L+=[x['ITENSXD']];
            c2Xnums_L+=[x['CHERN2XNUMS']];
            eX_L+=[x['EULERX']];
            mori_rows_L+=[x['MORIMATP']];
        #Determine which triangulations to glue together
        to_glue_L=glue_groups(itensXD_L,c2Xnums_L,eX_L,mori_rows_L);
        #Compress properties that should remain the same across the whole polytope into single variables
        JtoDmat=postchow[0]['JTOD'];
        invbasis=postchow[0]['INVBASIS'];
        #Add new properties to base tier of JSON
        print "+POLY1.{\"POLYID\":"+str(polyid)+"}>"+json.dumps({'BASIS':py2mat(basis),'EULER':int(eX_L[0]),'NGEOMS':len(to_glue_L),'JTOD':py2mat(JtoDmat),'INVBASIS':py2mat(invbasis)},separators=(',',':'));
        sys.stdout.flush();
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
            print "+GEOM1.{\"POLYID\":"+str(polyid)+",\"'GEOMN\":"+str(i+1)+"}>"+json.dumps({'POLYID':polyid,'GEOMN':i+1,'H11':h11,'NTRIANGS':len(to_glue_L[i]),'CHERN2XJ':py2mat(postchow[j]['CHERN2XJ']),'CHERN2XNUMS':py2mat(postchow[j]['CHERN2XNUMS']),'IPOLYXJ':py2mat(postchow[j]['IPOLYXJ']),'ITENSXJ':py2mat(postchow[j]['ITENSXJ']),'MORIMAT':py2mat(g_mori_rows),'KAHLERMAT':py2mat(g_kahler_rows)},separators=(',',':'));
            sys.stdout.flush();
            #Add new properties to TRIANGDATA tier of JSON
            m=0;
            for k in to_glue_L[i]:
                print "+TRIANG1.{\"POLYID\":"+str(polyid)+",\"GEOMN\":"+str(triangs[k]['GEOMN'])+",\"TRIANGN\":"+str(1)+"}>"+json.dumps({'GEOMN':i+1,'TRIANGN':m+1,'ALLTRIANGN':triangs[k]['GEOMN'],'SRIDEAL':py2mat(postchow[k]['SRIDEAL']),'CHERN2XD':py2mat(postchow[k]['CHERN2XD']),'IPOLYAD':py2mat(postchow[k]['IPOLYAD']),'ITENSAD':py2mat(postchow[k]['ITENSAD']),'IPOLYXD':py2mat(postchow[k]['IPOLYXD']),'ITENSXD':py2mat(postchow[k]['ITENSXD']),'IPOLYAJ':py2mat(postchow[k]['IPOLYAJ']),'ITENSAJ':py2mat(postchow[k]['ITENSAJ']),'CHERNAD':py2mat(postchow[k]['CHERNAD']),'CHERNAJ':py2mat(postchow[k]['CHERNAJ']),'CHERN3XD':py2mat(postchow[k]['CHERN3XD']),'CHERN3XJ':py2mat(postchow[k]['CHERN3XJ']),'MORIMATP':py2mat(postchow[k]['MORIMATP']),'KAHLERMATP':py2mat(postchow[k]['KAHLERMATP'])},separators=(',',':'));
                sys.stdout.flush();
                m+=1;
    except Exception as e:
        PrintException();
else:
    try:
        #While rank is not signalled to close
        while True:
            scatt=None;
            prechow=comm.scatter(scatt,root=0);
            if prechow==-1:
                #Rank has been signalled to close
                break;
            elif prechow==-2:
                #Rank is extraneous and no information is being passed
                gath=[];
            else:
                C_chunk,DD_chunk,JJ_chunk,dresverts_chunk,DtoJmat_chunk,Iprechow_chunk,triangs_chunk=prechow;
                #For each chunk of information assigned to this rank, do the following
                gath=[];
                for t in triangs_chunk:
                    #Obtain Mori and Kahler matrices
                    mori_rows=mori(dresverts_chunk,t['TRIANG']);
                    mori_cols=toriccy.transpose_list(mori_rows);
                    kahler_cols=[sum([DtoJmat_chunk[k][j]*vector(mori_cols[j]) for j in range(len(mori_cols))]) for k in range(len(DtoJmat_chunk))];
                    kahler_rows=toriccy.transpose_list(kahler_cols);
                    #Obtain Stanley-Reisner ideal and Chow ideal
                    SR=SR_ideal(dresverts_chunk,t['TRIANG']);
                    SRid=[prod([DD_chunk[j] for j in x]) for x in SR];
                    ISR=C_chunk.ideal(SRid);
                    Ichow=Iprechow_chunk+ISR;
                    #Obtain information about the Chow ring
                    ipolyAD,itensAD,ipolyXD,itensXD,invbasis,JtoDmat,cnAD,cnAJ=chowAmb(C_chunk,DD_chunk,JJ_chunk,dresverts_chunk,t['TRIANG'],Ichow);
                    ipolyAJ,itensAJ,ipolyXJ,itensXJ,c2XD,c3XD,c2XJ,c3XJ,c2Xnums,eX=chowHysurf(C_chunk,DD_chunk,JJ_chunk,DtoJmat_chunk,itensAD,itensXD,cnAD,cnAJ);
                    #Gather information into a list and pass it back to main rank
                    gath+=[{'SRIDEAL':SRid,'JTOD':JtoDmat,'INVBASIS':invbasis,'IPOLYAD':ipolyAD,'ITENSAD':itensAD,'IPOLYXD':ipolyXD,'ITENSXD':itensXD,'IPOLYAJ':ipolyAJ,'ITENSAJ':itensAJ,'IPOLYXJ':ipolyXJ,'ITENSXJ':itensXJ,'CHERNAD':cnAD,'CHERNAJ':cnAJ,'CHERN2XD':c2XD,'CHERN2XJ':c2XJ,'CHERN3XD':c3XD,'CHERN3XJ':c3XJ,'CHERN2XNUMS':c2Xnums,'EULERX':eX,'MORIMATP':mori_rows,'KAHLERMATP':kahler_rows}];       
                postchow_group=comm.gather(gath,root=0);
    except Exception as e:
        PrintException();