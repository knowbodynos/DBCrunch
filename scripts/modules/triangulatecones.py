#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,re,itertools,json;
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;
import mongolink.tools as tools;
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
    line=linecache.getline(filename, lineno, f.f_globals);
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename,lineno,line.strip(),exc_obj);
    print "More info: ",traceback.format_exc();

#Module-specific function definitions
def indep_rows(mat,vect=[]):
    "Returns a matrix containing only the linearly independent rows of mat."
    matker=mat.left_kernel().basis_matrix();
    newmat=mat;
    if vect==[]:
        while matker.rank()>0:
            rem=max([i for i in range(matker.ncols()) if all([x==0 for x in matker.columns()[i]])==False]);
            newmat=newmat.delete_rows([rem]);
            matker=newmat.left_kernel().basis_matrix();
        return newmat;
    elif vect!=[] and len(vect)==mat.nrows():
        newvect=vect;
        while matker.rank()>0:
            rem=max([i for i in range(matker.ncols()) if all([x==0 for x in matker.columns()[i]])==False]);
            newmat=newmat.delete_rows([rem]);
            newvect=newvect[:rem]+newvect[rem+1:];
            matker=newmat.left_kernel().basis_matrix();
        return (newmat,newvect);

def min_nonneg_basis(rows):
    "Returns the smallest pseudo-basis of the given rows such that each element is non-negative."
    poly=Polyhedron(lines=rows);
    pos=Polyhedron(rays=identity_matrix(len(rows[0])).columns());
    pos_int=poly.intersection(pos);
    rays=pos_int.rays();
    rays_vec=[vector(x) for x in rays];
    newmat_redund=matrix(ZZ,sorted(rays_vec,key=lambda x:x.norm()));
    newmat=indep_rows(newmat_redund);
    newmat_rows=[list(x) for x in sorted(newmat.rows(),key=lambda x:operator.itemgetter(*range(len(x)))(x))];
    return newmat_rows;

def LP2CWS(verts):
    "Converts the vertex matrix of a lattice polytope to its weight matrix."
    ker=matrix(verts).left_kernel();
    ker_mat=ker.matrix().rows();
    cws=min_nonneg_basis(ker_mat);
    return tools.transpose_list(cws);

def hodge_CY(lp_pts,dlp_pts,max_cones,max_dcones):
    "Computes the Hodge numbers of a Calabi-Yau threefold corresponding to a reflexive 4-dimensional polytope using Batyrev's method (http://arxiv.org/abs/alg-geom/9310003)."
    #Get fans
    fan=Fan(max_cones).cones()[1:];
    dfan=Fan(max_dcones).cones()[1:];
    #Get number of interior points in fans
    lp_ninter=[[sum([1 for i in range(len(lp_pts)) if y.relative_interior_contains(copy(lp_pts[i]))]) for y in x] for x in fan];
    dlp_ninter=[[sum([1 for i in range(len(dlp_pts)) if y.relative_interior_contains(copy(dlp_pts[i]))]) for y in x] for x in dfan];
    #Get dual cones
    fan_dual=[[Cone(rays=[y for y in dlp_pts if all([vector(x)*vector(y)==-1 for x in icone.rays().column_matrix().columns()])]) for icone in dimicones] for dimicones in fan];
    dfan_dual=[[Cone(rays=[y for y in lp_pts if all([vector(x)*vector(y)==-1 for x in icone.rays().column_matrix().columns()])]) for icone in dimicones] for dimicones in dfan];
    #Get number of interior points in dual fans
    lp_ninter_dual=[[sum([1 for i in range(len(dlp_pts)) if y.relative_interior_contains(copy(dlp_pts[i]))]) for y in x] for x in fan_dual];
    dlp_ninter_dual=[[sum([1 for i in range(len(lp_pts)) if y.relative_interior_contains(copy(lp_pts[i]))]) for y in x] for x in dfan_dual];
    #Get Hodge numbers
    h11=len(dlp_pts)-5-sum(dlp_ninter[-1])+sum([dlp_ninter[-2][i]*dlp_ninter_dual[-2][i] for i in range(len(dlp_ninter[-2]))]);
    h21=len(lp_pts)-5-sum(lp_ninter[-1])+sum([lp_ninter[-2][i]*lp_ninter_dual[-2][i] for i in range(len(lp_ninter[-2]))]);
    return [h11,h21];

def simpvol(verts):
    "Compute the volume of a simplex specified by a set of vertices."
    return abs(matrix(verts).det());

def dcbase(dresverts):
    "Compute the fundamental group and a set of basis divisors."
    fgp=0;
    for i in range(len(dresverts)):
        for j in range(i):
            for k in range(j):
                for l in range(k):
                    sv=simpvol([dresverts[m] for m in [i,j,k,l]]);
                    fgp=gcd(fgp,sv);
                    if sv==1:
                        basisinds=[m for m in range(len(dresverts)) if m not in [i,j,k,l]];
                        return [1,basisinds];
    if fgp>1:
        for i in range(len(dresverts)):
            for j in range(i):
                for k in range(j):
                    for l in range(k):
                        sv=simpvol([dresverts[m] for m in [i,j,k,l]]);
                        if sv==fgp:
                            basisinds=[m for m in range(len(dresverts)) if m not in [i,j,k,l]];
                            return [fgp,basisinds];

def bndry_pts_cones(lp,lp_pts,max_cones):
    "Compute the points that lie on the boundary of the specified cone."
    pts_dup_sep=[[list(lp_pts[i]) for i in range(len(lp_pts)) if (y.contains(lp_pts[i]) and not y.interior_contains(lp_pts[i]) and not lp.interior_contains(lp_pts[i]))] for y in max_cones];
    pts_sep=[[vector(y) for y in tools.deldup(x)]+[vector([0,0,0,0])] for x in pts_dup_sep];
    return pts_sep;

def FSRT_cone_from_resolved_verts(verts):
    "Triangulate the specified cone."
    pc=PointConfiguration(verts);
    triang=pc.restrict_to_regular_triangulations(regular=True).restrict_to_fine_triangulations(fine=True).restrict_to_star_triangulations(vector((0,0,0,0))).triangulations_list();
    return [list(x) for x in triang];

def is_regular(gpts,ctriang1,ctriang2):
    "Check if the union of two triangulated cones is regular."
    pairtriang=ctriang1+ctriang2;
    gcones=[Cone([gpts.column(i) for i in range(gpts.ncols()) if i not in x]) for x in pairtriang];
    ginters=gcones[0];
    for x in gcones[1:]:
        ginters=ginters.intersection(x);
        for y in x.facets():
            if y.intersection(ginters).is_equivalent(ginters):
                return False;
    return True;

def is_aligned(pts,cpts1,cpts2,ctriang1,ctriang2):
    "Check if the triangulations of two cones are coherent on their intersection."
    c1=Cone(cpts1);
    c2=Cone(cpts2);
    cinters=c1.intersection(c2);
    for x in ctriang1:
        s1=Cone([pts[j] for j in x]);
        s2=Cone([pts[j] for j in ctriang2[0]]);
        sinters=cinters.intersection(s1);
        i=0;
        while (i<len(ctriang2)-1) and (sinters.intersection(s2).is_trivial()):
            i+=1;
            s2=Cone([pts[j] for j in ctriang2[i]]);
        if (i==len(ctriang2)):
            return False;
    for y in ctriang2:
        s1=Cone([pts[j] for j in ctriang1[0]]);
        s2=Cone([pts[j] for j in y]);
        sinters=cinters.intersection(s2);
        i=0;
        while (i<len(ctriang1)-1) and (sinters.intersection(s1).is_trivial()):
            i+=1;
            s1=Cone([pts[j] for j in ctriang1[i]]);
        if (i==len(ctriang1)):
            return False;
    return True;

def knit_cones(pts,gpts,conepts,conetriang):
    "Glue triangulated cones together in a coherent manner."
    ncones=len(conepts);
    if ncones==1:
        return conetriang[0];
    else:
        mod=ncones%2;
        newconepts=[];
        newconetriang=[];
        for i in range(0,ncones-mod,2):
            cpts1=conepts[i];
            cpts2=conepts[i+1];
            ctriang1=conetriang[i];
            ctriang2=conetriang[i+1];
            if is_aligned(pts,cpts1,cpts2,ctriang1,ctriang2) and is_regular(gpts,ctriang1,ctriang2):
                newconepts+=[tools.deldup(cpts1+cpts2)];
                newconetriang+=[ctriang1+ctriang2];
            else:
                return [];
        if mod:
            newconepts+=[conepts[-1]];
            newconetriang+=[conetriang[-1]];
        return knit_cones(pts,gpts,newconepts,newconetriang);

def favorable(ndivsJ,h11):
    "Check if the polytope is favorable."
    return (h11==ndivsJ);

#################################################################################
#Main body
if rank==0:
    try:
        #IO Definitions
        polydoc=json.loads(sys.argv[1]);
        #Read in pertinent fields from JSON
        polyid=polydoc['POLYID'];
        nverts=mat2py(polydoc['NVERTS']);
        h11=polydoc['H11'];
        h21=polydoc['H21'];
        #Compute initial information corresponding to polytope
        lp=Polyhedron(vertices=nverts);
        sorted_dverts=[vector(x) for x in sorted([vector([y/gcd(x) for y in x]) for x in lp.polar().vertices()])];
        dlp=Polyhedron(vertices=sorted_dverts);
        dverts=[list(x) for x in sorted_dverts];
        cws=LP2CWS(sorted_dverts);
        max_cones=[Cone([x.vector() for x in list(lp.Hrepresentation(i).incident())]) for i in range(lp.n_Hrepresentation())];
        max_dcones=[Cone([x.vector() for x in list(dlp.Hrepresentation(i).incident())]) for i in range(dlp.n_Hrepresentation())];
        lp_pts=copy(lp.integral_points(threshold=1e10));
        dlp_pts=copy(dlp.integral_points(threshold=1e10));
        h11,h21=hodge_CY(lp_pts,dlp_pts,max_cones,max_dcones);
        batyh11,batyh21=hodge_CY(lp_pts,dlp_pts,max_cones,max_dcones);
        if (batyh11!=h11 or batyh21!=h21):
            raise ValueError('Computed Hodge pair ('+str(batyh11)+','+str(batyh21)+') does not match KS database ('+str(h11)+','+str(h21)+').');
        conepts=bndry_pts_cones(dlp,dlp_pts,max_dcones);
        origin=vector([0,0,0,0]);
        unsorted_dresverts=tools.deldup([x for y in conepts for x in y if x!=origin]);
        extra_dresverts=sorted([x for x in unsorted_dresverts if x not in sorted_dverts]);
        dresverts=[list(x) for x in sorted_dverts+extra_dresverts];
        fgp,basisinds=dcbase(dresverts);
        dresverts_vecs=sorted_dverts+extra_dresverts+[origin];
        originind=len(dresverts_vecs)-1;
        ######################## Begin parallel MPI scatter/gather of triangulation information ###############################
        scatt=[[dresverts_vecs]+x for x in tools.distribcores(conepts,size)];
        #If fewer cores are required than are available, pass extraneous cores no information
        if len(scatt)<size:
            scatt+=[-2 for x in range(len(scatt),size)];
        #Scatter and define rank-independent input variables
        cones=comm.scatter(scatt,root=0);
        #For each chunk of information assigned to this rank, do the following
        gath=[];
        for c in cones[1:]:
            #Triangulate cone c and relabel vertices according to full polytope
            newconetriang0=FSRT_cone_from_resolved_verts(c);
            newconetriang=[[[cones[0].index(c[z]) for z in y] for y in x] for x in newconetriang0];
            #Gather information into a list and pass it back to main rank
            gath+=[newconetriang];
        conetriangs_group=comm.gather(gath,root=0);
        #Signal ranks to exit current process (if there are no other processes, then exit other ranks)
        scatt=[-1 for j in range(size)];
        comm.scatter(scatt,root=0);
        #Reorganize gathered information into a serial form
        conetriangs=[x for y in conetriangs_group for x in y];
        poten_triangs0=list(itertools.product(*conetriangs));
        poten_triangs=[[sorted([sorted(x) for x in y]) for y in z] for z in poten_triangs0];
        #######################################################################################################################
        #Perform Gale transform and knit triangulated cones back together such that they form a fine, star, regular triangulation of the full polytope
        gdresverts=PointConfiguration(dresverts_vecs).Gale_transform();
        unsorted_triangs=[];
        for x in poten_triangs:
            knitted=knit_cones(dresverts_vecs,gdresverts,conepts,x);
            if len(knitted)>0:
                unsorted_triangs+=[sorted([[y for y in x if y!=originind] for x in knitted])];
        #Sort triangulations
        triangs=sorted(tools.deldup(unsorted_triangs));
        #Compute the resolved weight matrix and set the number of basis divisors
        rescws=LP2CWS(dresverts);
        rescws_mat=matrix(rescws);
        rescws_cols=rescws_mat.columns();
        ndivsJ=rescws_mat.rank();
        #Check if the polytope is favorable
        fav=favorable(ndivsJ,h11);
        #Create the pseudo-Chow polynomial ring
        C=PolynomialRing(QQ,names=['t']+['D'+str(i+1) for i in range(len(dresverts))]+['J'+str(i+1) for i in range(ndivsJ)]);
        DD=list(C.gens()[1:-ndivsJ]);
        JJ=list(C.gens()[-ndivsJ:]);
        #Define the matrix the converts toric divisors to basis divisors
        DtoJmat=[[1 if i==j else 0 for i in range(len(DD))] for j in basisinds];
        #Determine the number of triangulations corresponding to the polytope
        nalltriangs=len(triangs);
        #Add new properties to the base tier of the JSON
        print "+POLY1."+json.dumps({'POLYID':polyid},separators=(',',':'))+">"+json.dumps({'DVERTS':py2mat(dverts),'DRESVERTS':py2mat(dresverts),'CWS':py2mat(cws),'RESCWS':py2mat(rescws),'FAV':fav,'DTOJ':py2mat(DtoJmat),'FUNDGP':fgp,'NALLTRIANGS':nalltriangs},separators=(',',':'));
        for i in range(nalltriangs):
            print "+TRIANG1."+json.dumps({'POLYID':polyid,'GEOMN':i+1,'TRIANGN':1},separators=(',',':'))+">"+json.dumps({'POLYID':polyid,'GEOMN':i+1,'TRIANGN':1,'H11':h11,'TRIANG':py2mat(triangs[i])},separators=(',',':'));
        sys.stdout.flush();
    except Exception as e:
        PrintException();
else:
    try:
        #While rank is not signalled to close
        while True:
            scatt=None;
            cones=comm.scatter(scatt,root=0);
            if cones==-1:
                #Rank has been signalled to close
                break;
            elif cones==-2:
                #Rank is extraneous and no information is being passed
                gath=[];
            else:
                gath=[];
                for c in cones[1:]:
                    #Triangulate cone c and relabel vertices according to full polytope
                    newconetriang0=FSRT_cone_from_resolved_verts(c);
                    newconetriang=[[[cones[0].index(c[z]) for z in y] for y in x] for x in newconetriang0];
                    #Gather information into a list and pass it back to main rank
                    gath+=[newconetriang];
                conetriangs_group=comm.gather(gath,root=0);
    except Exception as e:
        PrintException();