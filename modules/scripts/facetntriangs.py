#!/shared/apps/sage-7.4/local/bin/sage -python

from sage.all_cmdline import *;

import sys,linecache,traceback,signal,json;
from contextlib import contextmanager;
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;
#import mongolink.tools as tools;

#################################################################################
#Misc. function definitions
class TimeoutException(Exception): pass;

@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!");
    signal.signal(signal.SIGALRM, signal_handler);
    signal.alarm(seconds);
    try:
        yield;
    finally:
        signal.alarm(0);

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
    
'''
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

def FSRT_facet_from_resolved_verts(resverts,facet):
    pc=PointConfiguration(facet);
    pretriangs=pc.restrict_to_regular_triangulations(regular=True).restrict_to_fine_triangulations(fine=True).triangulations_list();
    triangs+=[list([list([resverts.index(facet[z]) for z in y]) for y in x]) for x in pretriangs];
    return triangs;
'''

def n_FSRT_facet_from_resolved_verts(facetpts):
    pc=PointConfiguration(facetpts);
    #triangs=pc.restrict_to_regular_triangulations(regular=True).restrict_to_fine_triangulations(fine=True).triangulations_list();
    pc_fine=pc.restrict_to_fine_triangulations(fine=True);
    pc_fine_reg=pc_fine.restrict_to_regular_triangulations(regular=True);
    triangs_fine=pc_fine.triangulations_list();
    triangs_fine_reg=pc_fine_reg.triangulations_list();
    return [len(triangs_fine),len(triangs_fine_reg)];

#################################################################################
#Main body
try:
    #docsfile=sys.argv[1];
    #basecoll=sys.argv[2];
    #dbindexes=sys.argv[3:];
    #with open(docsfile,'r') as docstream:
    #    for line in docstream:
    for line in iter(sys.stdin.readline,''):
        maxconedoc=json.loads(line.rstrip("\n"));
        nform=mat2py(maxconedoc['NORMALFORM']);
        facetverts=[x for x in nform if not all([y==0 for y in x])];
        facet=LatticePolytope(facetverts);
        facetpts=[list(x) for x in facet.boundary_points()];
        try:
            with time_limit(3600):
                facetntriang_fine,facetntriang_fine_reg=n_FSRT_facet_from_resolved_verts(facetpts);
        except TimeoutException("Timed out!"):
            pass;
        else:
            facetntriang=n_FSRT_facet_from_resolved_verts(facetpts);
            print("+MAXCONE."+json.dumps({'NORMALFORM':py2mat(nform)},separators=(',',':'))+">"+json.dumps({'FACETNTRIANG':facetntriang_fine,'FACETNREGTRIANG':facetntriang_fine_reg},separators=(',',':')));
            print("@");#print("@"+basecoll+"."+json.dumps(dict([(x,maxconedoc[x]) for x in dbindexes]),separators=(',',':')));
            sys.stdout.flush();
except Exception as e:
    PrintException();