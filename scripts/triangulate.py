#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,re,itertools,json;
import toriccy.parse as parse;
import toriccy.tools as tools;

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

#################################################################################
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
    return cws;

def hodge_CY(lp_pts,dlp_pts,face_cones,face_cones_d):
    "Computes the Hodge numbers of a Calabi-Yau threefold corresponding to a reflexive 4-dimensional polytope using Batyrev's method (http://arxiv.org/abs/alg-geom/9310003)."
    #Get fans
    fan=Fan(face_cones).cones()[1:];
    dfan=Fan(face_cones_d).cones()[1:];
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
                        return [int(1),basisinds];
    if fgp>1:
        for i in range(len(dresverts)):
            for j in range(i):
                for k in range(j):
                    for l in range(k):
                        sv=simpvol([dresverts[m] for m in [i,j,k,l]]);
                        if sv==fgp:
                            basisinds=[m for m in range(len(dresverts)) if m not in [i,j,k,l]];
                            return [int(fgp),basisinds];

def favorable(ndivsJ,h11):
    "Check if the polytope is favorable."
    return (h11==ndivsJ);

#################################################################################
#Fine, star, regular triangulation (FSRT)-specific function definitions

def bndry_pts(lp,lp_pts=[],max_cones=[]):
    #Input: A Polyhedron object describing a reflexive lattice polytope.
    #Output: Vertices of the input polytope after resolving up to terminal singularities on the associated toric variety (i.e. all points on the convex hull not in the interior of the facets).
    #Options: 1) A list of all points (lp_pts) on the input lattice polytope can be provided to speed up computation time, if already known.
    #         2) A list of the maximal cones (max_cones) can be provided to speed up computation time, if already known.
    
    if len(lp_pts)==0:
        lp_pts=copy(lp.integral_points(threshold=1e10));
    if len(max_cones)==0:
        max_cones=[Cone([x.vector() for x in list(lp.Hrepresentation(i).incident())]) for i in range(lp.n_Hrepresentation())];
    pts_dup_sep=[[list(lp_pts[i]) for i in range(len(lp_pts)) if (y.contains(lp_pts[i]) and not y.interior_contains(lp_pts[i]) and not lp.interior_contains(lp_pts[i]))] for y in max_cones];
    pts_dup=[x for y in pts_dup_sep for x in y];
    pts=[vector(x) for x in tools.deldup(pts_dup)];
    return pts;

def FSRT_from_resolved_verts(resverts,max_cones=[],all=False):
    #Input: Vertices of a reflexive polytope after resolving up to terminal singularities on the associated toric variety (i.e. all points on the convex hull not in the interior of the facets).
    #Output: Lists of indices representing the combinations of input vertices, which form the intersection of the triangulated simplexes and the convex hull.
    #Options: 1) A list of the maximal cones (max_cones) can be provided to speed up computation time, if already known.
    #         2) The boolean option all specifies whether to compute one FSRT (fine, star, regular triangulation) or all of them.
    
    if len(max_cones)==0:
        lp=Polyhedron(vertices=[vector(x) for x in resverts]);
        max_cones=[Cone([x.vector() for x in list(lp.Hrepresentation(i).incident())]) for i in range(lp.n_Hrepresentation())];
    facets=[Set([resverts.index(y) for y in resverts if x.contains(vector(y))]) for x in max_cones];
    pc=PointConfiguration(resverts);
    if all:
        triangpc=pc.restrict_to_regular_triangulations(regular=True).restrict_to_fine_triangulations(fine=True).triangulations_list();
        pretriang=[[list(y) for y in list(x)] for x in triangpc];
        triang=[[y for x in w for y in Combinations(x,len(resverts[0])).list() if any([Set(y).issubset(z) for z in facets])] for w in pretriang];
        return sorted(tools.deldup([sorted(x) for x in triang]));
    else:
        triangpc=pc.restrict_to_regular_triangulations(regular=True).restrict_to_fine_triangulations(fine=True).triangulate();
        pretriang=[list(y) for y in list(triangpc)];
        triang=[y for x in pretriang for y in Combinations(x,len(resverts[0])).list() if any([Set(y).issubset(z) for z in facets])];
        return sorted(triang);
    
def FSRT_from_verts(verts,all=False):
    #Input: Vertices of a reflexive polytope.
    #Output: 1) Vertices of input polytope after resolving up to terminal singularities on the associated toric variety (i.e. all points on the convex hull not in the interior of the facets).
    #        2) Lists of indices representing the combinations of resolved vertices, which form the intersection of the triangulated simplexes and the convex hull.
    #Options: The boolean option all specifies whether to compute one FSRT (fine, star, regular triangulation) or all of them.
    
    verts=[vector(x) for x in verts];
    lp=Polyhedron(vertices=verts);
    max_cones=[Cone([x.vector() for x in list(lp.Hrepresentation(i).incident())]) for i in range(lp.n_Hrepresentation())];
    unsorted_resverts=bndry_pts(lp,max_cones=max_cones);
    extra_resverts=sorted([x for x in unsorted_resverts if x not in verts]);
    resverts=[list(x) for x in verts+extra_resverts];
    triang=FSRT_from_resolved_verts(resverts,max_cones=max_cones,all=all);
    return [resverts,triang];
    
def FSRT_from_polar_verts(verts,all=False):
    #Input: Vertices of a reflexive polytope.
    #Output: 1) Vertices of the polar dual to the input polytope after resolving up to terminal singularities on the associated toric variety (i.e. all points on the convex hull not in the interior of the facets).
    #        2) Lists of indices representing the combinations of resolved vertices, which form the intersection of the triangulated simplexes and the convex hull.
    #Options: The boolean option all specifies whether to compute one FSRT (fine, star, regular triangulation) or all of them.

    lp=Polyhedron(vertices=verts);
    sorted_dverts=sorted([vector([y/gcd(x) for y in x]) for x in lp.polar().vertices()]);
    return FSRT_from_verts(sorted_dverts,all=all);

#Main body
try:
    #IO Definitions
    polydoc=json.loads(sys.argv[1]);
    #Read in pertinent fields from JSON
    nverts=polydoc['NVERTS'];
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
    if (h11!=polydoc['H11'] or h21!=polydoc['H21']):
        raise ValueError('Computed Hodge pair ('+str(h11)+','+str(h21)+') does not match KS database ('+str(polydoc['H11'])+','+str(polydoc['H21'])+').');
    unsorted_dresverts=bndry_pts(dlp,max_cones=max_dcones);
    extra_dresverts=sorted([x for x in unsorted_dresverts if x not in sorted_dverts]);
    dresverts=[list(x) for x in sorted_dverts+extra_dresverts];
    triangs=FSRT_from_resolved_verts(dresverts,max_cones=max_dcones,all=all);
    fgp,basisinds=dcbase(dresverts);
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
    print "+POLY>"+parse.pythondictionary2mathematicarules({'DVERTS':dverts,'DRESVERTS':dresverts,'CWS':cws,'RESCWS':rescws,'FAV':fav,'DTOJ':DtoJmat,'FUNDGP':fgp,'NALLTRIANGS':nalltriangs});
    for i in range(nalltriangs)
        print "+TRIANG>"+parse.pythondictionary2mathematicarules({'POLYID':polydoc['POLYID'],'ALLTRIANGN':i+1,'TRIANG':triangs[i]});
    sys.stdout.flush();
except Exception as e:
    PrintException();