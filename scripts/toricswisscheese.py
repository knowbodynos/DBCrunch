#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,json;
from mpi4py import MPI;

comm=MPI.COMM_WORLD;
size=comm.Get_size();
rank=comm.Get_rank();

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

def py2mat(lst):
    "Converts a Python list to a string depicting a list in Mathematica format."
    return str(lst).replace(" ","").replace("[","{").replace("]","}");

def mat2py(lst):
    "Converts a string depicting a list in Mathematica format to a Python list."
    return eval(str(lst).replace(" ","").replace("{","[").replace("}","]"));

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

def deldup(lst):
    "Delete duplicate elements in lst."
    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]];

def transpose_list(lst):
    "Get the transpose of a list of lists."
    return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))];

#Module-specific function definitions
def simpvol(verts):
    "Compute the volume of a simplex specified by a set of vertices."
    return abs(matrix(verts).det());

def groupabsetpairs(origsetpairs,NL,h11,ab=0):
    "Computes the full list of sets of possible a or b bases with NL large cycles that already satisfy the small cycle condition."
    basesetpairs=[x for x in origsetpairs if len(x[ab])==1];
    unionsetpairs=list(basesetpairs);
    for x in basesetpairs:
        for y in origsetpairs:
            if x[1-ab]==y[1-ab]:
                z=[None,None];
                z[ab]=x[ab].union(y[ab]);
                z[1-ab]=x[1-ab];
                absetsize=ab*h11+(1-2*ab)*NL;
                if (z not in unionsetpairs) and (len(z[ab])<=absetsize):
                    unionsetpairs+=[z];
    if unionsetpairs==origsetpairs:
        if ab==1:
            reducedsetpairs=[[list(x[0]),list(x[1])] for x in unionsetpairs if (len(x[0])==NL and len(x[1])==h11-NL)];
            return reducedsetpairs;
        else:
            return groupabsetpairs(unionsetpairs,NL,h11,1);
    else:
        return groupabsetpairs(unionsetpairs,NL,h11,ab);

def ToricSwissCheese(homogeneity_on,h11,NL,dresverts,fgp,fav,JtoDmat,mori_rows,itensXD):
    "Solves for the rotation matrices of a Toric Swiss Cheese solution."
    if fav:
        mori_cols=transpose_list(mori_rows);
        ndivsD=len(JtoDmat);
        RaLbssetpairs=[];
        #Small Cycle
        for i in range(ndivsD):
            for j in range(ndivsD):
                if (all([x==0 for x in itensXD[i][j]])):
                    RaLbssetpairs+=[[{i},{j}]];
        #Take (NL,h11-NL) subsets that satisfy the small cycle condition
        RaLbsgroups=groupabsetpairs(RaLbssetpairs,NL,h11);
        #Check for orthogonality (required for a basis) and basis change conditions
        Rabgroups=[];
        Tab=[];
        rind=0;
        ind=0;
        int_basis_a=False;
        int_basis_b=False;
        doneflag=False;
        for group in RaLbsgroups:
            aspossset=Set([z for z in range(ndivsD) if z not in group[0]]).subsets(h11-NL).list();
            bLpossset=Set([z for z in range(ndivsD) if z not in group[1]]).subsets(NL).list();
            for asposs in aspossset:
                afullrank=(matrix(ZZ,[JtoDmat[j] for j in group[0]+list(asposs)]).rank()==h11);
                if afullrank:
                    for bLposs in bLpossset:
                        bfullrank=(matrix(ZZ,[JtoDmat[j] for j in list(bLposs)+group[1]]).rank()==h11);
                        if bfullrank:
                            r=[[group[0],list(asposs)],[list(bLposs),group[1]]];
                            ra=r[0][0]+r[0][1];
                            #Volume
                            volflag=False;
                            lkconeflag=False;
                            skconeflag=False;
                            lcflag=False;
                            homflag=False;
                            i=0;
                            while (i<len(r[0][0]) and not volflag):
                                j=0;
                                while (j<len(ra) and not volflag):
                                    k=0;
                                    while (k<len(ra) and not volflag):
                                        if (itensXD[r[0][0][i]][ra[j]][ra[k]]!=0):
                                            volflag=True;
                                        k+=1;
                                    j+=1;
                                i+=1;
                            if volflag:
                                #Kahler Cone (Large part)
                                lkconeflag=all([(all([y>=0 for y in mori_cols[z]]) or all([y<=0 for y in mori_cols[z]])) for z in r[0][0]]);
                                if lkconeflag:
                                    #Kahler Cone (Small part)
                                    #skconeflag=False;
                                    #all_solcones=[Cone([[mori_rows[i][r[0][1][j]] for j in range(len(r[0][1]))]]).dual() for i in range(len(mori_rows)) if all([mori_rows[i][k]==0 for k in r[0][0]])];
                                    #solcone=Cone([[0 for j in range(len(r[0][1]))]]).dual();
                                    #for newsolcone in all_solcones:
                                    #    solcone=solcone.intersection(newsolcone);
                                    #if solcone.dim()==len(r[0][1]):
                                    #    skconeflag=True;
                                    #else:
                                    #    in_interior=(not any([solcone.intersection(z).is_equivalent(solcone) for y in all_solcones for z in y.facets()]));
                                    #    if in_interior:
                                    #        skconeflag=True;
                                    skconeflag=Cone([[mori_rows[i][r[0][1][j]] for j in range(len(r[0][1]))] for i in range(len(mori_rows)) if all([mori_rows[i][k]==0 for k in r[0][0]])]).is_strictly_convex();
                                    if skconeflag:
                                        #Large Cycle
                                        lcflag=False;
                                        for j in r[0][0]:
                                            for k in ra:
                                                if (not any([itensXD[i][j][k]==0 for i in r[1][0]])):
                                                    lcflag=True;
                                                if lcflag:
                                                    break;
                                            if lcflag:
                                                break;
                                        if lcflag:
                                            #Homogeneity
                                            if homogeneity_on:
                                                homflag=False;
                                                for j in r[0][1]:
                                                    if(not any([itensXD[i][i][j]==0 for i in r[1][1]])):
                                                        homflag=True;
                                                    if homflag:
                                                        break;
                                            else:
                                                homflag=True;
                            if (volflag and lkconeflag and skconeflag and lcflag and homflag):
                                #Convert to rotation matrices
                                Ta=[JtoDmat[j] for j in r[0][0]+r[0][1]];
                                Tb=[JtoDmat[j] for j in r[1][0]+r[1][1]];
                                Rabgroups+=[r];
                                Tab+=[[Ta,Tb]];
                                #Check for integer bases
                                a_integer=simpvol([dresverts[m] for m in range(ndivsD) if m not in r[0][0]+r[0][1]])==fgp;
                                b_integer=simpvol([dresverts[m] for m in range(ndivsD) if m not in r[1][0]+r[1][1]])==fgp;
                                if a_integer and b_integer:
                                    int_basis_a=True;
                                    int_basis_b=True;
                                    ind=rind;
                                    doneflag=True;
                                    break;
                                elif a_integer and (not b_integer) and not (int_basis_a or int_basis_b):
                                    int_basis_a=True;
                                    int_basis_b=False;
                                    ind=rind;
                                elif (not a_integer) and b_integer and not (int_basis_a or int_basis_b):
                                    int_basis_a=False;
                                    int_basis_b=True;
                                    ind=rind;
                                rind+=1;
                if doneflag:
                    break;
            if doneflag:
                break;                        
        if len(Tab)==0:
            return [[],int_basis_a,int_basis_b];
        else:
            return [Tab[ind],int_basis_a,int_basis_b];
    else:
        return ["unfav",0,0];

#Main body
if rank==0:
    try:
        time0=time.time();
        #IO Definitions
        infile=sys.argv[1];
        inpos=eval(sys.argv[2]);
        outfile=sys.argv[3];
        outbatchn=sys.argv[4];
        #Check if input/output files exist
        if not os.path.exists(infile):
            raise ValueError('Argument infile ('+infile+') must be an existing file.');
        if not os.path.exists(outfile):
            raise ValueError('Argument outfile ('+outfile+') must be an existing file.');
        #Lock input file, read JSON from input position, and unlock
        with open(infile,"r") as instream:
            fcntl.flock(instream,fcntl.LOCK_EX);
            instream.seek(inpos);
            tempdict=json.loads(instream.readline().replace("\n",""));
            fcntl.flock(instream,fcntl.LOCK_UN);
        #Read in pertinent fields from JSON
        h11=tempdict['H11'];
        dresverts=mat2py(tempdict['DRESVERTS']);
        fgp=tempdict['FUNDGP'];
        fav=tempdict['FAV'];
        JtoDmat=mat2py(tempdict['JTOD']);
        geomdata=tempdict['GEOMDATA'];
        geomdata_distrib=[[geomn,mat2py(triangdata['MORIMATP']),mat2py(triangdata['ITENSXD'])] for geomn in range(len(geomdata)) for triangdata in geomdata[geomn]['TRIANGDATA']];
        ######################## Begin parallel MPI scatter/gather of toric swiss cheese information ###############################
        scatt=[[h11,dresverts,fgp,fav,JtoDmat,x] for x in distribcores(geomdata_distrib,size)];
        #If fewer cores are required than are available, pass extraneous cores no information
        if len(scatt)<size:
            scatt+=[-2 for x in range(len(scatt),size)];
        #Scatter and define rank-independent input variables
        pretsc=comm.scatter(scatt,root=0);
        h11_chunk,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,tscin_chunk=pretsc;
        #Reorganize chunks of information fed to this rank, so that it can be read in series of whole geometries
        tscin_chunkinds=sorted(deldup(transpose_list(tscin_chunk)[0]));
        tscin_chunk_byinds=[[i,[x[1:] for x in tscin_chunk if x[0]==i]] for i in tscin_chunkinds];
        #Loop for each chunk of information corresponding to a single geometry
        gath=[];
        for t in tscin_chunk_byinds:
            #Loop for each number of large cycles from 1 to h11-1
            tscNL_L_chunk=[];
            for NL in range(1,h11_chunk):
                #Loop for each triangulation for the current number of large cycles
                tsc_L_chunk=[];
                scflag=0;
                for x in t[1]:
                    #Get swiss cheese rotation matrices for this triangulation
                    tsc_chunk=ToricSwissCheese(True,h11_chunk,NL,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,*x);
                    #If both rotation matrices rotate the basis into another integer basis, use them and skip to the next triangulation. Else add them to a list 
                    if (tsc_chunk[1] and tsc_chunk[2]):
                        tscNL_L_chunk+=[tsc_chunk];
                        scflag=1;
                        break;
                    tsc_L_chunk+=[tsc_chunk];
                #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one rotates into an integer basis, then use them and skip to the next NL
                if not scflag:
                    for y in tsc_L_chunk:
                        if y[1] or y[2]:
                            tscNL_L_chunk+=[y];
                            scflag=1;
                            break;
                #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one exists and is favorable, then use them and skip to the next NL
                if not scflag:
                    for y in tsc_L_chunk:
                        if y[0]!=[] and y[0]!="unfav":
                            tscNL_L_chunk+=[y];
                            scflag=1;
                            break;
                #If we still have not already chosen a swiss cheese solution for this NL, then just use the first pair of rotation matrices (even if they are empty or unfavorable) and skip to the next NL
                if not scflag:
                    tscNL_L_chunk+=[tsc_L_chunk[0]];
            #Gather information into a list for each geometry and pass it back to main rank
            gath+=[[t[0],tscNL_L_chunk]];
        posttsc_group=comm.gather(gath,root=0);
        #Signal ranks to exit current process (if there are no other processes, then exit other ranks)
        scatt=[-1 for j in range(size)];
        pretsc=comm.scatter(scatt,root=0);
        #Reorganize gathered information into a serial form
        posttsc_redist=[x for y in posttsc_group for x in y];
        posttsc=[transpose_list([x[1] for x in posttsc_redist if x[0]==i]) for i in range(len(geomdata))];
        #######################################################################################################################
        #Recombine gathered chunks into a single list of rotation matrices for each geometry
        #Loop over geometries
        for i in range(len(geomdata)):
            #Loop over numbers of large cycles for current geometry
            tscNL_L=[];
            NL=1;
            for NLx in posttsc[i]:
                #Loop over pairs of swiss cheese solutions taken from each chunk for the current number of large cycles
                tsc_L=[];
                scflag=0;
                for y in NLx:
                    #If both rotation matrices rotate into integer bases, use them and skip to the next chunk. Else, add them to a list
                    if (y[1] and y[2]):
                        #tscNL_L+=[y];
                        tscNL_L+=[{'NLARGE':NL,'RMAT2CYCLE':py2mat(y[0][0]),'RMAT4CYCLE':py2mat(y[0][1]),'INTBASIS2CYCLE':bool(y[1]),'INTBASIS4CYCLE':bool(y[2])}];
                        scflag=1;
                        break;
                    tsc_L+=[y];
                #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one rotates into an integer basis, then use them and skip to the next NL
                if not scflag:
                    for z in tsc_L:
                        if z[1] or z[2]:
                            #tscNL_L+=[z];
                            tscNL_L+=[{'NLARGE':NL,'RMAT2CYCLE':py2mat(z[0][0]),'RMAT4CYCLE':py2mat(z[0][1]),'INTBASIS2CYCLE':bool(z[1]),'INTBASIS4CYCLE':bool(z[2])}];
                            scflag=1;
                            break;
                #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one exists and is favorable, then use them and skip to the next NL
                if not scflag:
                    for z in tsc_L:
                        if z[0]!=[] and z[0]!="unfav":
                            #tscNL_L+=[z];
                            tscNL_L+=[{'NLARGE':NL,'RMAT2CYCLE':py2mat(z[0][0]),'RMAT4CYCLE':py2mat(z[0][1]),'INTBASIS2CYCLE':bool(z[1]),'INTBASIS4CYCLE':bool(z[2])}];
                            scflag=1;
                            break;
                #If we still have not already chosen a swiss cheese solution for this NL, then just use the first pair of rotation matrices (even if they are empty or unfavorable) and skip to the next NL
                #if not scflag:
                    #tscNL_L+=[[]];
                    #tscNL_L+=[{'NLARGE':NL}];
                NL+=1;
            #Add new property to the GEOMDATA tier of the JSON
            #tempdict['GEOMDATA'][i].update({'SWISSCHEESE':py2mat(tscNL_L)});
            tempdict['GEOMDATA'][i].update({'SWISSCHEESE':tscNL_L});
        #Format JSON for output to file
        strtempdict=json.dumps(tempdict).replace(" ","")+"\n";
        #Wait until output file not in use. Then lock file, output JSON, and unlock file
        with open(outfile,"a") as outstream:
            fcntl.flock(outstream,fcntl.LOCK_EX);
            outstream.seek(0,2);
            outstream.write(strtempdict);
            outstream.flush();
            fcntl.flock(outstream,fcntl.LOCK_UN);
        #Output Group #, Bytes written, and Total time to output log
        time1=time.time();
        print "Group #: "+outbatchn;
        print "Bytes written: "+str(sys.getsizeof(strtempdict));
        print "Total time: "+str(time1-time0);
    except Exception as e:
        PrintException();
else:
    try:
        #While rank is not signalled to close
        while True:
            scatt=None;
            pretsc=comm.scatter(scatt,root=0);
            if pretsc==-1:
                #Rank has been signalled to close
                break;
            elif pretsc==-2:
                #Rank is extraneous and no information is being passed
                gath=[];
            else:
                h11_chunk,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,tscin_chunk=pretsc;
                #Reorganize chunks of information fed to this rank, so that it can be read in series of whole geometries
                tscin_chunkinds=sorted(deldup(transpose_list(tscin_chunk)[0]));
                tscin_chunk_byinds=[[i,[x[1:] for x in tscin_chunk if x[0]==i]] for i in tscin_chunkinds];
                #Loop for each chunk of information corresponding to a single geometry
                gath=[];
                for t in tscin_chunk_byinds:
                    #Loop for each number of large cycles from 1 to h11-1
                    tscNL_L_chunk=[];
                    for NL in range(1,h11_chunk):
                        #Loop for each triangulation for the current number of large cycles
                        tsc_L_chunk=[];
                        scflag=0;
                        for x in t[1]:
                            #Get swiss cheese rotation matrices for this triangulation
                            tsc_chunk=ToricSwissCheese(True,h11_chunk,NL,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,*x);
                            #If both rotation matrices rotate the basis into another integer basis, use them and skip to the next triangulation. Else add them to a list 
                            if (tsc_chunk[1] and tsc_chunk[2]):
                                tscNL_L_chunk+=[tsc_chunk];
                                scflag=1;
                                break;
                            tsc_L_chunk+=[tsc_chunk];
                        #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one rotates into an integer basis, then use them and skip to the next NL
                        if not scflag:
                            for y in tsc_L_chunk:
                                if y[1] or y[2]:
                                    tscNL_L_chunk+=[y];
                                    scflag=1;
                                    break;
                        #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one exists and is favorable, then use them and skip to the next NL
                        if not scflag:
                            for y in tsc_L_chunk:
                                if y[0]!=[] and y[0]!="unfav":
                                    tscNL_L_chunk+=[y];
                                    scflag=1;
                                    break;
                        #If we still have not already chosen a swiss cheese solution for this NL, then just use the first pair of rotation matrices (even if they are empty or unfavorable) and skip to the next NL
                        if not scflag:
                            tscNL_L_chunk+=[tsc_L_chunk[0]];
                    #Gather information into a list for each geometry and pass it back to main rank
                    gath+=[[t[0],tscNL_L_chunk]];
                posttsc_group=comm.gather(gath,root=0);
    except Exception as e:
        PrintException();