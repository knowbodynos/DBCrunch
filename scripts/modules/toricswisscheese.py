#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

#Created by
#Ross Altman
#10/12/2015

from sage.all_cmdline import *;

import sys,os,fcntl,errno,linecache,traceback,time,subprocess,signal,json,mongolink;
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;
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

def default_sigpipe():
    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

#################################################################################
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
        mori_cols=mongolink.transpose_list(mori_rows);
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
            return [NL,[],int_basis_a,int_basis_b];
        else:
            return [NL,Tab[ind],int_basis_a,int_basis_b];
    else:
        return [NL,"unfav",0,0];

#################################################################################
#Main body
if rank==0:
    try:
        #IO Definitions
        geomdoc=json.loads(sys.argv[1]);
        #Read in pertinent fields from JSON
        polyid=geomdoc['POLYID'];
        geomn=geomdoc['GEOMN'];
        h11=geomdoc['H11'];
        dresverts=mat2py(geomdoc['DRESVERTS']);
        fgp=geomdoc['FUNDGP'];
        fav=geomdoc['FAV'];
        JtoDmat=mat2py(geomdoc['JTOD']);

        packagepath=subprocess.Popen("echo \"${SLURMONGO_ROOT}\" | head -c -1",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe).communicate()[0];
        statepath=packagepath+"/state";
        mongourifile=statepath+"/mongouri";
        with open(mongourifile,"r") as mongouristream:
            mongouri=mongouristream.readline().rstrip("\n");

        mongoclient=mongolink.MongoClient(mongouri+"?authMechanism=SCRAM-SHA-1");
        dbname=mongouri.split("/")[-1];
        db=mongoclient[dbname];
        triangdata=mongolink.collectionfind(db,'TRIANG1',{'H11':h11,'POLYID':polyid,'GEOMN':geomn},{'_id':0,'MORIMATP':1,'ITENSXD':1},formatresult='expression');
        mongoclient.close();
        ######################## Begin parallel MPI scatter/gather of toric swiss cheese information ###############################
        scatt=[[h11,dresverts,fgp,fav,JtoDmat,x] for x in mongolink.distribcores(triangdata,size)];
        #If fewer cores are required than are available, pass extraneous cores no information
        if len(scatt)<size:
            scatt+=[-2 for x in range(len(scatt),size)];
        #Scatter and define rank-independent input variables
        pretoricswisscheese=comm.scatter(scatt,root=0);
        h11_chunk,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,triangdata_chunk=pretoricswisscheese;
        #Loop for each number of large cycles from 1 to h11-1
        gath=[];
        for NL in range(1,h11_chunk):
            toricswisscheese_NL_chunk=[];
            scflag=False;
            #Loop over each triangulation with the current number of large cycles
            for x in triangdata_chunk:
                #Get swiss cheese rotation matrices for this triangulation
                toricswisscheese_chunk=ToricSwissCheese(True,h11_chunk,NL,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,x['MORIMATP'],x['ITENSXD']);
                #If both rotation matrices rotate the basis into another integer basis, use them and skip to the next triangulation. Else add them to a list 
                int_basis_a=toricswisscheese_chunk[2];
                int_basis_b=toricswisscheese_chunk[3];
                if (int_basis_a and int_basis_b):
                    gath+=[toricswisscheese_chunk];
                    scflag=True;
                    break;
                toricswisscheese_NL_chunk+=[toricswisscheese_chunk];
            #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list only one of them rotates into an integer basis, then use them and skip to the next NL
            if not scflag:
                for toricswisscheese_chunk in toricswisscheese_NL_chunk:
                    int_basis_a=toricswisscheese_chunk[2];
                    int_basis_b=toricswisscheese_chunk[3];
                    if (int_basis_a or int_basis_b):
                        gath+=[toricswisscheese_chunk];
                        scflag=True;
                        break;
            #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one of them exists and is favorable, then use them and skip to the next NL
            if not scflag:
                for toricswisscheese_chunk in toricswisscheese_NL_chunk:
                    if len(toricswisscheese_chunk[1])>0 and toricswisscheese_chunk[1]!="unfav":
                        gath+=[toricswisscheese_chunk];
                        scflag=True;
                        break;
            #If we still have not already chosen a swiss cheese solution for this NL, then just use the first pair of rotation matrices (even if they are empty or unfavorable) and skip to the next NL
            if not scflag:
                toricswisscheese_chunk=toricswisscheese_NL_chunk[0];
                gath+=[toricswisscheese_chunk];
        posttoricswisscheese_group=comm.gather(gath,root=0);
        #Signal ranks to exit current process (if there are no other processes, then exit other ranks)
        scatt=[-1 for j in range(size)];
        pretoricswisscheese=comm.scatter(scatt,root=0);
        #Reorganize gathered information into a serial form
        posttoricswisscheese=[x for y in posttoricswisscheese_group for x in y];
        #posttoricswisscheese=mongolink.transpose_list(posttoricswisscheese_redist);
        #print posttoricswisscheese;
        for toricswisscheese_NL in posttoricswisscheese:
            if len(toricswisscheese_NL[1])>0 and toricswisscheese_NL[1]!="unfav":
                print "+SWISSCHEESE1."+json.dumps({'POLYID':polyid,'GEOMN':geomn,'NLARGE':toricswisscheese_NL[0]},separators=(',',':'))+">"+json.dumps({'POLYID':polyid,'GEOMN':geomn,'NLARGE':toricswisscheese_NL[0],'RMAT2CYCLE':py2mat(toricswisscheese_NL[1][0]),'RMAT4CYCLE':py2mat(toricswisscheese_NL[1][1]),'INTBASIS2CYCLE':bool(toricswisscheese_NL[2]),'INTBASIS4CYCLE':bool(toricswisscheese_NL[3])},separators=(',',':'));
            else:
                print "None";
            sys.stdout.flush();
        #######################################################################################################################
        ##Recombine gathered chunks into a single list of rotation matrices for each geometry
        ##Loop over numbers of large cycles for current geometry
        #NL=1;
        #for NLx in posttoricswisscheese:
        #    #Loop over pairs of swiss cheese solutions taken from each chunk for the current number of large cycles
        #    tsc_L=[];
        #    scflag=False;
        #    for y in NLx:
        #        #If both rotation matrices rotate into integer bases, use them and skip to the next chunk. Else, add them to a list
        #        if (y[1] and y[2]):
        #            #tscNL_L+=[y];
        #            print "+SWISSCHEESE1.{\"POLYID\":"+str(polyid)+",\"'GEOMN\":"+str(geomn)+",\"'NLARGE\":"+str(NL)+"}>"+json.dumps({'POLYID':polyid,'GEOMN':geomn,'NLARGE':NL,'RMAT2CYCLE':py2mat(y[0][0]),'RMAT4CYCLE':py2mat(y[0][1]),'INTBASIS2CYCLE':bool(y[1]),'INTBASIS4CYCLE':bool(y[2])},separators=(',',':'));
        #            scflag=True;
        #            break;
        #        tsc_L+=[y];
        #    #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one rotates into an integer basis, then use them and skip to the next NL
        #    if not scflag:
        #        for z in tsc_L:
        #            if z[1] or z[2]:
        #                #tscNL_L+=[z];
        #                print "+SWISSCHEESE1.{\"POLYID\":"+str(polyid)+",\"'GEOMN\":"+str(geomn)+",\"'NLARGE\":"+str(NL)+"}>"+json.dumps({'POLYID':polyid,'GEOMN':geomn,'NLARGE':NL,'RMAT2CYCLE':py2mat(z[0][0]),'RMAT4CYCLE':py2mat(z[0][1]),'INTBASIS2CYCLE':bool(z[1]),'INTBASIS4CYCLE':bool(z[2])},separators=(',',':'));
        #                scflag=True;
        #                break;
        #    #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one exists and is favorable, then use them and skip to the next NL
        #    if not scflag:
        #        for z in tsc_L:
        #            if z[0]!=[] and z[0]!="unfav":
        #                #tscNL_L+=[z];
        #                print "+SWISSCHEESE1.{\"POLYID\":"+str(polyid)+",\"'GEOMN\":"+str(geomn)+",\"'NLARGE\":"+str(NL)+"}>"+json.dumps({'POLYID':polyid,'GEOMN':geomn,'NLARGE':NL,'RMAT2CYCLE':py2mat(z[0][0]),'RMAT4CYCLE':py2mat(z[0][1]),'INTBASIS2CYCLE':bool(z[1]),'INTBASIS4CYCLE':bool(z[2])},separators=(',',':'));
        #                scflag=True;
        #                break;
        #    #If we still have not already chosen a swiss cheese solution for this NL, then just use the first pair of rotation matrices (even if they are empty or unfavorable) and skip to the next NL
        #    #if not scflag:
        #        #tscNL_L+=[[]];
        #        #tscNL_L+=[{'NLARGE':NL}];
        #    NL+=1;
    except Exception as e:
        PrintException();
else:
    try:
        #While rank is not signalled to close
        while True:
            scatt=None;
            pretoricswisscheese=comm.scatter(scatt,root=0);
            if pretoricswisscheese==-1:
                #Rank has been signalled to close
                break;
            elif pretoricswisscheese==-2:
                #Rank is extraneous and no information is being passed
                gath=[];
            else:
                h11_chunk,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,triangdata_chunk=pretoricswisscheese;
                #Loop for each number of large cycles from 1 to h11-1
                gath=[];
                for NL in range(1,h11_chunk):
                    toricswisscheese_NL_chunk=[];
                    scflag=False;
                    #Loop over each triangulation with the current number of large cycles
                    for x in triangdata_chunk:
                        #Get swiss cheese rotation matrices for this triangulation
                        toricswisscheese_chunk=ToricSwissCheese(True,h11_chunk,NL,dresverts_chunk,fgp_chunk,fav_chunk,JtoDmat_chunk,x['MORIMATP'],x['ITENSXD']);
                        #If both rotation matrices rotate the basis into another integer basis, use them and skip to the next triangulation. Else add them to a list 
                        int_basis_a=toricswisscheese_chunk[1];
                        int_basis_b=toricswisscheese_chunk[2];
                        if (int_basis_a and int_basis_b):
                            gath+=[toricswisscheese_chunk];
                            scflag=True;
                            break;
                        toricswisscheese_NL_chunk+=[toricswisscheese_chunk];
                    #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list only one of them rotates into an integer basis, then use them and skip to the next NL
                    if not scflag:
                        for toricswisscheese_chunk in toricswisscheese_NL_chunk:
                            int_basis_a=toricswisscheese_chunk[1];
                            int_basis_b=toricswisscheese_chunk[2];
                            if (int_basis_a or int_basis_b):
                                gath+=[toricswisscheese_chunk];
                                scflag=True;
                                break;
                    #If we have not already chosen a swiss cheese solution for this NL, check if for any pair of rotation matrices from the list one of them exists and is favorable, then use them and skip to the next NL
                    if not scflag:
                        for toricswisscheese_chunk in toricswisscheese_NL_chunk:
                            if toricswisscheese_chunk[0]!=[] and toricswisscheese_chunk[0]!="unfav":
                                gath+=[toricswisscheese_chunk];
                                scflag=True;
                                break;
                    #If we still have not already chosen a swiss cheese solution for this NL, then just use the first pair of rotation matrices (even if they are empty or unfavorable) and skip to the next NL
                    if not scflag:
                        toricswisscheese_chunk=toricswisscheese_NL_chunk[0];
                        gath+=[toricswisscheese_chunk];
                posttoricswisscheese_group=comm.gather(gath,root=0);
    except Exception as e:
        PrintException();