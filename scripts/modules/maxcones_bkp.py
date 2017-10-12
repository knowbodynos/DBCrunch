#!/shared/apps/sage-7.4/local/bin/sage -python

from sage.all_cmdline import *;

import sys,linecache,traceback,json,mongolink;#os,tempfile,time,datetime,subprocess,signal
from mongolink.parse import pythonlist2mathematicalist as py2mat;
from mongolink.parse import mathematicalist2pythonlist as mat2py;
import mongolink.tools as tools;

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

#def default_sigpipe():
#    signal.signal(signal.SIGPIPE,signal.SIG_DFL);

'''
def timestamp2unit(timestamp,unit="seconds"):
    if timestamp=="infinite":
        return timestamp;
    else:
        days=0;
        if "-" in timestamp:
            daysstr,timestamp=timestamp.split("-");
            days=int(daysstr);
        hours,minutes,seconds=[int(x) for x in timestamp.split(":")];
        hours+=days*24;
        minutes+=hours*60;
        seconds+=minutes*60;
        if unit=="seconds":
            return seconds;
        elif unit=="minutes":
            return float(seconds)/60.;
        elif unit=="hours":
            return float(seconds)/(60.*60.);
        elif unit=="days":
            return float(seconds)/(60.*60.*24.);
        else:
            return 0;
'''

#def timeleft(starttime,timelimit):
#    "Determine if runtime limit has been reached."
#    if timelimit=="infinite":
#        return True;
#    else:
#        return ((timestamp2unit(timelimit)-(time.time()-starttime)))>0;

try:
    #starttime=time.time();

    workpath=sys.argv[1];
    jobstepname=sys.argv[2];
    dbindexes=eval(sys.argv[3]);

    with open(workpath+"/"+jobstepname+".docs","r") as docstream:
        count=0;
        #polydoc=next(polycurs,None);
        line=docstream.readline();
        while line!="":# and timeleft(starttime,timelimit):
            polydoc=json.loads(line.rstrip("\n"));
            #iddoc=dict([(x,polydoc[x]) for x in dbindexes if x in polydoc.keys()]);
            #Read in pertinent fields from JSON
            polyid=polydoc['POLYID'];
            nverts=mat2py(polydoc['NVERTS']);

            lp=LatticePolytope(nverts);
            dlp=LatticePolytope(lp.polar().normal_form());

            dverts=[list(x) for x in dlp.vertices().column_matrix().columns()];

            #lp_facets=lp.faces_lp(codim=1);

            #lp_facetbndrypts=[[list(y) for y in x.boundary_points()] for x in lp_facets];

            #lp_bndrypts_dup=[y for x in lp_facetbndrypts for y in x];
            #lp_bndrypts=[lp_bndrypts_dup[i] for i in range(len(lp_bndrypts_dup)) if lp_bndrypts_dup[i] not in lp_bndrypts_dup[:i]];

            dlp_facets=dlp.faces_lp(dim=3);

            #dlp_facetbndrypts=[[list(y) for y in x.boundary_points()] for x in dlp_facets];
            #dlp_facetinterpts=[[list(y) for y in x.interior_points()] for x in dlp_facets];
            #dlp_maxcone_normalform_dup=[[list(y) for y in LatticePolytope(x.vertices().column_matrix().columns()+[vector((0,0,0,0))]).normal_form().column_matrix().columns()] for x in dlp_facets];
            #dlp_maxcone_normalform_inds=[i for i in range(len(dlp_maxcone_normalform_dup)) if dlp_maxcone_normalform_dup[i] not in dlp_maxcone_normalform_dup[:i]];
            
            #dlp_bndrypts_dup=[y for x in dlp_facetbndrypts for y in x];
            #dlp_bndrypts=[dlp_bndrypts_dup[i] for i in range(len(dlp_bndrypts_dup)) if dlp_bndrypts_dup[i] not in dlp_bndrypts_dup[:i]];

            #dlp_interpts_dup=[y for x in dlp_facetinterpts for y in x];
            #dlp_interpts=[dlp_interpts_dup[i] for i in range(len(dlp_interpts_dup)) if dlp_interpts_dup[i] not in dlp_interpts_dup[:i]];

            nformfaceinfolist_dup=[];
            for facet in dlp_facets:
                nform=[list(w) for w in LatticePolytope(facet.vertices().column_matrix().columns()+[vector((0,0,0,0))]).normal_form().column_matrix().columns()];
                dim0=[facet.nvertices()];
                dim1=[[len(z),sum(z),min(z),max(z)] for z in [[len(y.interior_points()) for y in facet.faces_lp(dim=1)]]][0];
                dim2=[[len(z),sum(z),min(z),max(z)] for z in [[len(y.interior_points()) for y in facet.faces_lp(dim=2)]]][0];
                nformfaceinfolist_dup+=[[nform]+[dim0+dim1+dim2]];
            nformfaceinfolist=[nformfaceinfolist_dup[i] for i in range(len(nformfaceinfolist_dup)) if nformfaceinfolist_dup[i] not in nformfaceinfolist_dup[:i]];

            print("+POLY."+json.dumps({'POLYID':polyid},separators=(',',':'))+">"+json.dumps({'DVERTS':py2mat(dverts)},separators=(',',':')));
            sys.stdout.flush();
            
            for nformfaceinfo in nformfaceinfolist:
                nform=nformfaceinfo[0];
                faceinfo=nformfaceinfo[1];
                ninstances=nformfaceinfolist_dup.count(nformfaceinfo);
                print("&MAXCONE."+json.dumps({'NORMALFORM':py2mat(nform)},separators=(',',':'))+">"+json.dumps({'POS':{'POLYID':polyid,'NINST':ninstances}},separators=(',',':')));
                print("+MAXCONE."+json.dumps({'NORMALFORM':py2mat(nform)},separators=(',',':'))+">"+json.dumps({'FACEINFO':py2mat(faceinfo)},separators=(',',':')));
                sys.stdout.flush();

            print("@POLY."+json.dumps({'POLYID':polyid},separators=(',',':')));
            sys.stdout.flush();

            line=docstream.readline();

    #with tempfile.NamedTemporaryFile(dir=workpath,delete=False) as tempstream:
    #    while line!="":
    #        tempstream.write(line);
    #        tempstream.flush();
    #        line=docstream.readline();
    #    os.rename(tempstream.name,docstream.name);

    #if line!="" and not timeleft(starttime,timelimit):   
        #submit=subprocess.Popen("sbatch "+mainpath+"/"+jobnumpad+"/maxcones_"+jobnumpad+".job",shell=True,stdout=subprocess.PIPE,preexec_fn=default_sigpipe);
        #submitcomm=submit.communicate()[0].rstrip("\n");
        #Print information about job submission
        #print "";
        #print datetime.datetime.now().strftime("%Y %m %d %H:%M:%S");
        #print "Res"+submitcomm[1:]+" as maxcones_"+jobstepname+".";
        #print "";
        #print "";
        #print "Paused"
        #sys.stdout.flush();

    #docstream.close();
except Exception as e:
    PrintException();