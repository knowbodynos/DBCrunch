#!/shared/apps/sage-7.4/local/bin/sage -python

from sage.all_cmdline import *;

import sys,linecache,traceback,json;
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
    line=linecache.getline(filename, lineno, f.f_globals);
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename,lineno,line.strip(),exc_obj);
    print "More info: ",traceback.format_exc();

#################################################################################
#Main body
try:
    #IO Definitions
    polydoc=json.loads(sys.argv[1]);
    #Read in pertinent fields from JSON
    polyid=polydoc['POLYID'];
    nverts=mat2py(polydoc['NVERTS']);
    lp=LatticePolytope(nverts);
    dlp=lp.polar();

    dverts=[list(x) for x in dlp.normal_form().column_matrix().columns()];

    lp_interpts=[y for x in lp.faces_lp(codim=1) for y in x.interior_points()];
    lp_noninterpts=[list(x) for x in lp.points() if x not in lp_interpts];

    dlp_interpts=[y for x in dlp.faces_lp(codim=1) for y in x.interior_points()];
    dlp_noninterpts=[list(x) for x in dlp.points() if x not in dlp_interpts];
    #Add new properties to the base tier of the JSON
    print "+POLY."+json.dumps({'POLYID':polyid},separators=(',',':'))+">"+json.dumps({'DVERTS':py2mat(dverts),'NNINTPTS':py2mat(lp_noninterpts),'DNINTPTS':py2mat(dlp_noninterpts)},separators=(',',':'));
    sys.stdout.flush();
except Exception as e:
    PrintException();