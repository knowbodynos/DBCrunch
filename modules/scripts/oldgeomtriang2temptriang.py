#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

from sage.all_cmdline import *;

import sys,os,linecache,traceback,json,mongolink;
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

#################################################################################
#Main body
try:
    #IO Definitions
    polygeomtriangdoc=json.loads(sys.argv[1]);
    #Read in pertinent fields from JSON
    polyid=polygeomtriangdoc['POLYID'];
    oldgeomn=polygeomtriangdoc['GEOMN'];
    oldtriangn=polygeomtriangdoc['TRIANGN'];
    #Add new properties to base tier of JSON
    print "+TRIANGtemp."+json.dumps({'POLYID':polyid,'GEOMN':oldgeomn,'TRIANGN':oldtriangn},separators=(',',':'))+">"+json.dumps({'H11':polygeomtriangdoc['H11'],'EULER':polygeomtriangdoc['EULER'],'BASIS':polygeomtriangdoc['BASIS'],'JTOD':polygeomtriangdoc['JTOD'],'INVBASIS':polygeomtriangdoc['INVBASIS'],'CHERN2XJ':polygeomtriangdoc['CHERN2XJ'],'CHERN2XNUMS':polygeomtriangdoc['CHERN2XNUMS'],'IPOLYXJ':polygeomtriangdoc['IPOLYXJ'],'ITENSXJ':polygeomtriangdoc['ITENSXJ']},separators=(',',':'));
    sys.stdout.flush();
except Exception as e:
    PrintException();