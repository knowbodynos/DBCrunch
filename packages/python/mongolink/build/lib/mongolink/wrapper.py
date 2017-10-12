import sys,linecache,traceback,json;

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

def wrap():
    def decorate(func):
        def call(*args,**kwargs):
            docsfile=sys.argv[1];
            if len(sys.argv)>3:
                basecollection=sys.argv[2];
                dbindexes=sys.argv[3:];
            with open(docsfile,"r") as docstream:
                for line in docstream:
                    doc=json.loads(line.rstrip("\n"));
                    try:
                        func(doc,*args,**kwargs);
                    except Exception as e:
                        PrintException();
                    if len(sys.argv)>3:
                        print("@"+basecollection+"."+json.dumps(dict([(x,doc[x]) for x in dbindexes]),separators=(',',':')));
                    sys.stdout.flush();
        return call;
    return decorate;