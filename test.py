#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,time;

def main():
    line=sys.stdin.readline().rstrip("\n");
    while line!="":
    	starttime=time.time();
    	while (time.time()-starttime)<5:
    		time.sleep(0.1);
        sys.stdout.write("out: "+str(line)+"\n");
        sys.stdout.flush();
        line=sys.stdin.readline().rstrip("\n");

if __name__=='__main__':
    main();