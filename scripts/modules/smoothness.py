#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

from sage.all_cmdline import *;
import sys,json,mongolink,itertools;
from mongolink.parse import mathematicalist2pythonlist as mat2py

involdoc=json.loads(sys.argv[1]);
polyid=involdoc['POLYID'];
geomn=involdoc['GEOMN'];
triangn=involdoc['TRIANGN'];
involn=involdoc['INVOLN'];
rescws=mat2py(involdoc['RESCWS']);
symcyterms=involdoc['SYMCYPOLY'];

gens="("+",".join(["x"+str(i+1) for i in range(len(rescws))])+")";
var(gens.replace("(","").replace(")","").replace(","," "));
srideal=eval(involdoc['SRIDEAL'].replace("{","[[").replace("}","]]").replace(",","],[").replace("*",",").replace("D","x"));
supersrsectors=mongolink.deldup([set(x) for x in list(itertools.product(*srideal))]);
srsectors=[str([y-1 for y in supersrsectors[i]]).replace("[","").replace("]","").replace(" ","") for i in range(len(supersrsectors)) if not any([supersrsectors[i].issuperset(x) for x in supersrsectors[:i]+supersrsectors[i+1:]])];

singular.option("noredefine");

symcypoly="+".join([str(int(ZZ.random_element(-2*len(symcyterms),2*len(symcyterms))))+"*"+x for x in symcyterms]);
singular.eval("ring r=1500450271,"+gens+",dp; ideal P="+symcypoly+";");
maxdim1=max([int(singular.eval("ideal SR="+x+"; ideal I=SR,P,jacob(P); int(dim(groebner(I)));")) for x in srsectors]);

symcypoly="+".join([str(int(ZZ.random_element(-2*len(symcyterms),2*len(symcyterms))))+"*"+x for x in symcyterms]);
singular.eval("ring r=1500450271,"+gens+",dp; ideal P="+symcypoly+";");
maxdim2=max([int(singular.eval("ideal SR="+x+"; ideal I=SR,P,jacob(P); int(dim(groebner(I)));")) for x in srsectors]);

if maxdim1==maxdim2:
    maxdim=maxdim1;
else:
    symcypoly="+".join([str(int(ZZ.random_element(-2*len(symcyterms),2*len(symcyterms))))+"*"+x for x in symcyterms]);
    singular.eval("ring r=1500450271,"+gens+",dp; ideal P="+symcypoly+";");
    maxdim3=max([int(singular.eval("ideal SR="+x+"; ideal I=SR,P,jacob(P); int(dim(groebner(I)));")) for x in srsectors]);
    maxdim=maxdim3;
        
query={"POLYID":polyid,"GEOMN":geomn,"TRIANGN":triangn,"INVOLN":involn};
output={"CYSINGDIM":maxdim};

print "+INVOL."+json.dumps(query,separators=(',',':'))+">"+json.dumps(output,separators=(',',':'));
sys.stdout.flush();