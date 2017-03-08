import re,copy,bson,json;
from . import tools;

def bsonsize(doc):
    return len(bson.BSON.encode(doc));

def pythonlist2mathematicalist(lst):
    "Converts a Python list to a string depicting a list in Mathematica format."    
    return str(lst).replace(" ","").replace("[","{").replace("]","}");

def mathematicalist2pythonlist(lst):
    "Converts a string depicting a list in Mathematica format to a Python list."
    return eval(str(lst).replace(" ","").replace("{","[").replace("}","]"));

def pythondictionary2mathematicarules(dic):
    return str(dic).replace("u'","'").replace(" ","").replace("[","{").replace("]","}").replace("'","\\\"").replace(":","->");

def finddicts(nested,dictkeys=[],nondictkeys=[],orig=True):
    "Get all positions of sub-dictionaries within the nested dictionary nested."
    intermednondictkeys=[];
    founddicts=[];
    if type(nested)==dict:
        nesteditems=nested.items();
        for i in range(len(nesteditems)):
            if ((type(nesteditems[i][1])==dict) or (type(nesteditems[i][1])==list)) and (nesteditems[i][0]!="_id"):
                founddicts+=finddicts(nesteditems[i][1],dictkeys+[nesteditems[i][0]],nondictkeys+intermednondictkeys,orig=False);
            else:
                intermednondictkeys+=[dictkeys+[nesteditems[i][0]]];
    elif type(nested)==list:
        for i in range(len(nested)):
            if (type(nested[i])==dict) or (type(nested[i])==list):
                founddicts+=finddicts(nested[i],dictkeys+[i],nondictkeys+intermednondictkeys,orig=False);
            else:
                intermednondictkeys+=[dictkeys+[i]];
    listsallowed=tools.deldup(founddicts+nondictkeys+intermednondictkeys);
    dictonly=tools.deldup([y[:filter(lambda x:type(y[x]) in [str,unicode],range(len(y)))[-1]+1] for y in listsallowed]);
    if orig:
        nobasedict=tools.deldup([x if (not (type(tools.nestind(nested,x)) in [str,unicode]) or not x[-2]=="INVOL") else x[:-1] for x in dictonly]);
        return nobasedict;
    else:
        return dictonly;

def stringform2expressionform(s):
    "Convert compressed value to expanded value."
    if type(s)==str:
        s1=unicode(s,'utf-8');
    else:
        s1=s;
    if type(s1)==unicode:
        if s1.replace(" ","").replace("{","").replace("}","").replace(",","").replace("-","").replace("/","").isnumeric():
            result=eval(s1.replace("{","[").replace("}","]"));
        else:
            s1=s1.replace("{","[").replace("}","]");
            while s1.count("->")>0:
                s1=re.sub('\[([^\[,\]]*?)->([^\[,\]]*?)\]',r'{\1:\2}',s1);
            result=re.sub('([\[{,:])([^\[\]{},:0-9]+.*?)(?=([\]},:]))',r'\1"\2"',s1);
    else:
        result=s1;
    return result;

def expressionform2stringform(e):
    "Convert expanded value to compressed value."
    if (type(e)==list) or (type(e)==dict):
        result=unicode(e).replace(" ","").replace("':'","->").replace("[","{").replace("]","}").replace("'","");
    else:
        result=e;
    return result;

def string2expression(doc):
    "Convert compressed document to expanded document."
    newdoc=copy.deepcopy(doc);
    for x in finddicts(newdoc):
        tools.nestind(newdoc,x,subf=stringform2expressionform);
    return newdoc;

def expression2string(doc):
    "Convert expanded document to compressed document."
    newdoc=copy.deepcopy(doc);
    for x in finddicts(newdoc):
        tools.nestind(newdoc,x,subf=expressionform2stringform);
    return newdoc;