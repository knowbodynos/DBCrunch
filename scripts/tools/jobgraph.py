#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

import sys,re,toriccy,matplotlib;
import networkx as nx;
matplotlib.use('Agg');
import matplotlib.pyplot as plt;
from matplotlib.backends.backend_pdf import PdfPages;
plt.ioff();

modname=sys.argv[1];
controllerpath=sys.argv[2];
controllername=sys.argv[3];
workpath=sys.argv[4];
pdffile=sys.argv[5];

joblist=[];
jobsteplist=[];
try:
    with open(controllerpath+"/controller_"+modname+"_"+controllername+".out","r") as outstream:
        for line in outstream:
            if "batch job" in line:
                if len(jobsteplist)>0:
                    joblist+=[jobsteplist];
                jobsteplist=[];
            if "job step" in line:
                jobsteplist+=[re.sub("^.* "+modname+"_.*?_(.*?) .*$\n",r"\1",line).split("_")];
        if len(jobsteplist)>0:
            joblist+=[jobsteplist];

    maxdepth=len(joblist[0][0]);
    expandedleaves=[];
    for leavesbatch in joblist:
        ileaf=0;
        expandedfirst=[leavesbatch[ileaf][:i+1] for i in range(len(leavesbatch[ileaf])-1)];
        while ileaf<len(leavesbatch)-1:
            leavespair=leavesbatch[ileaf:ileaf+2];
            if all([len(x)>1 for x in leavespair]):
                expandedpairfirst=list(reversed([leavespair[0][:i+1] for i in range(len(leavespair[0])-1)]));
                expandedpairlast=[leavespair[1][:i+1] for i in range(len(leavespair[1])-1)];
                while (len(expandedpairfirst)>0) and (len(expandedpairlast)>0) and (expandedpairfirst[-1]==expandedpairlast[0]):
                    expandedpairfirst=expandedpairfirst[:-1];
                    expandedpairlast=expandedpairlast[1:];
                recombineleavespair=[leavespair[0]]+expandedpairfirst+expandedpairlast+[leavespair[1]];
            else:
                recombineleavespair=leavespair;
            leavesbatch=leavesbatch[:ileaf]+recombineleavespair+leavesbatch[ileaf+2:];
            ileaf+=len(recombineleavespair)-1;
        expandedlast=list(reversed([leavespair[1][:i+1] for i in range(len(leavespair[1])-1)]));
        expandedleaves+=[expandedfirst+leavesbatch+expandedlast];

    orderedexpandedleaves=toriccy.deldup([y for x in expandedleaves for y in x]);
    expandedleavesindexes=[[orderedexpandedleaves.index(y) for y in x] for x in expandedleaves];
    expandedleavessplitindexes=[];
    for i in range(len(expandedleaves)):
        expandedleavessplitindexbatch=[z+max([0]+[y for x in expandedleavessplitindexes for y in x if len(x)>0])-expandedleavesindexes[i][0]+1 for z in expandedleavesindexes[i]];
        expandedleavessplitindexes+=[expandedleavessplitindexbatch];
    expandedleavesitems=toriccy.deldup([(expandedleavessplitindexes[i][j],expandedleaves[i][j]) for i in range(len(expandedleaves)) for j in range(len(expandedleaves[i]))]);

    expandedleavespathrules=[(x[i],x[i+1]) for x in expandedleavessplitindexes for i in range(len(x)-1)];
    indexestolabels=[(x[0],x[1][-1]) for x in expandedleavesitems];
    indexestocoords=[(expandedleavesitems[i][0],((len(expandedleavesitems[i][1])-1),-1.5-4*sum([len(x[1])==maxdepth for x in expandedleavesitems[:i]]))) for i in range(len(expandedleavesitems))];
    indexnodecolors=['lightgray' for x in expandedleavesitems];
    indexnodesizes=[500 for x in expandedleavesitems];
    newindexeslabels=[];
    newindlist=[];
    newind=max([x[0] for x in expandedleavesitems])+1;
    ycoord=0;
    for x in expandedleavesitems:
        if len(x[1])==maxdepth:
            logfilename=modname+"_"+controllername+"_"+"_".join(x[1])+".log";
            try:
                with open(workpath+"/"+logfilename,"r") as logstream:
                    for line in logstream:
                        if any([y in line for y in ["CPUTime","MaxRSS","MaxVMSize","BSONSize"]]):
                            expandedleavespathrules+=[(x[0],newind)];
                            #indexestolabels+=[(newind,line.rstrip("\n"))];
                            indexestocoords+=[(newind,(maxdepth+1,ycoord))];
                            indexnodecolors+=['lightgray'];
                            indexnodesizes+=[0];
                            newindexeslabels+=[[maxdepth+1.05,ycoord-0.1,line.rstrip("\n")]];
                            newindlist+=[newind];
                            newind+=1;
                            ycoord-=1;
            except IOError:
                print "File path \""+pworkpath+"/"+logfilename+"\" does not exist.";
                sys.stdout.flush();
    indexestolabels=dict(indexestolabels);
    indexestocoords=dict(indexestocoords);

    xmin=-1;
    xmax=maxdepth+1;
    ymin=1-4*sum([len(x[1])==maxdepth for x in expandedleavesitems]);
    ymax=0;

    fig=plt.figure(figsize=(xmax,-ymin));
    plt.axis("off");
    X=nx.MultiDiGraph();
    X.add_edges_from(expandedleavespathrules);
    nx.draw_networkx_nodes(X,indexestocoords,node_color=indexnodecolors,node_size=indexnodesizes,node_shape='s');
    nx.draw_networkx_edges(X,indexestocoords);
    nx.draw_networkx_labels(X,indexestocoords,labels=indexestolabels);
    for x in newindexeslabels:
        plt.text(*x,bbox=dict(facecolor='lightgray',alpha=1),horizontalalignment='left');
    plt.xlim(xmin,xmax);
    plt.ylim(ymin,ymax);
    pdf_pages=PdfPages(controllerpath+"/"+modname+"_"+controllername+"_"+pdffile+".pdf");
    pdf_pages.savefig(fig,bbox_inches="tight");
    pdf_pages.close();
except IOError:
    print "File path \""+controllerpath+"/controller_"+modname+"_"+controllername+".out\" does not exist.";
    sys.stdout.flush();