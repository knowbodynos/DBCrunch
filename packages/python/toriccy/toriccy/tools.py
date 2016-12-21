#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

def deldup(lst):
    "Delete duplicate elements in lst."
    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]];

def transpose_list(lst):
    "Get the transpose of a list of lists."
    return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))];

def nestind(nested,indlist,subf=None):
    "Get element with indices given in indlist from nested list. If subf is defined as a function, replace current element with function output."
    if len(indlist)==0:
        return nested;
    if len(indlist)==1:
        if subf!=None:
            nested.update({indlist[0]:subf(nested[indlist[0]])});
        else:
            return nested[indlist[0]];
    if len(indlist)>1:
        return nestind(nested[indlist[0]],indlist[1:],subf);