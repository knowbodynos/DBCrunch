def deldup(lst):
    "Delete duplicate elements in lst."
    return [lst[i] for i in range(len(lst)) if lst[i] not in lst[:i]];

def transpose_list(lst):
    "Get the transpose of a list of lists."
    try:
        if len(lst)==0:
            raise IndexError('List is empty.');
        elif not all([len(x)==len(lst[0]) for x in lst]):
            raise IndexError('Lists have different sizes.');
        else:
            return [[lst[i][j] for i in range(len(lst))] for j in range(len(lst[0]))];
    except IndexError:
        return [];

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

def distribcores(lst,size):
    "Distribute information in lst into chunks of size size in order to scatter to various cores."
    L=len(lst);
    mod=L%size;
    split=[];
    j=0;
    for i in range(size):
        increm=((L-mod)/size);
        if i<mod:
            split+=[lst[j:j+increm+1]];
            j+=increm+1;
        else:
            split+=[lst[j:j+increm]];
            j+=increm;
    return split;