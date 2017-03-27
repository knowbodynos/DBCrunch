#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python
from sage.all_cmdline import *

from sage.schemes.toric.variety import normalize_names
from sage.rings.polynomial.polydict import PolyDict
from sage.libs.singular.function_factory import singular_function
from functools import reduce
import itertools
import numpy as np
from numpy.linalg import matrix_rank
import math
import fractions
from sympy import Matrix
import copy
from time import time
import json
import sage.logic.propcalc as propcalc
import re
import bson
import sys
from collections import Counter
# from mongolink.parse import pythonlist2mathematicalist as py2mat
# from mongolink.parse import mathematicalist2pythonlist as mat2py

### FORMAT CONVERTERS ###

def sr_m2p(sr):
    sr = str(sr)
    sr = re.sub(" ", "", sr)
    sr = sr.replace("{", "")
    sr = sr.replace("}", "")
    sr = sr.replace("D", "")
    sr = sr.replace("*", "")
    sr = sr.split(",")

    sr = [[int(w) for w in x] for x in sr]
    return sr


def sr_p2m(sr):
    sr = [['D' + str(w) for w in x] for x in sr]
    sr = ['*'.join(w) for w in sr]
    sr = ','.join(sr)
    sr = str(sr)
    sr = sr.replace("[","")
    sr = sr.replace("]","")
    sr = "{" + sr + "}"
    return sr


def rwmat_m2p(rwmat):
    rwmat = str(rwmat)
    rwmat = re.sub(" ", "", rwmat)
    rwmat = rwmat.replace("{","[")
    rwmat = rwmat.replace("}","]")
    return np.transpose(np.array(eval(rwmat)))


def rwmat_p2m(rwmat):
    rwmat = np.transpose(rwmat).tolist()
    rwmat = [str(w) for w in rwmat]
    rwmat = [w.replace("[", "{") for w in rwmat]
    rwmat = [w.replace("]", "}") for w in rwmat]
    rwmat = str(rwmat)
    rwmat = '{' + rwmat[1:-1] + '}'
    rwmat = rwmat.replace('\'', '')
    return rwmat


def bi_m2p(bi):
    bi = str(bi)
    bi = re.sub(" ", "", bi)
    bi = bi.replace("{", "")
    bi = bi.replace("}", "")
    bi = bi.split(",")

    basis = []
    for x in bi:
        if x[0] == 'D':
            basis.append(int(x[1:]))
    return basis


def bi_p2m(bi):
    n = len(bi)
    bi = ['D' + str(w) for w in bi]

    basis = []
    for i in range(n):
        basis.append(['J' + str(i+1), bi[i]])

    basis = str(basis)
    basis = basis.replace("[", "{")
    basis = basis.replace("]", "}")
    basis = basis.replace('\'', '')
    return basis


def invol_m2p(invol):
    invol = str(invol)
    invol = re.sub(" ", "", invol)
    invol = invol[1:-1]
    invol = invol.split(",")
    involP = []

    for s in invol:
        s = s.replace("D", "")
        s = s.split("->")
        s = [int(w) for w in s]
        involP.append(sorted(s))

    involP = [tuple(w) for w in involP]
    involP = tuple(involP)
    involP = list(set(involP))
    involP = sorted([list(w) for w in involP])
    return involP


def invol_p2m(invol):
    involM = []
    for x in invol:
        involM.append(x)
        involM.append(x[::-1])

    involM = [['D'+str(w) for w in x] for x in involM]
    involM = ['->'.join(x) for x in involM]

    involM = str(involM)
    involM = involM.replace("[", "{")
    involM = involM.replace("]", "}")
    involM = involM.replace("'", "")
    return involM


def oplanes_m2p(op, pr):
    op = str(op)
    op = op[2:-2]
    op = op.split("},{")
    op = [w.split(",") for w in op]
    op = [[pr(w) for w in x] for x in op]
    return op


def oplanes_p2m(op):
    op = str(op)
    op = op.replace("[", "{")
    op = op.replace("]", "}")
    op = re.sub(" ", "", op)
    return op


def itens_m2p(itens):
    itens = itens.replace("{", "[")
    itens = itens.replace("}", "]")
    itens = eval(itens)
    return itens


### CONVERTERS END HERE ###
    

def inv(sigma, poly, pr):
    """Returns the polynomial after being acted on by sigma"""
    k = len(sigma)
    d = poly.dict()
    d2 = {}
          
    for key in d.keys():
        nkey = list(key)
        for i in range(k):
            swap = sigma[i]
            nkey[swap[0]] = key[swap[1]]
            nkey[swap[1]] = key[swap[0]]
        nkey = tuple(nkey)
        d2[nkey] = d[key]
            
        
    pd2 = PolyDict(d2)
    poly = pd2.poly_repr(normalize_names("x+", pr.ngens()))
        
    return poly


def sift(vecs):
    """Sifts the given list to find a set of linearly independent vectors."""
    basis = []
    p = len(vecs)
    q = len(vecs[0])
    if is_nonzero(vecs[0]):
        basis.append(vecs[0])
    
    for i in range(1, p):
        vcols = [x for x in basis]
        vcols.append(vecs[i])
        vmat = matrix(ZZ, vcols).transpose()
        vmat = vmat.rref()
        if vmat.rank() > len(basis):
            basis.append(vecs[i])
    
    return basis


def symm_poly(poly, ai, pr):
    """Determines the terms of the CY which are symmetric under the involution."""
    p_str = str(poly)
    p_terms = p_str.split('+')
    for i in range(len(p_terms)):
        p_terms[i] = p_terms[i].split('-')
    p_terms = list(itertools.chain.from_iterable(p_terms))
    p_terms = [p.strip() for p in p_terms]

    p_terms = [pr(p) for p in p_terms]
    n = len(p_terms)
    symm = []
    for i in range(n):
        parity = 1
        term = p_terms[i]
        d = term.dict()
        exps = list(d.keys()[0])
        for j in range(len(exps)):
            if j in ai:
                parity *= int((-1)**exps[j])

        if parity == 1:
            symm.append(p_terms[i])

    # Compute ntuned, the number of terms that must be set to zero
    # Maybe will need this at some point?
    #ntuned = len(p_terms) - len(symm)
    symm_poly = pr(0)
    for i in range(len(symm)):
        symm_poly += pr(symm[i])
    return symm_poly


def symm_poly_swap(poly, sigma, pr):
    """Determines the terms of the CY which are symmetric under the involution."""
    pStr = str(poly)
    pTerms = pStr.split('+')
    for i in range(len(pTerms)):
        pTerms[i] = pTerms[i].split('-')
    pTerms = list(itertools.chain.from_iterable(pTerms))
    pTerms = [p.strip() for p in pTerms]

    pTerms = [pr(p) for p in pTerms]
    n = len(pTerms)
    symm = []
    for i in range(n):
        term = pTerms[i]
        sigmaTerm = pr(inv(sigma, term, pr))
        if sigmaTerm in pTerms:
            symm.append(term)

    symmPoly = pr(0)
    for i in range(len(symm)):
        symmPoly += pr(symm[i])
    return symmPoly


def poly_index_raiser(poly, pr):
    n = pr.ngens()
    prN = PolynomialRing(base_ring=pr.base_ring(), names=normalize_names('x+', n+1))
    xN = prN.gens()
    poly = prN(poly)
    pt = [xN[i+1] for i in range(n)]
    pt = pt + [0]
    poly = poly(pt)
    
    upNames = ['x' + str(i+1) for i in range(n)]
    prUp = PolynomialRing(base_ring=pr.base_ring(), names=upNames)
    return prUp(poly)


def poly_index_lowerer(poly, pr):
    n = pr.ngens()
    prN = PolynomialRing(base_ring=pr.base_ring(), names=normalize_names('x+', n+1))
    xN = prN.gens()
    poly = prN(poly)
    pt = [xN[i] for i in range(n)]
    pt = [0] + pt
    poly = poly(pt)

    downNames = normalize_names('x+', n)
    prDown = PolynomialRing(base_ring=pr.base_ring(), names=downNames)
    return prDown(poly)


def raise_fsets(fsets, pr):
    for i in range(len(fsets)):
        for j in range(len(fsets[i])):
            fsets[i][j] = poly_index_raiser(pr(fsets[i][j]), pr)
    return fsets


def lower_fsets(fsets, pr):
    for i in range(len(fsets)):
        for j in range(len(fsets[i])):
            fsets[i][j] = poly_index_lowerer(pr(fsets[i][j]), pr)
    return fsets



def even_power(n):
    """Returns the exponent of the largest power of 2 that divides n."""
    if n == 0 or n == 1:
        return 0
    factors = list(factor(n))
    if factors[0][0] == 2:
        return factors[0][1]
    return 0


def symm_terms(poly, ai, pr):
    """Determines the terms of the CY which are symmetric under the involution."""
    p_str = str(poly)
    p_terms = p_str.split('+')
    for i in range(len(p_terms)):
        p_terms[i] = p_terms[i].split('-')
    p_terms = list(itertools.chain.from_iterable(p_terms))
    p_terms = [p.strip() for p in p_terms]

    p_terms = [pr(p) for p in p_terms]
    n = len(p_terms)
    symm = []
    for i in range(n):
        parity = 1
        term = p_terms[i]
        d = term.dict()
        exps = list(d.keys()[0])
        for j in range(len(exps)):
            if j in ai:
                parity *= int((-1)**exps[j])

        if parity == 1:
            symm.append(p_terms[i])

    ntuned = len(p_terms) - len(symm)
    return symm


def cy_polys(symm_terms, pr):
    ks = len(symm_terms)
    n_polys = 2**ks
    lst = list(itertools.product([0,1], repeat=ks))

    polys = []
    base_poly = pr(0)
    for i in range(n_polys):
        v = lst[i]
        poly = pr(base_poly)
        for j in range(ks):
            if v[j] == 1:
                poly += pr(symm_terms[j])
        polys.append(poly)

    return polys


def symm(sigma, poly, pr):
    """Returns the part of the polynomial poly symmetric under the involution sigma"""
    spoly = inv(sigma, poly, pr)
    symm = (pr(poly) + pr(spoly)) / 2
    return symm


def asymm(sigma, poly, pr):
    """Returns the part of the polynomial poly antisymmetric under the involution sigma"""
    spoly = inv(sigma, poly, pr)
    asymm = (pr(poly) - pr(spoly)) / 2
    return asymm


def cs_moduli_tuned(sigma, poly, pr):
    """Returns the number of complex structure moduli that must be tuned to zero"""
    a = asymm(sigma, poly, pr)
    return len(a.dict())


def gcd(lst):
    """Computes the gcd of a list of numbers."""
    return reduce(fractions.gcd, lst)


def lcm(x, y):
    return (x * y) / gcd([x,y])


def lcmm(lst):
    lst = [w for w in lst if w != 0]
    return reduce(lcm, lst)


def reduced_vector(v):
    """Removes any common factor from a vector."""
    g = gcd(v)
    v = [int(x / g) for x in v]
    return v


def is_positive(v):
    x = True
    for vi in v:
        if vi <= 0:
            x = False
            break
    return x


def is_nonnegative(v):
    """Returns True if a vector has all nonnegative components, False otherwise."""
    v = list(v)
    x = True
    for vi in v:
        if vi < 0:
            x = False
            break
    return x


def is_nonpositive(v):
    """Returns True if a vector has all nonpositive components, False otherwise."""
    x = True
    for vi in v:
        if vi > 0:
            x = False
            break
    return x


def is_nonzero(v):
    """Returns True if a vector is nonzero, False otherwise."""
    x = False
    for vi in v:
        if vi != 0:
            x = True
            break
    return x


def count_substring(string, substring):
    """Counts how many times substring appears in string."""
    string = str(string)
    substring = str(substring)
    n = len(substring)
    m = len(string)
    count = 0
    for i in range(m-n+1):
        if string[i:i+n] == substring:
            count += 1
    return count


def polyhedron_solve(A, b):
    """Find all solutions to the matrix equation Ax=b, if a finite number exist."""
    A = matrix(A)
    (m, n) = (A.nrows(), A.ncols())

    zeros = [0 for i in range(n+1)]

    eqList = []
    for i in range(m):
        eq = []
        eq.append(int(-b[i]))
        for j in range(n):
            eq.append(int(A[i,j]))
        eqList.append(eq)

    ineqList = []
    for i in range(n):
        ineq = copy.deepcopy(zeros)
        ineq[i+1] = 1
        ineqList.append(ineq)

    sol = Polyhedron(eqns=eqList, ieqs=ineqList, base_ring=QQ)
    pts = sol.integral_points()
    pts = [list(w) for w in pts]
    return pts


def smith_solve(A, b):
    """Solve an not-full-rank (typically underdetermined) matrix equation of the form Ax = b for x."""
    Amat = matrix(A)
    (m, n) = (Amat.nrows(), Amat.ncols())
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    (D, L, R) = A.smith_form()
    c = np.dot(L, b)

    dcoeffs = []
    for i in range(m):
        row = D[i,:]
        row = list(list(row)[0])
        dcoeffs.append(row[i])

    isSol = True
    for i in range(m):
        if dcoeffs[i] == 0:
            pass
        elif c[i] % dcoeffs[i] == 0:
            pass
        else:
            isSol = False

    if not isSol:
        return None

    y = []
    for i in range(m):
        yv = int(c[i]/dcoeffs[i])
        y.append(yv)
    if n > m:
        for i in range(m, n):
            y.append(x[i-m])

        #xm = Ry will be our general solution set
        y = np.transpose(np.array(y))
        xm = np.dot(R, y)
        pr2 = PolynomialRing(base_ring=ZZ, names=normalize_names("x+", n - m))

        # We now need to find the vectors of the form xm such that all entries are nonnegative
        b = list(b)
        b = [w[0] for w in b]
        ineqList = []
        for i in range(len(xm)):
            expr = xm[i]
            ineq = [expr.constant_coefficient()]
            for j in range(m, n):
                ineq.append(expr.coefficient(x[j-m]))
            ineqList.append(ineq)

        zeros = [0 for i in range(m+1)]
        for i in range(n-m):
            vec = copy.deepcopy(zeros)
            vec[i+1] = 1
            ineqList.append(vec)
        #print(ineqList)

        sol = Polyhedron(ieqs=ineqList)
        pts = sol.integral_points()
        pts = [list(w) for w in pts]

        xsols = []
        for p in pts:
            xsol = []
            for i in range(len(xm)):
                xsol.append(pr2(xm[i])(p))
            xsols.append(xsol)

        return xsols
    else:
        xm = np.dot(R, y)
        return xm



def invariant_monomials(rwmat, sigma):
    """Computes the invariant monomials of the involution sigma with n factors to a term."""
    (m, n) = rwmat.shape
    k = len(sigma)
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()

    invars = []

    # Find the difference between charge columns of the divisors that are swapped
    ccols = []
    for swap in sigma:
        a = rwmat[:,swap[0]]
        b = rwmat[:,swap[1]]
        ccols.append([j - i for i, j in zip(a,b)])


    # Create the trivial invariant polynomials
    for swap in sigma:
        poly = x[swap[0]]*x[swap[1]]
        invars.append(poly)

    # To find all of the necessary generators for the nontrivial invariant monomials, loop over all subinvolutions
    for j in range(2, k+1):
        lst = list(itertools.combinations(range(k), j))
        lst = [list(w) for w in lst]
        for comb in lst:
            cols = [ccols[w] for w in comb]
            orthog_gens = []

            # The columns will all lie in a rank r subspace. Find the integral relations between these difference vectors
            # nz = np.nonzero(cols[0])[0].tolist()
            # c0 = cols[0][nz[0]]
            # c.append(c0)

            # for i in range(1, k):
            #     nz = np.nonzero(cols[i])[0].tolist()
            #     ci = cols[i][nz[0]]
            #     c.append(ci)

            ms = MatrixSpace(ZZ, j, m)
            cmat = ms(cols)
            cmat = cmat.T
            nsp = cmat.right_kernel()
            nsp = nsp.basis()
            nsp = [Matrix(list(w)) for w in nsp]
            if len(nsp) > 0:
                nsp = [w.transpose() for w in nsp]
                nsp = [w.tolist() for w in nsp]
                orthog_gens = [w[0] for w in nsp]

        
            # Create the nontrivial invariant polynomials using the generators of the orthogonal complement created above
            if len(orthog_gens) > 0:
                for gen in orthog_gens:
                    term1 = pr(1)
                    term2 = pr(1)
                    for i in range(j):
                        swap = sigma[comb[i]]
                        if gen[i] >=0:
                            i1 = swap[0]
                            i2 = swap[1]
                        else:
                            i1 = swap[1]
                            i2 = swap[0]
                            
                        g = int(abs(gen[i]))
                        term1 *= x[i1]**g
                        term2 *= x[i2]**g
                       
                    if term1 + term2 not in invars:
                        invars.append(term1 + term2)
                        invars.append(term1 - term2)

    return invars


def new_rwmat(rwmat, sigma, polys=None):
    """Finds the new reduced weight matrix after the Segre map."""
    (m, n) = rwmat.shape
    if polys == None:
        polys = invariant_monomials(rwmat, sigma)
    
    sigma_inds = sorted(list(itertools.chain.from_iterable(sigma)))
    keep_inds = list(set(range(n)) - set(sigma_inds))
    new_cols = []
    
    for poly in polys:
        nc = np.zeros(shape=m, dtype=int)
        pows = poly.dict().keys()[0]
        pows = list(pows)
        for i in range(len(pows)):
            nc += pows[i]*rwmat[:,i]
        new_cols.append(nc)
    new_cols = [list(w) for w in new_cols]

    q = 0
    qp = len(polys)
    k = len(sigma)
    lst = []
    new_n = n + qp - k
    ni = []
    for j in range(new_n):
        if j in keep_inds:
            lst.append(rwmat[:,j])
        else:
            if q < qp:
                lst.append(new_cols[q])
                q += 1
                ni.append(j)

    rw_new = np.transpose(np.array(lst))
    rwnFull = copy.deepcopy(rw_new)
    lst = [list(rw_new[i,:]) for i in range(m)]
    lst2 = sift(lst)
    ci = []
    li = []
    for i in range(m):
        if lst[i] in lst2 and lst[i] not in li:
            ci.append(i)
            li.append(lst[i])
    rw_new = np.matrix(lst2)
    return ci, ni, rw_new, rwnFull


def names_list(rwn, ni):
    (m, n) = rwn.shape
    old_inds = [x for x in range(n) if x not in ni]
    n1 = len(old_inds)
    n2 = n1 - n
    
    lst = []
    for i in range(n):
        if i in old_inds:
            lst.append('x' + str(i))
        else:
            lst.append('y' + str(i))

    if max(ni) >= n:
        for i in range(n, max(ni)+1):
            lst.append('y' + str(i))
    return lst



def defining_expression(rwmat, sigma, ni, polys=None):
    """Finds the defining equation among the list of new projective coordinates."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()
    
    def_exp = 0
    k1 = len(sigma)
    
    if polys == None:
        polys = invariant_monomials(rwmat, sigma)
    s = len(polys)
    k = int((s + 2) / 3)
    z = []

    ynames = ['y' + str(i) for i in ni]

    yr = PolynomialRing(base_ring=ZZ, names=ynames)
    y = yr.gens()
    
    for j in range(k - 1):
        yp = y[k + 2*j]
        ym = y[k + 2*j + 1]
        exps = polys[k + 2*j].dict().keys()[0]
        exps = list(exps)
        
        def_exp += yp**2 - ym**2
        t3 = y[0]**0
        for i in range(len(exps)):
            if exps[i] != 0:
                for s in range(k):
                    if pr.monomial_divides(x[i], polys[s]) and not yr.monomial_divides(y[s], t3):
                        t3 *= y[s]**exps[i]
                        
        def_exp -= 4*t3
        
    return def_exp
    
        
def anti_invariant_monomials(rwmat, sigma, polys, ni):
    """Returns the indices of the anti-invariant monomials corresponding to rwn and sigma."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))

    anti_inds = []
    for i in range(len(polys)):
        p = polys[i]
        if asymm(sigma, p, pr) == p:
            anti_inds.append(ni[i])

    return anti_inds


def sublist_of(lst1, lst2):
    """Returns True if lst1 is a sublist of lst2, False otherwise."""
    for j in lst1:
        if j not in lst2:
            return False
    return True


def valid_ff_exponents(lst):
    if len(lst) == 0:
        return False
    elif len(lst) == 1:
        return True
    
    m = gcd(lst)
    lst = [w for w in lst if w != m]
    for w in lst:
        if (w % m) != 0:
            return False
        elif int(w/m) % 2 != 1:
            return False
    return True


def check_odd(k):
    """Returns the odd integer to be checked."""
    return int(k / 2**even_power(k))


def fixed_flip_old(rwmat, ni, ai, polys, sr, rwn):
    """"Finds the fixed points of a CY under an involution that takes xi -> -xi for some set of coordinates xi. indices gives the indices of the flipped variables in the list of variables."""
    (m, n) = rwmat.shape
    (m2, n2) = rwn.shape
    names = names_list(rwmat, ni)
    srform = sr_bool(sr, n)

    pr1 = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))
    pr = PolynomialRing(base_ring=ZZ, names=names)
    x = pr.gens()
    x1 = pr1.gens()
    
    x2 = []
    for i in range(len(x)):
        if i in ni:
            j = ni.index(i)
            if j < len(polys):
                x2.append(polys[j])
        else:
            x2.append(x1[i])

    zi = [ai] # The trivial fixed set, xi=0 for all the flipped coordinates

    for i in range(m2):
        row = rwn[i,:].tolist()[0]
        zeroinds = [k for k in range(n2) if row[k] == 0]
        ainz = [w for w in ai if w not in zeroinds]
        aiz = [w for w in ai if w in zeroinds]
        q = len(ainz)

        for p in range(1, q+1):
            lst = list(itertools.combinations(ainz, p))
            for comb in lst:
                comb = list(comb)
                exps = [row[g] for g in comb]
                if valid_ff_exponents(exps):
                    kl = [row[j] for j in comb]
                    k = gcd(kl)

                    # Find the charged indices whose coordinates must be set to zero
                    if k != 0:
                        h = check_odd(k)
                        nfi = aiz + [w for w in ainz if w not in comb]
                        for j in range(n2):
                            z = rwn[i, j]
                            if (z*h % (2*k) != 0) and (j not in comb):
                                nfi.append(j)
                        nfi = list(set(nfi))
                        nfi = sorted(nfi)
                        zi.append(nfi)

    # Remove duplicates, if any
    zi2 = []
    for fs in zi:
        if fs not in zi2:
            zi2.append(fs)
    zi = zi2

    # Remove subset lists, if any
    lz = len(zi)
    remove = []
    for i in range(lz):
        for j in range(lz):
            if sublist_of(zi[i], zi[j]) and (i != j):
                remove.append(j)
    zi = [zi[i] for i in range(lz) if i not in remove]

    fsets = []
    fsetsx = []
    ziy = [[x[i] for i in f] for f in zi]
    zix = [[x2[i] for i in f] for f in zi]
    zis = [['(' + str(poly_to_bool2(str(positive_order(clear_exponents(w, n))),n)) + ')' for w in f] for f in zix]
    for i in range(len(zis)):
        ziform = propcalc.formula('&'.join(zis[i]))
        testform = propcalc.formula('(' + str(ziform) + ')&(' + str(srform) + ')')
        if not testform.is_contradiction():
            fsets.append(ziy[i])
            fsetsx.append(zix[i])

    return fsets, fsetsx


def fixed_swap(rwmat, sigma, ni, polys, sr):
    (m, n) = rwmat.shape
    k = len(sigma)
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()

    fixed_sets = []
    
    for i in range(k):
        swap = sigma[i]
        #fs_gens.append(x[swap[0]]**g - x[swap[1]]**g)
        fs = [swap[0], swap[1]]
        if fs not in sr:
            fixed_sets.append([x[swap[0]], x[swap[1]]])

    fixed_sets2 = []
    for f in fixed_sets:
        if f not in fixed_sets2:
            fixed_sets2.append(f)
    fixed_sets = fixed_sets2
        
    return fixed_sets
    


def general_poly_exps(rwmat, dd_charges, anti=True):
    """Finds the exponents for the most general polynomial consistent with the charges of the O7 plane."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()
    rwmat = np.array(rwmat) # Typecasting if necessary
    
    # Find the appropriate exponents via solving a Diophantine system via Smith normal form
    b = np.transpose(np.array([1*s for s in dd_charges]))
    cols = [rwmat[:,j].tolist() for j in range(n)]
    #cols = [[w[0] for w in cols[i]] for i in range(n)]
    Z = IntegerRing()
    rwm = matrix(Z, cols).T
    (D, L, R) = rwm.smith_form()
    c = np.dot(L, b)

    dcoeffs = []
    for i in range(m):
        row = D[i,:]
        row = list(list(row)[0])
        dcoeffs.append(row[i])
    
    is_sol = True
    for i in range(m):
        if dcoeffs[i] == 0:
            pass
        elif c[i] % dcoeffs[i] == 0:
            pass
        else:
            is_sol = False
    
    if not is_sol:
        return None
    
    y = []
    for i in range(m):
        yv = int(c[i]/dcoeffs[i])
        y.append(yv)
    if n > m:
        for i in range(m, n):
            y.append(x[i-m])
    
        # xm = Ry will be our general solution set
        y = np.transpose(np.array(y))
        xm = np.dot(R, y)
        pr2 = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n - m))

        # We now need to find the vectors of the form xm such that all entries are nonnegative
        ineq_list = []
        for i in range(len(xm)):
            expr = xm[i]
            ineq = [expr.constant_coefficient()]
            for j in range(m, n):
                ineq.append(expr.coefficient(x[j-m]))
            ineq_list.append(ineq)
        
        sol = Polyhedron(ieqs=ineq_list)
        pts = sol.integral_points()
        pts = [list(w) for w in pts]
        #pts = [w for w in pts if is_nonnegative_point(w)]
        
        xsols = []
        for p in pts:
            xsol = []
            for i in range(len(xm)):
                xsol.append(pr2(xm[i])(p))
            xsols.append(xsol)
        
        return xsols
    else:
        xm = np.dot(R, y)
        return xm
    
    
def general_poly(rwmat, dd_charges, exps=None, terms=False, pr=None):
    (m, n) = rwmat.shape
    if pr == None:
        pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()
    
    if exps == None:
        exps = general_poly_exps(rwmat, dd_charges)
    
    gen_terms = []
    for v in exps:
        term = 1
        for i in range(len(v)):
            term *= x[i]**v[i]
        gen_terms.append(term)
       
    if terms:
        return gen_terms

    gp = pr(0)
    for w in gen_terms:
        gp += pr(w)
        
    return gp
             
            
def is_nonnegative_point(pt):
    pos = True
    for w in pt:
        if w < 0:
            pos = False
    return pos


def positive_order(poly):
    """Returns the string of a two term invariant monomial, with the positive term given first (if there is a negative term)."""
    poly_str = str(poly)
    
    if '-' not in poly_str:
        return poly_str
    elif '+' not in poly_str:
        return poly_str
    elif poly_str.index('-') > poly_str.index('+'):
        return poly_str
    else:
        p_terms = poly_str.split('-')
        if len(p_terms) > 1:
            p_terms.pop(0)
        p_terms = p_terms[0].split('+')
        p2_str = p_terms[1] + '-' + p_terms[0]
        return p2_str


def clear_exponents(poly, n, pr=None):
    """Clears the exponents from a polynomial. Used when creating the new SR ideal after a coordinate change."""
    if pr == None:
        pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    names = pr.variable_names()
    d = poly.dict()
    d2 = {}
    
    for key in d.keys():
        nkey = list(key)
        for i in range(len(key)):
            if nkey[i] > 0 and nkey[i] != 1:
                nkey[i] = 1
        nkey = tuple(nkey)
        d2[nkey] = d[key]

    dnew = PolyDict(d2)
    npoly = dnew.poly_repr(names)
    return npoly



def poly_to_bool2(poly, n, bform=True):
    """Returns the boolean expression associated to a one- or two-term polynomial."""
    poly = positive_order(poly)
    p_str = str(poly)
    p_str = str(p_str)
    p_str = p_str.replace(' ', '')
    p_terms = p_str.split('+')
    for i in range(len(p_terms)):
        p_terms[i] = p_terms[i].split('-')
    p_terms = list(itertools.chain.from_iterable(p_terms))
    for i in range(len(p_terms)):
        s = p_terms[i]
        s = s.replace('*','|')
        s = '(' + s + ')'
        p_terms[i] = s
    p_str = "&".join(p_terms)
    
    if len(p_terms) > 1:
        vars_in = []
        for i in range(n):
            if str(i) in p_str:
                vars_in.append(i)

        var_cond = '(~('
        for j in range(len(vars_in)):
            var_cond += 'x' + str(vars_in[j])
            if j < len(vars_in) - 1:
                var_cond += '|'
        var_cond += '))'
        
        p_str = '(' + p_str + ')'
        p_str += '|' + var_cond

    if not bform:
        return p_str

    return propcalc.formula(p_str)


def poly_to_bool(poly, n, init=True):
    if init:
        poly = clear_exponents(poly, n)

    p_str = str(poly)
    p_terms = p_str.split('+')
    for i in range(len(p_terms)):
        p_terms[i] = p_terms[i].split('-')
    p_terms = list(itertools.chain.from_iterable(p_terms))
    p_terms = [s.strip() for s in p_terms]
    if '' in p_terms:
        p_terms.remove('')

    b_strs = []

    k = len(p_terms)
    for i in range(k+1):
        for comb in itertools.combinations(p_terms, i):
            comb = list(comb)
            t1 = comb
            t2 = [w for w in p_terms if w not in comb]

            if len(t1) > 2:
                t11s = str(poly_to_bool2(t1[0], n, False))
                t1.pop(0)
                t12s = str(poly_to_bool('+'.join(t1), n, False))
                t1s = '(' + t11s + '&' + t12s + ')|(~(' + t11s + '|' + t12s + '))'
            elif len(t1) == 1:
                t1s = poly_to_bool2(t1[0], n, False)
            elif len(t1) == 2:
                t11s = '(' + poly_to_bool2(t1[0], n, False) + ')'
                t12s = '(' + poly_to_bool2(t1[1], n, False) + ')'
                t1s = '(' + t11s + '&' + t12s + ')|(~(' + t11s + '|' + t12s + '))'
            else:
                t1s = ''

            if len(t2) > 2:
                t21s = str(poly_to_bool2(t2[0], n, False))
                t2.pop(0)
                t22s = str(poly_to_bool('+'.join(t2), n, False))
                t2s = '(' + t21s + '&' + t22s + ')|(~(' + t21s + '|' + t22s + '))'
            elif len(t2) == 1:
                t2s = poly_to_bool2(t2[0], n, False)
            elif len(t2) == 2:
                t21s = '(' + poly_to_bool2(t2[0], n, False) + ')'
                t22s = '(' + poly_to_bool2(t2[1], n, False) + ')'
                t2s = '(' + t21s + '&' + t22s + ')|(~(' + t21s + '|' + t22s + '))'
            else:
                t2s = ''

            if t1s != '' and t2s != '':
                v = '(' + t1s + '&' + t2s + ')|(~(' + t1s + '|' + t2s + '))'
            elif t1s == '':
                v = '(' + t2s + ')'
            else:
                v = '(' + t1s + ')'
            b_strs.append(v)


    b_str = ''
    for i in range(len(b_strs)):
        b_str += '(' + b_strs[i] + ')'
        if i != len(b_strs) - 1:
            b_str += '|'

    return propcalc.formula(b_str)


def charge_vector(poly, rwmat, pr=None):
    """Returns the charge vector corresponding to a given polynomial."""
    (m, n) = rwmat.shape
    p_str = str(poly)

    p_terms = p_str.split('+')
    for i in range(len(p_terms)):
        p_terms[i] = p_terms[i].split('-')
    p_terms = list(itertools.chain.from_iterable(p_terms))

    mon = p_terms[0]
    if pr == None:
        pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))
    npoly = pr(p_str)
    pd = npoly.dict()
    key = list(pd.keys()[0])

    charges = []
    for i in range(m):
        charge = 0
        for j in range(n):
            charge += rwmat[i,j]*key[j]
        charges.append(charge)

    return charges


def sectors(sr, pr):
    """Creates the sectors that are used in the codimension calculation."""
    q = len(sr)
    n = pr.ngens()
    sectors = []
    for i in range(1, q+1):
        sectors += list(itertools.combinations(range(n), i))

    # Keep only the sets that are contain at least one element from each SR ideal
    sectors2 = []
    for sec in sectors:
        keep = True
        for j in range(q):
            if len(set.intersection(set(sec), set(sr[j]))) == 0:
                keep = False
                break
        if keep:
            sectors2.append(sec)

    sectors = sectors2

    # Remove sublists
    remove = []
    for i in range(len(sectors)):
        for j in range(len(sectors)):
            if sublist_of(sectors[i], sectors[j]) and i != j:
                remove.append(j)

    sectors = [sectors[i] for i in range(len(sectors)) if i not in remove]

    # Create the polynomials
    x = pr.gens()
    gensets = []
    for sector in sectors:
        sector = sorted(sector)
        pgens = []
        for num in sector:
            pgens.append(pr(x[num] - 1))
        gensets.append(pgens)

    return gensets


def new_sr_ideal(rwmat, sigma, polys, sr, ni):
    """Returns the new SR ideal after the coordinate change"""
    (m, n) = rwmat.shape
    k = len(sigma)
    q = len(sr)
    r = len(polys)

    # Create variables to use for boolean logic
    vars = []
    for i in range(n):
        vars.append('x' + str(i))
    
    sr_txt = []
    for i in range(q):
        x = []
        for j in sr[i]:
            x.append(vars[j])
        sr_txt.append(x)

    # Create the logical formula for the original SR ideal
    sr_form = []
    for i in range(q):
        f_str = "(~("
        for j in range(len(sr_txt[i])):
            f_str += sr_txt[i][j]
            if j != len(sr_txt[i]) - 1:
                f_str += "&"
        f_str += "))"
        sr_form.append(f_str)
    f_str = '&'.join(sr_form)
    sr_form = propcalc.formula(f_str)

    # Clear any exponents from the invariant monomials, since the SR ideal is square-free
    polys_orig = copy.deepcopy(polys)
    polys = [clear_exponents(t, n) for t in polys]
    

    # Convert the invariant monomials to logical statements
    # Plus and minus go to and
    # Times goes to or
    logic_repl = []
    polys_txt = [str(x) for x in polys]
    
    for i in range(len(polys_txt)):
        w = polys_txt[i]
        w = w.replace(" ","") # Remove any whitespace
        w = positive_order(w)
        
        # Each polynomial will contain either one plus, one minus, or neither one
        # A leading minus messing this up has been accounted for with positive_order
        if '-' in w or '+' in w:
            if '-' in w:
                w = w.split('-')
                ch = '-'
            else:
                w = w.split('+')
                ch = '+'
            
            for i in range(len(w)):
                w[i] = '(' + w[i].replace('*', '|') + ')'
            w = w[0] + '&' + w[1]
            
            # Determine which toric variables appear in the polynomial
            vars_in = []
            for j in range(n):
                if str(j) in w:
                    vars_in.append(j)
            
            # Add the condition taking into account the new variable from being zero even when none of the x's are
            var_cond = '(~('
            for j in range(len(vars_in)):
                var_cond += 'x' + str(vars_in[j])
                if j < len(vars_in) - 1:
                    var_cond += '|'
            var_cond += '))'
            w += '|' + var_cond
            
        else:
            w = w.replace('*', '|')
        
        logic_repl.append(propcalc.formula(w))

    # The formulas corresponding to the variables used after the Segre map
    new_var_form = []
    j = 0
    new_n = n + r - 2*k
    for i in range(new_n):
        if i in ni:
            new_var_form.append(logic_repl[j])
            j += 1
        else:
            new_var_form.append(propcalc.formula('x' + str(i)))
    
    if j < len(logic_repl) - 1:
        new_var_form.append(logic_repl[j:])

    max_order = max(n, max(ni)) - 1

    # Find the new SR ideal formulas
    new_sr = []
    new_sry = []
    n2 = len(new_var_form)
    for i in range(1, max_order+1):
        lst = list(itertools.combinations(range(n2), i))
        for j in range(len(lst)):
            subset_inds = lst[j]
            subset = [new_var_form[w] for w in subset_inds]

            form_str = '~('
            for v in range(len(subset)):
                form_str += '(' + str(subset[v]) + ')'
                if v != len(subset) - 1:
                    form_str += '&'
            form_str += ')'
            form = propcalc.formula(form_str)

            # Keep only the formulas that are valid consequences of the original SR formula
            # Then construct the corresponding formula in terms of the ys
            if propcalc.valid_consequence(form, sr_form):
                new_sr.append(form)
                nform = '~('
                for c in range(len(subset_inds)):
                    ind = subset_inds[c]
                    if ind in ni:
                        nform += 'y' + str(ind)
                    else:
                        nform += 'x' + str(ind)
                    if c != len(subset_inds) - 1:
                        nform += '&'
                nform += ')'
                new_sry.append(propcalc.formula(nform))

    # These will be used later to reconvert the SR ideal into variable form
    names = names_list(rwmat, ni)
    pr = PolynomialRing(base_ring=ZZ, names=names)
    prx = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))
    x = pr.gens()

    # Re-sort the new SR ideal from longest to shortest, in terms of number of factors
    new_sry_txt = [str(w) for w in new_sry]
    new_sry_len = [len(w) for w in new_sry_txt]
    sort_inds = np.argsort(new_sry_len)
    new_sry_txt2 = []
    new_sr2 = []
    for i in range(len(sort_inds)):
        new_sr2.append(new_sr[sort_inds[i]])
        new_sry_txt2.append(new_sry_txt[sort_inds[i]])
    new_sr = new_sr2
    new_sry_txt = new_sry_txt2
    new_sry = [propcalc.formula(f) for f in new_sry_txt]

    new_sr_txt = [str(w) for w in new_sr]

    # Reduce down to the generating set
    keep_inds = []
    nn = len(new_sr)
    keep_inds = [0]
    for i in range(1, nn):
        if new_sr[i] == new_sr[0]:
            keep_inds.append(i)
        else:
            break

    start = max(keep_inds) + 1
    for i in range(start, nn):
        lst = copy.deepcopy(new_sr)
        lst_inds = [j for j in range(nn) if lst[j] == lst[i]]
        lst_inds = [j for j in range(nn) if j not in lst_inds]
        lst = [lst[j] for j in lst_inds if j < i]

        if propcalc.valid_consequence(new_sr[i], *lst) == False:
            keep_inds.append(i)
    new_sry = [new_sry[i] for i in keep_inds]

    # Reformat the new SR into variable form and cast as polynomial objects
    new_sry_txt = [str(w) for w in new_sry]
    for i in range(len(new_sry_txt)):
        new_sry_txt[i] = new_sry_txt[i].replace("~", "")
        new_sry_txt[i] = new_sry_txt[i].replace("&", "*")
        new_sry_txt[i] = new_sry_txt[i].replace('(', '')
        new_sry_txt[i] = new_sry_txt[i].replace(')', '')

    pri = PolynomialRing(QQ, names=names)
    new_sry = [pri(w) for w in new_sry_txt]
    Isr = pri.ideal(new_sry)
    minbase = singular_function('minbase')
    new_sry = minbase(Isr)
    new_sry = [pr(w) for w in new_sry]
    return new_sry


def basis_decomposition(rwmat, basis_inds, charges):
    """Decomposes the D_D charges in terms of the charge vectors of the cohomology basis."""
    (m, n) = rwmat.shape
    #dd_charges = np.transpose(np.array([4*s for s in o7_charges]))
    rw_cols = []
    for j in range(n):
        col = rwmat[:,j]
        col = col.tolist()
        rw_cols.append(col)
    
    basis_cols = [rw_cols[i] for i in basis_inds]
    basis_mat = np.transpose(np.array(basis_cols))
    
    vals = np.linalg.solve(basis_mat, charges)
    vals = [int(w) for w in vals]
    
    return vals


def partitions(vals):
    """Returns the set of all partitions of the given set of integers."""
    pars = []
    pnums = []
    total_num = 1
    for i in range(len(vals)):
        x = Partitions(vals[i])
        w = x.cardinality()
        pars.append(x)
        pnums.append(w)
        total_num *= w
    pars = [list(p) for p in pars]
    
    return list(itertools.product(*pars))
   
    
def cy_polynomial(nverts, dresverts):
    """Constructs the polynomial defining the CY hypersurface from the Newton and resolved dual polytope vertices."""
    k = len(dresverts)
    p = len(nverts)
    
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', k))
    x = pr.gens()
    
    dtope = LatticePolytope(nverts)
    pts = dtope.points()
    pts = [list(w) for w in pts]
    #pts.remove([0,0,0,0])
    
    q = len(pts)
    cy_terms = []
    for i in range(q):
        term = pr(1)
        pt = np.array(pts[i])
        for j in range(k):
            dpt = np.transpose(np.array(dresverts[j]))
            g = np.dot(pt, dpt) + 1
            term *= x[j]**g
        cy_terms.append(term)
    
    cy_poly = pr(0)
    for i in range(q):
        cy_poly += cy_terms[i]
        
    return cy_poly


def sr_text(sr, n, pr=None):
    q = len(sr)
    if pr == None:
        pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))

    # Create variables to use for boolean logic
    var = list(pr.variable_names())
    
    sr_txt = []
    for i in range(q):
        x = []
        for j in sr[i]:
            x.append(var[j])
        sr_txt.append(x)

    return sr_txt


def sr_bool(sr, n):
    q = len(sr)
    sr_txt = sr_text(sr, n)
    sr_form = []
    for i in range(q):
        f_str = "(~("
        for j in range(len(sr_txt[i])):
            f_str += sr_txt[i][j]
            if j != len(sr_txt[i]) - 1:
                f_str += "&"
        f_str += "))"
        sr_form.append(f_str)
    f_str = '&'.join(sr_form)
    sr_form = propcalc.formula(f_str)

    return sr_form


def sr_inds(polys, pr):
    n = pr.ngens()
    x = pr.gens()
    sr_inds = []
    polys = [pr(p) for p in polys]
    for i in range(len(polys)):
        s = []
        for j in range(n):
            if pr.monomial_divides(x[j], polys[i]):
                s.append(j)
        sr_inds.append(s)

    return sr_inds


def clear_coefficients(poly, pr):
    d = poly.dict()
    for key in d.keys():
        d[key] = 1
    return pr(d)



def poly_ytox(ypoly, polys, prx, pry, ni):
    nx = prx.ngens()
    ny = pry.ngens()
    x = prx.gens()
    y = pry.gens()

    # Create a polynomial ring which contains both the xs and ys as variables
    # This will be used for substituting in the invariant monomial expressions for the ys
    xvars = list(prx.variable_names())
    yvars = list(pry.variable_names())
    xyvars = xvars
    for var in yvars:
        if var not in xyvars:
            xyvars.append(var)

    prxy = PolynomialRing(base_ring=CC, names=xyvars)
    ypoly = prxy(ypoly)

    pt = []
    for i in range(nx):
        pt.append(x[i])
    for i in range(ny):
        if i in ni:
            pt.append(polys[ni.index(i)])

    return prx(ypoly(pt))


def fset_reduced_GGs(pr, fset, sr, cypoly, polys, ni, rwn=None):
    n = pr.ngens()
    x = pr.gens()
    srtxt = sr_text(sr, n, pr)
    srpolys = [pr('*'.join(w)) for w in srtxt]
    polys = [pr(p) for p in polys]
    k = len(fset)
    q = len(sr)

    # For the gauge group calculation, everything is already given in terms of the x's
    fsetx = fset

    # Check to see whether or not each element of the fixed set is trivially redundant
    for i in range(k):
        p = pr(fsetx[i])
        idp = pr.ideal(p)
        if cypoly.reduce(idp) == 0:
            fset2 = copy.deepcopy(fset)
            fset2.pop(i)
            return fset_reduced(pr, fset2, sr, cypoly, polys, ni, rwn)

    # if k < 2:
    #     return fsetx, fset

    bases = []
    secs = sectors(sr, pr)
    cyset = fsetx + [cypoly]
    for sec in secs:
        gset = cyset + sec
        ig = pr.ideal(gset)
        bases.append(ig.groebner_basis())
        #print(ig.groebner_basis())
    
    prz = PolynomialRing(base_ring=ZZ, names=pr.variable_names())
    good = False
    for base in bases:
        if base != [pr(1)]:
            good = True
            #print("not 1")
            break
    if not good:
        return None, None
    #print("=====")

    dims = [len(base) for base in bases]

    for i in range(k):
        lst = list(itertools.combinations(fsetx, i))
        for comb in lst:
            comb = list(comb)
            genset = [cypoly] + comb
            test = True
            for i in range(len(secs)):
                sec = secs[i]
                gset = genset + sec
                #print(gset)
                igen = pr.ideal(gset)
                d = len(igen.groebner_basis())
                if d != dims[i]:
                    test = False
                    break
            if test:
                return fsetx, comb

    return fsetx, fsetx

def fset_reduced(pr, fset, sr, cypoly, polys, ni, rwn=None):
    n = pr.ngens()
    x = pr.gens()
    srtxt = sr_text(sr, n, pr)
    srpolys = [pr('*'.join(w)) for w in srtxt]
    polys = [pr(p) for p in polys]
    k = len(fset)
    q = len(sr)

    if rwn != None:
        # Convert the fixed set (given in terms of the ys) into expressions in terms of x
        fstrs = [positive_order(w).strip() for w in fset]
        fsetx = []
        for i in range(k):
            fs = fstrs[i]
            ind = int(fs[1:])
            if fs[0] == 'y':
                fsetx.append(polys[ni.index(ind)])
            else:
                fsetx.append(x[ind])
    else:
        fsetx = fset

    # Check to see whether or not each element of the fixed set is trivially redundant
    for i in range(k):
        p = fsetx[i]
        idp = pr.ideal(p)
        if cypoly.reduce(idp) == 0:
            fset2 = copy.deepcopy(fset)
            fset2.pop(i)
            return fset_reduced(pr, fset2, sr, cypoly, polys, ni, rwn)

    # if k < 2:
    #     return fsetx, fset

    bases = []
    secs = sectors(sr, pr)
    cyset = fsetx + [cypoly]
    for sec in secs:
        gset = cyset + sec
        ig = pr.ideal(gset)
        bases.append(ig.groebner_basis())
        #print(ig.groebner_basis())
    
    prz = PolynomialRing(base_ring=ZZ, names=pr.variable_names())
    good = False
    for base in bases:
        if base != [pr(1)]:
            good = True
            #print("not 1")
            break
    if not good:
        return None, None
    #print("=====")

    dims = [len(base) for base in bases]

    for i in range(k):
        lst = list(itertools.combinations(fsetx, i))
        for comb in lst:
            comb = list(comb)
            genset = [cypoly] + comb
            test = True
            for i in range(len(secs)):
                sec = secs[i]
                gset = genset + sec
                #print(gset)
                igen = pr.ideal(gset)
                d = len(igen.groebner_basis())
                if d != dims[i]:
                    test = False
                    break
            if test:
                return fsetx, comb

    return fsetx, fsetx


def primes(n):
    """Note that the list only includes odd primes"""
    plist = [3]
    num = 3
    while len(plist) < n:
        num += 1
        prime = True
        for j in range(len(plist)):
            if num % plist[j] == 0:
                prime = False
                break
        if prime:
            plist.append(num)

    return plist


def cy_charges(rwmat):
    """Finds the total charge vector for the CY hypersurface."""
    (m, n) = rwmat.shape
    charges = []
    for i in range(m):
        charge = 0
        for j in range(n):
            charge += rwmat[i, j]
        charges.append(charge)
    return charges


def fset_inds(fset, pr):
    fset = [pr(f) for f in fset]
    x = pr.gens()

    fset_inds = []
    for f in fset:
        if f in x:
            fset_inds.append(x.index(f))

    return fset_inds


def o7_charges_old(rwmat, o7_red, pr):
    """Finds the total charge of the O7 planes. Note that the o7 list must be given in terms of the reduced fsets."""
    (m, n) = rwmat.shape
    if type(o7_red) == list:
        o7_red = [w[0] for w in o7_red]
    o7_inds = [fset_inds([o7], pr)[0] for o7 in o7_red]
    charges = [rwmat[:,j].tolist() for j in range(n)]
    charges = [charges[i] for i in o7_inds]

    o7_charges = []
    for i in range(m):
        val = 0
        for c in charges:
            val += c[i]
        o7_charges.append(val)

    return o7_charges


def o7_charges(rwmat, o7red):
    """Finds the total charge of the O7 planes. Note that the o7 list must be given in terms of the reduced fsets."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=CC, names=normalize_names('x+', n))
    vecs = []
    for o7 in o7red:
        vecs.append(charge_vector(o7[0], rwmat))

    if vecs == []:
        return []

    p = len(vecs[0])
    o7Charges = [0 for i in range(p)]
    for vec in vecs:
        for i in range(p):
            o7Charges[i] += vec[i]

    return o7Charges


def terms(poly):
    """Returns the terms of a polynomial."""
    p_str = str(poly)
    p_str = p_str.strip()
    p_terms = p_str.split('+')
    for i in range(len(p_terms)):
        p_terms[i] = p_terms[i].split('-')
    return list(itertools.chain.from_iterable(p_terms))


def nterms(poly):
    """Returns the number of terms of a polynomial."""
    return len(terms(poly))


def max_exponents(poly, pr):
    """Returns the maximum exponents appearing in the polynomial for each coordinate in the polynomial ring."""
    poly = pr(poly)
    d = poly.dict()
    n = pr.ngens()

    exps = [[] for i in range(n)]
    key_list = d.keys()
    for key in key_list:
        key = list(key)
        for i in range(len(key)):
            exps[i].append(key[i])

    return [max(w) for w in exps]


def subfactors(key):
    key = list(key)
    n = len(key)
    ranges = []
    for i in range(n):
        ranges.append(range(key[i]+1))
    lst = list(itertools.product(*ranges))
    lst.pop(0)
    return [tuple(w) for w in lst]


def nonzero_elements(t):
    """Returns the number of nonzero terms of the tuple or list t."""
    t = list(t)
    nz = 0
    for h in t:
        if h != 0:
            nz += 1
    return nz

def sigma_charges(rwmat, sigma):
    """Computes the action of the involution on the cohomology basis charge vectors."""
    (m, n) = rwmat.shape
    si = [w for b in sigma for w in b]
    rwmat = np.array(rwmat) # Typecasting if necessary
    charges = [rwmat[:,j].tolist() for j in range(n)]

    # Find the images of the divisors under the involution
    images = []
    for i in range(n):
        if i not in si:
            images.append(charges[i])
        else:
            for swap in sigma:
                if i in swap:
                    if i == swap[0]:
                        images.append(charges[swap[1]])
                    else:
                        images.append(charges[swap[0]])
    return images


def sigma_basis_charges(rwmat, sigma, bi):
    """Computes the action of the involution on the cohomology basis charge vectors."""
    (m, n) = rwmat.shape
    si = [w for b in sigma for w in b]
    rwmat = np.array(rwmat) # Typecasting if necessary

    # Find the images of the basis divisors under the involution
    bi_sigma = []
    for i in bi:
        if i not in si:
            bi_sigma.append(i)
        else:
            for swap in sigma:
                if i in swap:
                    if i == swap[0]:
                        bi_sigma.append(swap[1])
                    else:
                        bi_sigma.append(swap[0])
                    break

    # Find the charge vectors for the transformed basis divisors
    charges = [rwmat[:,j].tolist() for j in range(n)]
    charges = [charges[i] for i in bi_sigma]

    return charges


def sigma_basis(rwmat, sigma, bi, bs_charges=None):
    """Computes the action of the involution on the cohomology basis."""
    if bs_charges == None:
        bs_charges = sigma_basis_charges(rwmat, sigma, bi)

    # Find the basis decompositions of the transformed charge vectors
    bds = [basis_decomposition(rwmat, bi, bs_charges[i]) for i in range(len(bs_charges))]
    return bds


def tadpole_cancellation_basis(rwmat, sigma, bi, sigma_charges=None):
    (m, n) = rwmat.shape
    si = [w for b in sigma for w in b]
    if sigma_charges == None:
        sigma_charges = sigma_basis_charges(rwmat, sigma, bi)

    # Find the charge vectors for the basis divisors
    charges = [rwmat[:,j].tolist() for j in range(n)]
    charges = [charges[i] for i in bi]

    # The basis decompositions for the transformed charge vectors
    bds = sigma_basis(rwmat, sigma, bi)

    tcb = []
    for i in range(len(charges)):
        if bi[i] in si:
            vec = [(charges[i][j] + sigma_charges[i][j]) for j in range(m)]
        else:
            vec = charges[i]
        tcb.append(vec)
    return tcb


def read_inv_data(data_file="Involution_Data.txt"):
    """Reads the involution data from the text file."""
    data = []

    with open(data_file) as f:
        while True:
            try:
                data.append(eval(f.readline()))
            except EOFError:
                break

    return data


def sigma_list(lst, sigma):
    lst2 = copy.deepcopy(lst)
    for swap in sigma:
        lst2[swap[0]] = lst[swap[1]]
        lst2[swap[1]] = lst[swap[0]]
    return lst2


def fixed_flip(rwold, ni, ai, polys, sr, rwmat):
    """"Finds the fixed points of a CY under an involution that takes xi -> -xi for some set of coordinates xi. ai gives the indices of the flipped variables in the list of variables."""
    (m, n) = rwmat.shape
    (m0, n0) = rwold.shape
    names = names_list(rwold, ni)
    srform = sr_bool(sr, n0)

    pr1 = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n0))
    pr = PolynomialRing(base_ring=ZZ, names=names)
    x = pr.gens()
    x1 = pr1.gens()
    
    x2 = []
    for i in range(len(x)):
        if i in ni:
            j = ni.index(i)
            if j < len(polys):
                x2.append(polys[j])
        else:
            x2.append(x1[i])

    # Create the vector of +/- 1 depending on the involution
    sv = [(1-2*int(j in ai)) for j in range(n)]

    # Create the vector of the appropriate congruence values
    #bv = np.transpose(np.array([int((1-sv[j])/2) for j in range(n)]))
    bl = [int((1-sv[j])/2) for j in range(n)]
    #bv = matrix(bl).T

    # Create the upper bounds for the yi
    # If we use a fundamental phase domain of [0, 2pi], then the lower bounds are all zero
    ub = []
    for j in range(n):
        u = 0
        for i in range(m):
            u += rwmat[i,j]
        ub.append(u)

    rwcols = [rwmat[:,j].tolist()[0] for j in range(n)]
    zi = [ai] # The trivial fixed set
    nf = []

    for i in range(1,n+1):
        lst = list(itertools.combinations(range(n), i))
        for comb in lst:
            # The indices in comb are the indices that are nonzero
            comb = list(comb)
            z = [j for j in range(n) if j not in comb] # The zero indices

            # See if we can skip checking this fixed set
            test = False
            for nfset in nf:
                if sublist_of(z, nfset):
                    test = True
                    break
            if test:
                continue
            
            bv = [bl[c] for c in comb]
            uv = [ub[c] for c in comb]
            nLattice = list(itertools.product(*[range(u) for u in uv]))
            notfixed = True
            for latPt in nLattice:
                latPt = list(latPt)
                cols = [rwcols[c] for c in comb] 
                bvL = [bv[k]+2*latPt[k] for k in range(i)]
                mat = np.transpose(np.array(cols))
                cols2 = []
                for k in range(i):
                    cols2.append(cols[k] + [bvL[k]])
                mat2 = np.transpose(np.array(cols2))
                if np.linalg.matrix_rank(mat) == np.linalg.matrix_rank(mat2):
                    zi.append(z)
                    notfixed = False
                    break

            if notfixed:
                nf.append(z)

    # Remove duplicates, if any
    zi2 = []
    for fs in zi:
        if fs not in zi2:
            zi2.append(fs)
    zi = zi2

    # Remove subset lists, if any
    lz = len(zi)
    remove = []
    for i in range(lz):
        for j in range(lz):
            if sublist_of(zi[i], zi[j]) and (i != j):
                remove.append(j)
    zi = [zi[i] for i in range(lz) if i not in remove]

    # Convert the fixed set indices to coordinate representation
    # Also remove any sets that do not intersect the CY via Boolean algebra
    fsets = []
    fsetsx = []
    ziy = [[x[i] for i in f] for f in zi]
    zix = [[x2[i] for i in f] for f in zi]
    zis = [['(' + str(poly_to_bool2(str(positive_order(clear_exponents(w, n0))),n0)) + ')' for w in f] for f in zix]
    for i in range(len(zis)):
        ziform = propcalc.formula('&'.join(zis[i]))
        testform = propcalc.formula('(' + str(ziform) + ')&(' + str(srform) + ')')
        if not testform.is_contradiction():
            fsets.append(ziy[i]) 
            fsetsx.append(zix[i])

    return fsets, fsetsx


def matrix_reduce_np(mat):
    (m, n) = mat.shape
    cols = [mat[:,i].tolist() for i in range(n)]
    newCols = sift(cols)
    return np.transpose(np.array(newCols))


def matrix_reduce(mat):
    n = mat.ncols()
    cols = mat.columns()
    cols = [list(w) for w in cols]
    newCols = sift(cols)
    return matrix(newCols)


def gauge_group_config(rwmat, sigma, o7Charges, fsets, config):
    """
    Computes the gauge group corresponding to a given (toric divisor) configuration.
    This function assumes that the given configuration is one that satisfies the tadpole cancellation condition.
    """
    
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))
    case = divisor_case_torics(rwmat, config, o7Charges, fsets, pr)-1
    nums = [1,2,2]

    # The possible gauge groups
    ggs = ['U', 'SP', 'SO'] # May need to flip the last two

    gg = []
    gtype = ggs[case]
    num = nums[case]
    for j in range(len(config)):
        if config[j] != 0:
            gg.append(gtype + '(' + str(num*config[j]) + ')')

    return 'x'.join(gg)


#def gauge_groups_torics(polyid, geonum, trinum, involnum, h11, rwmat, sigma, o7s, bi):
def gauge_groups_torics(rwmat, sigma, o7s, bi, itens):
    """Computes the gauge groups."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))

    o7Charges = o7_charges(rwmat, o7s)
    ddCharges = [8*w for w in o7Charges]
    sigmaCols = sigma_charges(rwmat, sigma)
    rwCols = [rwmat[:,j].tolist() for j in range(n)]
    newCols = []

    for j in range(n):
        col = []
        for i in range(m):
            col.append(rwCols[j][i] + sigmaCols[j][i])
        newCols.append(col)
    xsMat = matrix(newCols).T
    
    # The configurations that cancel the DO7 charge
    configs = polyhedron_solve(xsMat, ddCharges)

    # To avoid redundancy, we need to remove orientifold image pairs from configs
    configs2 = []
    for cf in configs:
        if cf in configs2 or sigma_list(cf, sigma) in configs2:
            pass
        else:
            configs2.append(cf)
    configs = configs2

    # The possible gauge groups and number multipliers
    ggTypes = ['U', 'SP', 'SO']
    numCase = [1,2,2]

    # The dictionary keys
    #polyidkey = "POLYID"
    #geokey = "GEOMN"
    #trikey = "TRIANGN"
    #involkey = "INVOLN"
    #h11key = "H11"
    typekey = "GTYPE"
    degkey = "DEG"
    rankkey = "RANK"
    #monindkey = "MONOMIND"
    #monomskey = "MONOMIALS"
    monomkey = "MONOM"
    multkey = "MULT"
    #gpindkey = "GROUPIND"
    gpskey = "GGROUPS"
    fwkey = "FW"
    
    # Put the ID numbers into a dictionary
    #idDict = {polyidkey : int(polyid), geokey : int(geonum), trikey : int(trinum), involkey : int(involnum), h11key : int(h11)}
    idDict = {}

    # Some calculations for the Freed-Witten check
    # These two values are the same across the entire involution, so cheaper to only have to calculate them once
    # b is the basis decomposition of the Calabi-Yau cohomology class
    # c is the matrix that gives the basis decomposition of the toric divisors
    cyCharges = cy_charges(rwmat)
    b = basis_decomposition(rwmat, bi, cyCharges)
    rwcols = [rwmat[:,j].tolist() for j in range(n)]
    c = [basis_decomposition(rwmat, bi, rwc) for rwc in rwcols]

    # print("b:", b)
    # print("c:",c)
    # print("config:", configs[0])

    # Now, let's calculate which toric divisors need fluxes
    # For the extremal/maximal brane case, this will be enough, since every brane will be wrapped on a toric divisor
    fws = []
    zeros = [0 for j in range(n)]
    for j in range(n):
        a = copy.deepcopy(zeros)
        a[j] = 1
        fws.append(freed_witten(a, bi, rwmat, itens))
    fwDivisors = [j for j in range(n) if not fws[j]] # The divisors that need fluxes

    # print("FW divs:", fwDivisors)

    #mon = []
    ggList = []
    ggDictList = []
    for i in range(len(configs)):
        gtypes = []
        nums = []
        ggs = []
        config = configs[i]
        poly = str(pr({tuple(config) : 1}))
        #mon.append(poly)

        # Put the configuration-dependent information into the dictionary
        configDict = copy.deepcopy(idDict)
        #configDict[monindkey] = int(i+1)
        configDict[monomkey] = str(poly_index_raiser(poly, pr))

        # See if the Freed-Witten condition is satisfied
        sconfig = sigma_list(config, sigma)
        nzi = [int(w) for w in np.nonzero(config)[0]]
        snz =[int(w) for w in np.nonzero(sconfig)[0]]
        fw = [x for x in fwDivisors if (x in nzi or x in snz)] # The divisors in the configuration (or its orientifold image) that need fluxes
        configDict[fwkey] = fw

        # Do the first loop to get the data for each brane in the configuration
        # This loop is done first so we can get the multiplicities
        for j in range(len(config)):
            if config[j] != 0:
                cf = [0 for p in range(n)]
                cf[j] = config[j]
                case = divisor_case_torics(rwmat, sigma, cf, o7s, pr)-1
                gtype = ggTypes[case]
                num = numCase[case]*config[j]
                gtypes.append(gtype)
                nums.append(num)
                ggs.append(gtype + '(' + str(num) + ')')

        # Get the multiplicities
        ggCount = Counter(ggs)
        mults = []
        for k in range(len(ggs)):
            mults.append(ggCount[ggs[k]])

        # Get the reduced version of the list
        used = []
        ggsList = []
        w = 1
        for k in range(len(ggs)):
            # Skip if this gauge group has already been covered
            if ggs[k] in used:
                continue
            ggsList.append([gtypes[k], nums[k], mults[k], ggs[k]])
            w += 1
            used.append(ggs[k])

        # Add these gauge groups to the overall list, if necessary
        for grp in used:
            if grp not in ggList:
                ggList.append(grp)

        ggList = sorted(ggList)

        # Now create a dictionary for each factor group in the configuration
        for j in range(len(ggsList)):
            ggDict = copy.deepcopy(configDict)
            [gtype, num, mult, grp] = ggsList[j]
            ggDict[typekey] = str(gtype)
            ggDict[multkey] = int(mult)
            ggDict[degkey] = int(num)
            #ggDict[gpindkey] = int(ggList.index(grp))
            ggDict[rankkey] = int(gprank(gtype, num))
	    #ggDict = json.dumps(ggDict,separators=(',',':'))
        ggDictList.append(ggDict)

    #mon = [str(poly_index_raiser(w, pr)) for w in mon]
    #monDict = {polyidkey : polyid, geokey : geonum, trikey : trinum, involkey : involnum}
    #monDict[monomskey] = mon
    #monDict[gpskey] = ggList

    #return monDict, ggDictList
    return ggDictList


def gprank(type, n):
    if type == 'SO':
        return int(n/2)
    elif type == 'SP':
        return int(n/2)
    else:
        return int(n)


def freed_witten(a, bi, rwmat, itens):
    """Checks whether or not the the divisor defined by D = a_i * D_i satisfies the Freed-Witten condition without fluxes."""
    # b is the basis decomposition of X, the Calabi-Yau
    # c is the matrix that gives the basis decomposition of the toric divisors

    # Get the charge vector for the divisor
    DCharges = []
    (m, n) = rwmat.shape
    for i in range(m):
        v = 0
        for j in range(n):
            v += a[j]*rwmat[i, j]
        DCharges.append(v)

    # Decompose on the basis
    b = basis_decomposition(rwmat, bi, DCharges)
    #print("Original decomp", b)

    # We now need to check a few subtleties
    # First, check for any basis divisors that don't intersect a_i * D_i = b_i * J_i. We don't need to consider their coefficients.

    # Construct the intersection numbers for our divisor b_i * J_i
    h11 = len(bi)
    itensArrs = [np.array(w) for w in itens]
    divItens = np.zeros(shape=itensArrs[0].shape, dtype=int)
    for j in range(h11):
        toAdd = np.multiply(b[j], itensArrs[j])
        divItens = np.add(toAdd, divItens)
    #print("This div:", divItens)

    # Check which divisors don't intersect it
    dontIntersect = []
    for i in range(h11):
        if not is_nonzero(divItens[i,:]):
            dontIntersect.append(i)
    #print("Don't intersect", dontIntersect)

    # Set their coefficients to zero (which won't affect the integrality when divided by 2)
    for j in dontIntersect:
        b[j] = 0
    #print("New decomp:", b)

    # Now let's check for basis divisors that are numerically equivalent on D
    equiv = [[0]]
    for i in range(1, h11):
        inSomeList = False
        for j in range(len(equiv)):
            inlst = True
            for p in range(len(divItens[i,:])):
                k = equiv[j][0]
                if divItens[i,p] != divItens[k, p]:
                    inlst = False
                    break

            # for k in equiv[j]:
            #     compCheck = True
            #     for p in range(len(divItens[i,:])):
            #         if divItens[i,p] != divItens[k,p]:
            #             compCheck = False
            #             break
            #     if not compCheck:
            #         inlst = False
            #         break

            if inlst:
                equiv[j].append(i)
                inSomeList = True

        if not inSomeList:
            equiv.append([i])
    #print("Equiv", equiv)

    # Combine the coefficients of any divisors that are numerically equivalent
    b2 = []
    for i in range(len(equiv)):
        b2.append(sum([b[k] for k in equiv[i]]))
    #print("Final decomp:", b2)

    # Check the Freed-Witten condition that the first Chern class of D is integral
    for i in range(len(b2)):
        if (b2[i] % 2) != 0:
            return False
    return True



def sigma_invariant(rwmat, c, sigma):
    """Returns whether or not the cohomology class of a toric divisor combination is invariant under sigma."""
    (m, n) = rwmat.shape
    charges = [rwmat[:,j].tolist() for j in range(n)]
    sigmaCharges = sigma_charges(rwmat, sigma)

    c = list(c)
    nzi = np.nonzero(c)
    nzi = nzi[0]
    nzi = [int(w) for w in nzi]

    origUse = [charges[w] for w in nzi]
    sigUse = [sigmaCharges[w] for w in nzi]

    origVec = []
    for i in range(m):
        val = 0
        for w in origUse:
            val += w[i]
        origVec.append(val)


    sigComb = []
    for i in range(m):
        val = 0
        for w in sigUse:
            val += w[i]
        sigComb.append(val)

    if sigComb == origVec:
        return True
    return False



def divisor_case_torics(rwmat, sigma, c, o7s, pr):
    """Returns which of the three cases the D7-brane falls into. See arXiv 0811.2936v3, Section 2.1."""

    # We first check whether the divisor lies in an O7 plane - i.e. is it pointwise fixed
    # This should basically never happen, but may as well be thorough

    # Remove the zero entries from c, since these won't appear in its generating monomial
    c = list(c)
    nzi = np.nonzero(c)
    nzi = nzi[0]
    nzi = [int(w) for w in nzi]
    #cnz = [c[i] for i in nzi]

    # Construct the generating monomial of c
    x = pr.gens()
    f = [x[i] for i in nzi]

    onO7 = True
    for w in f:
        if [pr(w)] not in o7s:
            onO7 = False
            break
    if onO7:
        return 3

    sigInvar = sigma_invariant(rwmat, c, sigma)
    
    if sigInvar:
        return 2
    else:
        return 1



def gauge_groups_Xs_partitions(rwmat, sigma, o7Charges, fsets, pr, bi):
    # PARTITIONING IS DONE, CONVERTING TO GAUGE GROUPS IS NOT
    """Computes the gauge groups."""

    ddCharges = [8*w for w in o7Charges]
    a = basis_decomposition(rwmat, bi, ddCharges)

    # Classify the monomials by which case they fall under
    v = o7_cancellation_vector(rwmat, sigma, bi, a)
    configs = o7_cancellation_configurations(rwmat, bi, v)
    nums = [1,2,2]

    # The possible gauge groups
    ggs = ['U', 'SP', 'SO'] # May need to flip the last two

    gaugeGroups = []
    for i in range(len(configs)):
        gg = []
        config = configs[i]
        partitions = partition_set(config)

        for par in partitions:
            cases = []
            for i in range(len(par)):
                cases.append(divisor_case_Xs(rwmat, sigma, bi, par[i], o7Charges, fsets)-1)


def partition_set(a):
    """Find the partitions for the gauge groups calculation."""
    n = len(a)
    s = sum(a)
    t = n*s

    # Create the inequalities forcing each variable to be nonnegative
    zeros = [0 for i in range(t+1)]
    ineqList = []
    for i in range(1,t+1):
        v = copy.deepcopy(zeros)
        v[i] = 1
        ineqList.append(v)

    # Next create the equalities to give the partition relations
    zeros = [0 for i in range(t+1)]
    eqList = []
    for i in range(n):
        v = copy.deepcopy(zeros)
        v[0] = -a[i]
        for j in range(s):
            v[j*n+i+1] = 1
        eqList.append(v)

    # Now create the polyhedron whose integral points will give us the required partitions
    sol = Polyhedron(ieqs=ineqList, eqns=eqList)
    pts = sol.integral_points()
    pts = [list(w) for w in pts]

    # Reinterpret these points as the divisor partitions
    partitions = []
    for pt in pts:
        part = []
        for i in range(s):
            part.append(pt[i*n:(i+1)*n])
        partitions.append(part)

    # Remove the all zero entries
    for i in range(len(partitions)):
        part = partitions[i]
        remove = []
        for j in range(len(part)):
            p = part[j]
            zero = True
            for k in range(len(p)):
                if p[k] != 0:
                    zero = False
                    break
            if zero:
                remove.append(j)
        partitions[i] = [part[w] for w in range(len(part)) if w not in remove]

    # Remove duplicates
    partitions2 = []
    for par in partitions:
        if par not in partitions2:
            partitions2.append(par)

    return partitions2


#def gauge_groups_invariants(polyid, geonum, trinum, involnum, h11, rwmat, sigma, o7s, bi):
def gauge_groups_invariants(rwmat, sigma, o7s, bi):
    """Computes the gauge groups."""
    o7Charges = o7_charges(rwmat, o7s)
    ddCharges = [4*w for w in o7Charges]
    polys = invariant_monomials(rwmat, sigma)
    ci, ni, rwn, rwnFull = new_rwmat(rwmat, sigma, polys)

    # Construct the list of the new (y) coordinates in terms of the xs
    ylist = []
    (m, n) = rwnFull.shape
    (m0, n0) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('y+', n))
    prx = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n0))
    x = prx.gens()

    nmax = int(max(max(ni), n))
    k = 0
    for i in range(nmax):
        if i in ni:
            ylist.append(prx(polys[k]))
            k += 1
        else:
            ylist.append(x[i])

    # The configurations that cancel the DO7 charge
    configs = polyhedron_solve(rwnFull, ddCharges)
    cases = [divisor_case_invariants(c, o7s, prx, ylist)-1 for c in configs]
    
    # The possible gauge groups and number multipliers
    ggTypes = ['U', 'SP', 'SO']
    numCase = [1,2,2]

    # The dictionary keys
    #polyidkey = "POLYID"
    #geokey = "GEOMN"
    #trikey = "TRIANGN"
    #involkey = "INVOLN"
    #h11key = "H11"
    typekey = "GTYPE"
    degkey = "DEG"
    rankkey = "RANK"
    #monindkey = "MONOMIND"
    #monomskey = "MONOMIALS"
    monomkey = "MONOM"
    multkey = "MULT"
    #gpindkey = "GROUPIND"
    gpskey = "GGROUPS"
    fwkey = "FW"

    # Put the ID numbers into a dictionary
    #idDict = {polyidkey : polyid, geokey : geonum, trikey : trinum, involkey : involnum, h11key : h11}
    idDict = {}

    # Need to put in Freed-Witten

    ggDictList = []
    #mon = []
    for i in range(len(configs)):
        gtypes = []
        nums = []
        ggs = []
        config = configs[i]
        poly = str(pr({tuple(config) : 1}))
        #mon.append(poly)

        # Put the configuration-dependent information into the dictionary
        configDict = copy.deepcopy(idDict)
        #configDict[monindkey] = int(i+1)
        configDict[monomkey] = poly

        # Need to add Freed-Witten check

        # Do the first loop to get the data for each brane in the configuration
        # This loop is done first so we can get the multiplicities
        for j in range(len(config)):
            if config[j] != 0:
                cf = [0 for p in range(n)]
                cf[j] = config[j]
                case = divisor_case_invariants(cf, o7s, prx, ylist)-1
                gtype = ggTypes[case]
                num = numCase[case]*config[j]
                gtypes.append(gtype)
                nums.append(num)
                ggs.append(gtype + '(' + str(num) + ')')

        # Get the multiplicities
        ggCount = Counter(ggs)
        mults = []
        for k in range(len(ggs)):
            mults.append(ggCount[ggs[k]])

        # Get the reduced version of the list
        used = []
        ggsList = []
        w = 1
        for k in range(len(ggs)):
            # Skip if this gauge group has already been covered
            if ggs[k] in used:
                continue
            ggsList.append([gtypes[k], nums[k], mults[k], w])
            w += 1
            used.append(ggs[k])

        # Now create a dictionary for each factor group in the configuration
        for j in range(len(ggsList)):
            ggDict = copy.deepcopy(configDict)
            [gtype, num, mult, gpind] = ggsList[j]
            ggDict[typekey] = gtype
            ggDict[multkey] = mult
            ggDict[degkey] = num
            ggDict[rankkey] = gprank(gtype, num)
            #ggDict[gpindkey] = gpind
            ggDictList.append(ggDict)

    # Now convert the monomials into x form
    ylist = [poly_index_raiser(w, prx) for w in ylist]

    polyConv = {}
    for j in range(n):
        if j in ni:
            polyConv['y' + str(j)] = '(' + str(ylist[j]) + ')'
        else:
            polyConv['y' + str(j)] = str(ylist[j])
    #for i in range(len(mon)):
    #    for key in polyConv.keys():
    #        mon[i] = mon[i].replace(key, polyConv[key])
    for i in range(len(ggDictList)):
        for key in polyConv.keys():
            ggDictList[i][monomkey] = ggDictList[i][monomkey].replace(key, polyConv[key])

    #return mon, ggDictList
    return ggDictList



def divisor_case_invariants(c, o7s, pr, ylist):
    """Returns which of the three cases the D7-brane falls into. See arXiv 0811.2936v3, Section 2.1."""

    # First, we want to check whether or not the divisor lies on an O7 plane
    # Extract the nonzero elements of c
    c = list(c)
    nzi = np.nonzero(c)
    nzi = nzi[0]
    nzi = [int(w) for w in nzi]
    #cnz = [c[i] for i in nzi]

    f = [ylist[i] for i in nzi]

    onO7 = True
    for w in f:
        if [pr(w)] not in o7s:
            onO7 = False
            break
    if onO7:
        return 3

    # If not, the charge vector is necessarily sigma-invariant
    return 2


def gauge_groups_combined(polyid, geonum, trinum, involnum, rwmat, sigma, o7s, bi, STs=True):
    """Computes the gauge groups."""
    o7Charges = o7_charges(rwmat, o7s)
    ddCharges = [8*w for w in o7Charges]
    polys = invariant_monomials(rwmat, sigma)
    ci, ni, rwn, rwnFull = new_rwmat(rwmat, sigma, polys)
    k = len(sigma)

    # Construct the new list of coordinates
    (m, n) = rwmat.shape
    (m1, n1) = rwnFull.shape

    # If we don't want to keep the single-monomial invariant polynomials
    if not STs:
        polys = polys[k:]

    nNew = len(polys)

    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))
    xynames = list(pr.variable_names()) + ['y' + str(i) for i in range(nNew)]
    prXY = PolynomialRing(base_ring=ZZ, names=xynames)
    x = pr.gens()

    # Create the list of potential brane locations
    # It consists of the toric divisors plus any two-term (anti-)invariant monomials
    coordList = []
    for i in range(n):
        coordList.append(x[i])

    for i in range(nNew):
        coordList.append(pr(polys[i]))

    sigmaCols = sigma_charges(rwmat, sigma)
    rwCols = [rwmat[:,j].tolist() for j in range(n)]
    invCols = [charge_vector(polys[i], rwmat) for i in range(nNew)]
    newCols = []

    for j in range(n):
        col = []
        for i in range(m):
            col.append(rwCols[j][i] + sigmaCols[j][i])
        newCols.append(col)

    for j in range(nNew):
        col = []
        for i in range(m):
            col.append(2*invCols[j][i])
        newCols.append(col)

    csMat = matrix(newCols).T

    # The configurations that cancel the DO7 charge
    configs = polyhedron_solve(csMat, ddCharges)

    # To avoid redundancy, we need to remove orientifold image pairs from configs
    # Note that this works since we only need to worry about this for the toric coordinates
    configs2 = []
    for cf in configs:
        if cf in configs2 or sigma_list(cf, sigma) in configs2:
            pass
        else:
            configs2.append(cf)
    configs = configs2

    # The possible gauge groups and number multipliers
    ggTypes = ['U', 'SP', 'SO']
    numCase = [1,2,2]

    # The dictionary keys
    polyidkey = "POLYID"
    geokey = "GEOMN"
    trikey = "TRIANGN"
    involkey = "INVOLN"
    typekey = "GTYPE"
    degkey = "DEG"
    rankkey = "RANK"
    monindkey = "MONOMIND"
    multkey = "MULT"
    gpindkey = "GROUPIND"
    fwkey = "FW"

    # Put the ID numbers in a dictionary
    idDict = {polyidkey : polyid, geokey : geonum, trikey : trinum, involkey : involnum}

    # Need to add in Freed-Witten

    ggDictList = []
    for i in range(len(configs)):
        gtypes = []
        nums = []
        ggs = []
        config = configs[i]
        poly = str(prXY({tuple(config) : 1}))
        mon.append(poly)

        # Put the configuration-dependent information into the dictionary
        configDict = copy.deepcopy(idDict)
        configDict[monindkey] = str(i+1)

        # Need to add the Freed-Witten check

        # Do the first loop to get the data for each brane in the configuration
        # This loop is done first so we can get the multiplicities
        for j in range(len(config)):
            if config[j] != 0:
                cf = [0 for p in range(n)]
                cf[j] = config[j]
                case = divisor_case_combined(rwmat, sigma, cf, o7s, pr, coordList)-1
                gtype = ggTypes[case]
                num = numCase[case]*config[j]
                gtypes.append(gtype)
                nums.append(num)
                ggs.append(gtype + '(' + str(num) + ')')

        # Get the multiplicities
        ggCount = Counter(ggs)
        mults = []
        for k in rnage(len(ggs)):
            mults.append(ggCount[ggs[k]])

        # Get the reduced version of the list
        used = []
        ggsList = []
        w = 1
        for k in range(len(ggs)):
            # Skip if this gauge group has already been covered
            if ggs[k] in used:
                continue
            ggsList.append([gtypes[k], nums[k], mults[k], w])
            w += 1
            used.append(ggs[k])

        # Now create a dictionary for each factor group in the configuration
        for j in range(len(ggsList)):
            ggDict = copy.deepcopy(configDict)
            [gtype, num, mult, gpind] = ggsList[j]
            ggDict[typekey] = gtype
            ggDict[multkey] = mult
            ggDict[degkey] = num
            ggDict[rankkey] = gprank(gtype, num)
            ggDict[gpindkey] = gpind
            ggDictList.append(ggDict)


    # Finally, raise the monomial indices and replace the y's with x's
    # First raise the indices on the invariant polynomials
    invars = coordList[n:]
    invars = [poly_index_raiser(w, pr) for w in invars]

    # Now, we raise the index of the toric coordinates
    xy1names = list(pr.variable_names()) + ['x' + str(n)] + ['y' + str(i) for i in range(nNew)]
    prXY1 = PolynomialRing(base_ring=ZZ, names=xy1names)
    xy1 = prXY1.gens()
    pt = xy[1:] + [0] + xy[n+1:]
    mon = [w(pt) for w in mon]

    # Finally, we replace the y's with the x's
    polyConv = {}
    for j in range(nNew):
        polyConv['y' + str(j)] = '(' + str(coordList[j]) + ')'

    for i in range(len(mon)):
        for key in polyConv.keys():
            mon[i] = mon[i].replace(key, polyConv[key])

    return mon, ggDictList



def divisor_case_combined(rwmat, sigma, cf, o7s, pr, coordList):
    """Returns which of the three cases the D7-brane falls into. See arXiv 0811.2936v3, Section 2.1."""

    # First, we want to check whether or not the divisor lies on an O7 plane
    # Extract the nonzero elements of c
    c = list(c)
    nzi = np.nonzero(c)
    nzi = nzi[0]
    nzi = [int(w) for w in nzi]

    f = [coordList[i] for i in nzi]

    onO7 = True
    for w in f:
        if [pr(w)] not in o7s:
            onO7 = False
            break
    if onO7:
        return 3

    # If the divisor is not on an O7, check whether or not its cohomology class is sigma-invariant
    # This really only depends on the toric portion of the configuration, as the classes of the (anti-)invariant divisors are not affected by sigma
    (m, n) = rwmat.shape
    cf = cf[:n]
    return divisor_case_torics(rwmat, sigma, cf, o7s, pr)


def global_factor_tuning(poly, pr):
    """Returns which moduli must be tuned to zero to get each global factor."""
    n = pr.ngens()
    x = pr.gens()
    poly = pr(poly)

    # Get the list of terms, number of terms, and max exponents for each variable
    pdict = poly.dict()
    nterms = len(pdict)
    pkeys = pdict.keys()
    pterms = [pr(pdict[key]) for key in pkeys]
    maxExps = [max([w[i] for w in pkeys]) for i in range(n)]

    # Get the number of moduli that can be left on to get the global factor
    numTuned = []
    for i in range(n):
        nums = []
        checkKeys = pkeys
        for p in range(1, maxExps[i]+1):
            nkeys = []
            for w in checkKeys:
                if w[i] >= p:
                    nkeys.append(w)
            checkKeys = nkeys
            nums.append(nterms - len(nkeys))
        numTuned.append(nums)

    return nterms, numTuned


def toric_tuning(polyid, geonum, trinum, involnum, rwmat, sigma, o7s):
    """Get the tuning results for a given toric example."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))
    o7Charges = o7_charges(rwmat, o7s)
    ddCharges = [8*w for w in o7Charges]
    sigmaCols = sigma_charges(rwmat, sigma)
    rwCols = [rwmat[:,j].tolist() for j in range(n)]
    newCols = []

    for j in range(n):
        col = []
        for i in range(m):
            col.append(rwCols[j][i] + sigmaCols[j][i])
        newCols.append(col)
    xsMat = matrix(newCols).T

    configs = polyhedron_solve(xsMat, ddCharges)
    polyTerms = [pr({tuple(c) : 1}) for c in configs]
    poly = sum(polyTerms)

    # Gauge group types and number multipliers
    ggTypes = ['U', 'SP', 'SO']
    numCase = [1,2,2]

    # Get the divisor case for each toric divisor
    zeros = [0 for i in range(n)]
    configs = []
    for i in range(n):
        config = copy.deepcopy(zeros)
        config[i] = 1
        configs.append(config)
    cases = [divisor_case_torics(rwmat, sigma, c, o7s, pr)-1 for c in configs]
    nums = [numCase[case] for case in cases]
    gtypes = [ggTypes[case] for case in cases]

    # Determine the gauge groups and their tuning
    nterms, numTuned = global_factor_tuning(poly, pr)

    ggDict = {}
    for i in range(n):
        num = nums[i]
        gtype = gtypes[i]
        for j in range(len(numTuned[i])):
            gg = gtype + '(' + str(num*(j+1)) + ')'

            # If this group is already in the dictionary, update the tuning number if necessary
            if gg in ggDict.keys():
                if numTuned[i][j] < ggDict[gg]:
                    ggDict[gg] = numTuned[i][j]
            # Otherwise, add it to the dictionary
            else:
                ggDict[gg] = numTuned[i][j]

    # Now convert to a database-compatible format
    polyidkey = "POLYID"
    geokey = "GEOMN"
    trikey = "TRIANGN"
    involkey = "INVOLN"
    typekey = "GTYPE"
    degkey = "DEG"
    tunedkey = "NTUNED"
    termskey = "NTERMS"
    idDict = {polyidkey : polyid, geokey : geonum, trikey : trinum, involkey : involnum}
    idDict[termskey] = nterms

    dictList = []
    for key in ggDict.keys():
        gpDict = copy.deepcopy(idDict)
        LparInd = key.index('(')
        RparInd = key.index(')')
        gpDict[typekey] = key[:LparInd]
        gpDict[degkey] = int(key[LparInd+1:RparInd])
        gpDict[tunedkey] = ggDict[key]
        dictList.append(gpDict)

    return dictList



def read_JSON(string):
    """Reads in results as a JSON string, returns a dict of which the properties are the values."""
    string.replace("{","[")
    string.replace("}","]")
    string = string.split('\'')
    string = '\"'.join(string)
    return json.loads(string)


def string_to_polylist(lst, pr):
    if lst == '[]':
        return []

    # Remove spaces
    lst = re.sub(" ", "", lst)

    # Split the string into a list, each element of which is (the string of a) fixed set list
    if '],[' in lst:
        lst = lst.split('],[')
        for i in range(len(lst)):
            if i != 0:
                lst[i] = '[' + lst[i]
            if i != len(lst) - 1:
                lst[i] = lst[i] + ']'
        lst[0] = lst[0][1:]
        lst[-1] = lst[-1][:-1]
    else:
        lst = [lst[1:-1]]

    # Now, for each fixed set list string, split into a real list, and convert elements to polynomials
    for i in range(len(lst)):
        el = lst[i]
        el = el[1:-1]
        el = el.split(',')
        el = [pr(w) for w in el]
        lst[i] = el

    return lst


def is_subset(oplane1, oplane2, sr, cypoly, pr):
    """Determines whether oplane1 is a subset of oplane2 when restricted to the Calabi-Yau hypersurface."""

    secs = sectors(sr, pr)
    gset2 = oplane2 + [cypoly]
    gset12 = oplane1 + oplane2 + [cypoly]

    for s in secs:
        ig2 = pr.ideal(gset2 + s)
        ig12 = pr.ideal(gset12 + s)

        gb1 = ig2.groebner_basis()
        gb12 = ig12.groebner_basis()
        gb1 = sorted(gb1)
        gb12 = sorted(gb12)

        if gb1 != gb12:
            return False

    return True



def are_connected(oplanes, sr, cypoly, pr):
    """Determines whether the given list of oplanes has a common intersection."""

    secs = sectors(sr, pr)
    gset = [cypoly]
    for oplane in oplanes:
        gset += oplane

    for s in secs:
        ig = gset + s
        gb = ig.groebner_basis()
        if gb != [pr(1)]:
            return True

    return False


def run_gauge_groups(filename):
    """Find the gauge groups for the given data."""
    # GGs: rwmat, sigma, o7Charges, fsets, pr, bi

    with open(filename) as f:
        data = []
        while True:
            try:
                dt = f.readline()
                if dt == '':
                    break
                data.append(dt)
            except EOFError:
                break

    polykey = "POLYID"
    trikey = "TRIANGN"
    geokey = "GEOMN"
    sigmakey = "INVOL"
    o7key = "O7"
    o7redkey = "O7RED"
    o5key = "O5"
    o5redkey = "O5RED"
    o3key = "O3"
    o3redkey = "O3RED"
    o1key = "O1"
    o1redkey = "O1RED"
    otherkey = "OTHER"
    rwmkey = "RESCWS"
    srkey = "SRIDEAL"
    bikey = "BASISDIVS"

    gaugeGroups = []
    num = len(data)
    print(num)
    for i in range(1):
        print(i)
        results = read_JSON(data[i])
        sigma = eval(results[sigmakey])
        sr = eval(results[srkey])
        rwmat = np.array(eval(results[rwmkey]))
        (m ,n) = rwmat.shape
        pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n+1))
        o7red = str(results[o7redkey])
        o7red = string_to_polylist(o7red, pr)
        o7 = str(results[o7key])
        o5 = str(results[o5key])
        o3 = str(results[o3key])
        o7 = string_to_polylist(o7, pr)
        o5 = string_to_polylist(o5, pr)
        o3 = string_to_polylist(o3, pr)
        #o7red = lower_fsets(o7red, pr)
        o7 = lower_fsets(o7, pr)
        o5 = lower_fsets(o5, pr)
        o3 = lower_fsets(o3, pr)
        fsets = o7 + o5 + o3
        bi = results[bikey]

        # Reindex to start at zero (since Mathematica starts at one)
        #sr = [[w-1 for w in z] for z in sr]
        sigma = [[w-1 for w in z] for z in sigma]
        bi = [i-1 for i in bi]

        o7Charges = o7_charges(rwmat, o7red)

        # If there are no O7s
        if o7Charges == []:
            gaugeGroups.append({})
            continue

        # print(rwmat)
        # print(o7red)
        # print(o7Charges)
        GGsX = gauge_groups_Xs(rwmat, sigma, o7Charges, fsets, bi)
        gaugeGroups.append(GGsX)

    d = gaugeGroups[0]
    print(len(bson.BSON.encode(d)))



def run_gauge_groups_local(datafile, resultsfile, outfile, problems):
    """Find the gauge groups for the given data set. To be run from the local results files."""
    # Use the reduced data files

    # Read in the (reduced) data file
    with open(datafile) as f:
        data = []
        while True:
            try:
                dt = f.readline()
                if dt == '':
                    break
                data.append(dt)
            except EOFError:
                break

    # Read in the results file
    with open(resultsfile) as f:
        results = []
        while True:
            try:
                dr = f.readline()
                if dr == '':
                    break
                results.append(dr)
            except EOFError:
                break

    data = [eval(w) for w in data]
    num = len(data)
    rnum = len(results)

    polykey = "POLYID"
    trikey = "TRIANGN"
    geokey = "GEOMN"
    sigmakey = "INVOL"
    o7key = "O7"
    o7redkey = "O7RED"
    o5key = "O5"
    o5redkey = "O5RED"
    o3key = "O3"
    o3redkey = "O3RED"
    o1key = "O1"
    o1redkey = "O1RED"
    otherkey = "OTHER"
    rwmkey = "RESCWS"
    srkey = "SRIDEAL"
    bikey = "BASISDIVS"

    # Check if the number of data and result entries match
    # if num != rnum:
    #     print("Number mismatch")
    #     return None

    gaugeGroups = []
    ct = 0
    with open(outfile, 'w') as f:
        for i in range(num):
            print(i)

            if i in problems:
                continue

            [polyid, geonum, bi, rwmat, trinum, sr, sigma] = data[i]
            result = read_JSON(results[ct])
            sigma = eval(result[sigmakey])
            sr = eval(result[srkey])
            rwmat = np.array(eval(result[rwmkey]))
            (m, n) = rwmat.shape
            pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n+1))
            o7 = str(result[o7key])
            o5 = str(result[o5key])
            o3 = str(result[o3key])
            o7 = string_to_polylist(o7, pr)
            o5 = string_to_polylist(o5, pr)
            o3 = string_to_polylist(o3, pr)
            o7 = lower_fsets(o7, pr)
            o5 = lower_fsets(o5, pr)
            o3 = lower_fsets(o3, pr)
            fsets = o7 + o5 + o3


            # Get the ID numbers from the results
            # The ones from the data were already obtained above
            geores = eval(result[geokey])
            trires = eval(result[trikey])
            polyres = eval(result[polykey])

            # Check that the ID numbers between the data and the results match
            if geores != geonum or trires != trinum or polyres != polyid:
                print("ID mismatch")
                print(geores, geonum)
                print(trires, trinum)
                print(polyres, polyid)
                return gaugeGroups


            # Reindex to start at zero (since Mathematica starts at one)
            #sr = [[w-1 for w in z] for z in sr]
            sigma = [[w-1 for w in z] for z in sigma]
            bi = [w-1 for w in bi]

            # If an O7 is not length 1, we need to reduce it (since I forgot to include this in the local files)
            # o7red = []
            # for o7plane in o7:
            #     if len(o7plane) == 1:
            #         o7red.append(o7plane)
            #     else:
            #         print("No good")
            #         continue
            #         polys = invariant_monomials(rwmat, sigma)
            #         k = len(sigma)
            #         (m, n) = rwmat.shape
            #         if len(polys) > k:
            #             ci, ni, rwn, rwnFull = new_rwmat(rwmat, sigma, polys)
            #             pr = PolynomialRing(base_ring=CC, names=normalize_names('x+', n))
            #             prx = PolynomialRing(base_ring=CC, names=normalize_names('x+', n))
            #             pry = PolynomialRing(base_ring=CC, names=names_list(rwn, ni))

            #             # Create the CY polynomial
            #             charges = cy_charges(rwmat)
            #             cypoly = general_poly(rwmat, charges, pr=pr)
            #             cypoly = symm_poly_swap(cypoly, sigma, pr)
            #             pr = prx

            #             o7plane = [pr(str(w)) for w in o7plane]
            #             o7plane = [pr(w) for w in o7plane]
            #             #print(o7plane)
            #             o7plane, o7planeRed = fset_reduced_GGs(pr, o7plane, sr, cypoly, polys, ni, rwn)
            #             o7red.append(o7planeRed)
            #         else:
            #             pr = PolynomialRing(base_ring=CC, names=normalize_names('x+', n))
            #             ni = [w for b in sigma for w in b]

            #             # Create the CY polynomial
            #             charges = cy_charges(rwmat)
            #             cypoly = general_poly(rwmat, charges, pr=pr)
            #             cypoly = symm_poly_swap(cypoly, sigma, pr)
            #             rwn = None

            #             o7plane, o7planeRed = fset_reduced_GGs(pr, o7plane, sr, cypoly, polys, ni, rwn)


            o7Charges = o7_charges(rwmat, o7)
            #print(o7Charges)

            # print(rwmat)
            # print("Sigma:", sigma)
            # print("O7:", o7)
            # print("Basis:", bi)

            # If there are no O7s, skip
            if o7Charges == []:
                gaugeGroups.append({})
                f.write(str({}))
                f.write('\n')

            # Otherwise, do the gauge group calculation
            else:
                monDict, GGsX = gauge_groups_torics(polyid, geonum, trinum, 1, rwmat, sigma, o7, bi)
                #ggTuneX = toric_tuning(1, 1, 1, 1, rwmat, sigma, o7)
                #GGsX = gauge_groups_invariants(1, 1, 1, 1, rwmat, sigma, o7, bi)
                #GGsX = gauge_groups_combined(1, 1, 1, 1, rwmat, sigma, o7, bi, False)
                gaugeGroups.append(GGsX)

                # Write the gauge group dictionary to the output file
                print(monDict)
                print(GGsX)
                f.write(str(json.dumps(monDict)))
                f.write('\n')
                f.write(str(GGsX))
                f.write('\n')
            ct += 1


def gauge_groups_one(involdoc):
    """Computes the gauge groups for one example."""

    # Read in the query return as a JSON dictionary and get the necessary data in string form
    #h11key = "H11"
    rwmkey = "RESCWS"
    sigmakey = "INVOL"
    oplaneskey = "OPLANES"
    odimkey = "ODIM"
    oidealkey = "OIDEAL"
    bikey = "BASIS"
    polyidkey = "POLYID"
    geokey = "GEOMN"
    trikey = "TRIANGN"
    involkey = "INVOLN"
    itenskey = "ITENSXJ"

    involdoc = json.loads(involdoc)
    #h11 = involdoc[h11key]
    rwmat = str(involdoc[rwmkey])
    sigma = str(involdoc[sigmakey])
    o7s = [x[oidealkey] for x in involdoc[oplaneskey] if x[odimkey]==7]
    bi = str(involdoc[bikey])
    polyid = int(involdoc[polyidkey])
    geonum = int(involdoc[geokey])
    trinum = int(involdoc[trikey])
    involnum = int(involdoc[involkey])
    itens = str(involdoc[itenskey])

    # Convert to Python format and extract the relevant information
    rwmat = rwmat_m2p(rwmat)
    sigma = invol_m2p(sigma)
    bi = bi_m2p(bi)
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+',n+1))
    #o7s = oplanes_m2p(o7s, pr)
    o7s = [[pr(y) for y in x] for x in o7s]
    itens = itens_m2p(itens)

    # Reindex
    sigma = [[w-1 for w in x] for x in sigma]
    bi = [w-1 for w in bi]
    o7s = lower_fsets(o7s, pr)

    query = {}
    query[polyidkey] = polyid
    query[geokey] = geonum
    query[trikey] = trinum
    query[involkey] = involnum
    
    # Find the gauge groups
    #mons, GGsT = gauge_groups_torics(polyid, geonum, trinum, involnum, rwmat, sigma, o7s, bi)
    #GGsT = gauge_groups_torics(polyid, geonum, trinum, involnum, h11, rwmat, sigma, o7s, bi)
    GGsT = gauge_groups_torics(rwmat, sigma, o7s, bi, itens)
    #GGsI = gauge_groups_invariants(polyid, geonum, trinum, involnum, rwmat, sigma, o7s, bi)
    #GGsC = gauge_groups_combined(polyid, geonum, trinum, involnum, rwmat, sigma, o7s, bi, False)
    #return query, mons, GGsT
    return query, GGsT



involdoc = sys.argv[1]

#query, monDict, GGsX = gauge_groups_one(involdoc)
query, GGsX = gauge_groups_one(involdoc)

#print "+INVOL."+json.dumps(query,separators=(',',':'))+">"+json.dumps(monDict,separators=(',',':'))
for i in range(len(GGsX)):
    GGsX[i]["GAUGEN"] = i+1
gaugeDict = {"GAUGE":GGsX}
print "+INVOL."+json.dumps(query,separators=(',',':'))+">"+json.dumps(gaugeDict,separators=(',',':'))
sys.stdout.flush()


# a = [0,1,2,0,0,0,0]
# bi = [1,4,6]
# rwmat = np.transpose(rwmat_m2p('{{0,0,0,1,0,1,1},{0,0,1,0,1,0,0},{1,1,0,2,0,2,0}}'))
# #Original
# #itens = itens_m2p('{{{0,0,0},{0,0,3},{0,3,2}},{{0,0,3},{0,0,0},{3,0,-6}},{{0,3,2},{3,0,-6},{2,-6,-8}}}')
# #Modified
# itens = itens_m2p('{{{0,0,9},{1,0,1},{0,3,2}},{{1,0,1},{0,0,3},{3,0,-6}},{{0,3,2},{3,0,-6},{2,-6,-8}}}')
# print(rwmat)
# print(itens)

# fw = freed_witten(a, bi, rwmat, itens)
# print(fw)