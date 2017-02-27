#!/shared/apps/sage/sage-5.12/spkg/bin/sage -python

from sage.all_cmdline import *
from sage.schemes.toric.variety import normalize_names
from sage.rings.polynomial.polydict import PolyDict
from sage.libs.singular.function_factory import singular_function
import sage.logic.propcalc as propcalc
from itertools import permutations
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
from collections import defaultdict
import mongolink.tools as tools
import mongolink.parse as parse
from mongolink.parse import pythonlist2mathematicalist as py2mat
from mongolink.parse import mathematicalist2pythonlist as mat2py
import sys
import re
    
def inv(invol, poly, pr):
    """Returns the polynomial after being acted on by invol"""
    k = len(invol)
    d = poly.dict()
    d2 = {}
          
    for key in d.keys():
        nkey = list(key)
        for i in range(k):
            swap = invol[i]
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


def symm_poly_swap(poly, invol, pr):
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
        involTerm = pr(inv(invol, term, pr))
        if involTerm in pTerms:
            symm.append(term)

    symmPoly = pr(0)
    for i in range(len(symm)):
        symmPoly += pr(symm[i])
    return symmPoly


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


def symm(invol, poly, pr):
    """Returns the part of the polynomial poly symmetric under the involution invol"""
    spoly = inv(invol, poly, pr)
    symm = (pr(poly) + pr(spoly)) / 2
    return symm


def asymm(invol, poly, pr):
    """Returns the part of the polynomial poly antisymmetric under the involution invol"""
    spoly = inv(invol, poly, pr)
    asymm = (pr(poly) - pr(spoly)) / 2
    return asymm


def cs_moduli_tuned(invol, poly, pr):
    """Returns the number of complex structure moduli that must be tuned to zero"""
    a = asymm(invol, poly, pr)
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
        for i in range(m):
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
        print(ineqList)

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



def invariant_monomials(rwmat, invol):
    """Computes the invariant monomials of the involution invol with n factors to a term."""
    (m, n) = rwmat.shape
    k = len(invol)
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()

    invars = []

    # Find the difference between charge columns of the divisors that are swapped
    ccols = []
    for swap in invol:
        a = rwmat[:,swap[0]]
        b = rwmat[:,swap[1]]
        ccols.append([j - i for i, j in zip(a,b)])


    # Create the trivial invariant polynomials
    for swap in invol:
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
                        swap = invol[comb[i]]
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


def new_rwmat(rwmat, invol, polys=None):
    """Finds the new reduced weight matrix after the Segre map."""
    (m, n) = rwmat.shape
    if polys == None:
        polys = invariant_monomials(rwmat, invol)
    
    invol_inds = sorted(list(itertools.chain.from_iterable(invol)))
    keep_inds = list(set(range(n)) - set(invol_inds))
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
    k = len(invol)
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



def defining_expression(rwmat, invol, ni, polys=None):
    """Finds the defining equation among the list of new projective coordinates."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()
    
    def_exp = 0
    k1 = len(invol)
    
    if polys == None:
        polys = invariant_monomials(rwmat, invol)
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
    
        
def anti_invariant_monomials(rwmat, invol, polys, ni):
    """Returns the indices of the anti-invariant monomials corresponding to rwn and invol."""
    (m, n) = rwmat.shape
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names('x+', n))

    anti_inds = []
    for i in range(len(polys)):
        p = polys[i]
        if asymm(invol, p, pr) == p:
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


def fixed_swap(rwmat, invol, ni, polys, sr):
    (m, n) = rwmat.shape
    k = len(invol)
    pr = PolynomialRing(base_ring=ZZ, names=normalize_names("x+",n))
    x = pr.gens()

    fixed_sets = []
    
    for i in range(k):
        swap = invol[i]
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


def new_sr_ideal(rwmat, invol, polys, sr, ni):
    """Returns the new SR ideal after the coordinate change"""
    (m, n) = rwmat.shape
    k = len(invol)
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


def o7_charges(rwmat, o7_red, pr):
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



def invol_basis_charges(rwmat, invol, bi):
    """Computes the action of the involution on the cohomology basis charge vectors."""
    (m, n) = rwmat.shape
    si = [w for b in invol for w in b]
    rwmat = np.array(rwmat) # Typecasting if necessary

    # Find the images of the basis divisors under the involution
    bi_invol = []
    for i in bi:
        if i not in si:
            bi_invol.append(i)
        else:
            for swap in invol:
                if i in swap:
                    if i == swap[0]:
                        bi_invol.append(swap[1])
                    else:
                        bi_invol.append(swap[0])
                    break

    # Find the charge vectors for the transformed basis divisors
    charges = [rwmat[:,j].tolist() for j in range(n)]
    charges = [charges[i] for i in bi_invol]

    return charges


def invol_basis(rwmat, invol, bi, bs_charges=None):
    """Computes the action of the involution on the cohomology basis."""
    if bs_charges == None:
        bs_charges = invol_basis_charges(rwmat, invol, bi)

    # Find the basis decompositions of the transformed charge vectors
    bds = [basis_decomposition(rwmat, bi, bs_charges[i]) for i in range(len(bs_charges))]
    return bds


def tadpole_cancellation_basis(rwmat, invol, bi, invol_charges=None):
    (m, n) = rwmat.shape
    si = [w for b in invol for w in b]
    if invol_charges == None:
        invol_charges = invol_basis_charges(rwmat, invol, bi)

    # Find the charge vectors for the basis divisors
    charges = [rwmat[:,j].tolist() for j in range(n)]
    charges = [charges[i] for i in bi]

    # The basis decompositions for the transformed charge vectors
    bds = invol_basis(rwmat, invol, bi)

    tcb = []
    for i in range(len(charges)):
        if bi[i] in si:
            vec = [(charges[i][j] + invol_charges[i][j]) for j in range(m)]
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

    pr1c = PolynomialRing(base_ring=CC, names=normalize_names('x+', n0))
    
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

            # Check if this set is logically consistent
            # That is, take into account the dependencies among the new coordinates
            # We ignore the set of the Groebner basis is [1]
            idGens = []
            for k in range(n):
                if k in comb and k in ni:
                    idGens.append(pr1c(x2[k]-1))
                elif k in z:
                    idGens.append(pr1c(x2[k]))
            idl = pr1c.ideal(idGens)
            gb = idl.groebner_basis()
            if gb == [1]:
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


def gauge_groups_Ys(rwmat, rwnFull, invol, polys, o7Charges, fsets, prx, pry, ai, bi, ni):
    """Computes the gauge groups."""
    ddCharges = [4*w for w in o7Charges]
    divPoly = general_poly(rwnFull, ddCharges, pr=pry)
    divPoly = symm_poly(divPoly, ai, pry)
    monomials = terms(divPoly)
    monomials = [pry(m.strip()) for m in monomials]

    # Classify the monomials by which case they fall under
    facs = [factor(p) for p in monomials]
    cases = [divisor_case_Ys(p, rwmat, rwnFull, invol, fsets, bi, pry)-1 for p in monomials]
    #print(monomials)

    # The possible gauge groups
    ggs = ['U', 'SP', 'SO']  # May need to flip the last two

    gaugeGroups = []
    for i in range(len(monomials)):
        gg = []
        gtype = ggs[cases[i]]
        for j in range(len(facs[i])):
            gg.append(gtype +'(' + str(2*facs[i][j][1]) + ')')
        gg = 'x'.join(gg)
        gaugeGroups.append(gg)

    return gaugeGroups



def divisor_case_Ys(p, rwmat, rwnFull, invol, fsets, bi, pry):
    """Returns which of the three cases the D7-brane falls into. See arXiv 0811.2936v3, Section 2.1."""
    # Check whether the divisor lies in an O7 plane - i.e. is it pointwise fixed
    f = factor(p)
    f = [w[0] for w in f]

    onO7 = True
    for w in f:
        if [w] not in fsets:
            onO7 = False
    if onO7:
        return 3


    # If not, determine whether or not its charge vector is invol-invariant
    v = charge_vector(p, rwnFull, pr=pry)
    bd = basis_decomposition(rwmat, bi, v)
    involBasis = invol_basis_charges(rwmat, invol, bi)

    vinvol = []
    for i in range(len(involBasis[0])):
        vs = 0
        for j in range(len(involBasis)):
            vs += bd[j]*involBasis[j][i]
        vinvol.append(vs)

    if v != vinvol:
        return 1
    else:
        return 2


def o7_cancellation_vector(rwmat, invol, bi, a):
    """a is the basis decomposition of the DD charge vector."""
    bds = invol_basis(rwmat, invol, bi)
    Z = IntegerRing()

    # sMatrix represents invol as a linear transformation on the basis charge vectors
    # Use this instead of invol_basis_charges for factorization purposes
    sMatrix = matrix(Z, bds).T
    k = sMatrix.nrows()
    ident = identity_matrix(ZZ, k)
    bd2 = ident + sMatrix
    a = matrix(a).T
    nt = bd2.solve_right(a)
    return list(nt.T)


def o7_cancellation_configurations(rwmat, bi, a):
    """Finds the configurations that cancel the D7 charge."""
    (m, n) = rwmat.shape
    rwmat = np.array(rwmat) # Typecasting if necessary

    # Find the solutions via solving a Diophantine system via Smith normal form
    decomps = [basis_decomposition(rwmat, bi, rwmat[:,i].tolist()) for i in range(n)]
    B = matrix(ZZ, decomps).T
    a = matrix(ZZ, a).T
    return smith_solve(B, a)


def gauge_groups_Xs(rwmat, invol, o7Charges, fsets, pr, ai, bi, ni):
    """Computes the gauge groups."""

    ddCharges = [8*w for w in o7Charges]
    a = basis_decomposition(rwmat, bi, ddCharges)

    # Classify the monomials by which case they fall under
    v = o7_cancellation_vector(rwmat, invol, bi, a)
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
                cases.append(divisor_case_Xs(rwmat, invol, bi, par[i], o7Charges, fsets)-1)






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



def divisor_case_Xs(rwmat, invol, bi, c, o7Charges, fsets):
    """Returns which of the three cases the D7-brane falls into. See arXiv 0811.2936v3, Section 2.1."""
    # Check whether the divisor lies in an O7 plane - i.e. is it pointwise fixed
    c = list(c)
    nzi = np.nonzero(c)
    nzi = nzi[0]
    nzi = [int(w) for w in nzi]

    cnz = [c[i] for i in nzi]
    f = ['x'+str(i) for i in cnz]

    onO7 = True
    for w in f:
        if [w] not in fsets:
            onO7 = False
    if onO7:
        return 3

    # If not, check whether the divisor's charge vector is invol-invariant
    # Since x + invol(x) = ddCharges, the two terms are equal if x = ddCharges/2
    (m, n) = rwmat.shape
    testVector = [4*w for w in o7Charges]
    chargeVector = []
    for i in range(m):
        cs = 0
        for j in range(n):
            cs += c[j]*rwmat[i,j]
        chargeVector.append(cs)

    if chargeVector == testVector:
        return 2
    else:
        return 1


def read_JSON(string):
    """Reads in results as a JSON string, returns a dict of which the properties are the values."""
    string.replace("{","[")
    string.replace("}","]")
    string = string.split('\'')
    string = '\"'.join(string)
    return json.loads(string)

#Hodge splitting
def hodgesplit(h11,h21,invol,basisinds,dresverts):
    R=PolynomialRing(QQ,['t'+str(i+1) for i in range(len(basisinds))]+['D'+str(i+1) for i in range(len(dresverts))]+['Ep'+str(i+1) for i in range(len(invol))]+['Em'+str(i+1) for i in range(len(invol))]+['J'+str(i+1) for i in range(len(basisinds))]);
    vars=R.gens_dict();
    Ilin=R.ideal([sum([dresverts[i][j]*vars['D'+str(i+1)] for i in range(len(dresverts))]) for j in range(len(dresverts[0]))]);
    Isplit=R.ideal([z for y in [[2*vars['D'+str(invol[i][0]+1)]-(vars['Ep'+str(i+1)]+vars['Em'+str(i+1)]),2*vars['D'+str(invol[i][1]+1)]-(vars['Ep'+str(i+1)]-vars['Em'+str(i+1)])] for i in range(len(invol))] for z in y]);
    Ibasisinds=R.ideal([vars['D'+str(basisinds[i]+1)]-vars['J'+str(i+1)] for i in range(len(basisinds))]);
    hysurf=sum([vars['D'+str(i+1)] for i in range(len(dresverts))]);
    hyideal=(Ilin+Ibasisinds).quotient(R.ideal(hysurf));
    J=sum([vars['t'+str(i+1)]*vars['D'+str(basisinds[i]+1)] for i in range(len(basisinds))]);
    Jreduced=J.reduce(Ilin+Isplit);
    Isym=R.ideal([y for y in [Jreduced.coefficient(vars['Em'+str(i+1)]) for i in range(len(invol))] if y!=0]);
    symJ=Jreduced.reduce(Isym);
    symJcoefficients=tools.transpose_list([y for y in [[symJ.coefficient(vars['Ep'+str(i+1)]),(vars['D'+str(invol[i][0]+1)]+vars['D'+str(invol[i][1]+1)]).reduce(hyideal)] for i in range(len(invol))]+[[symJ.coefficient(vars['D'+str(i+1)]),vars['D'+str(i+1)].reduce(hyideal)] for i in range(len(dresverts))] if y[0]!=0]);
    Iasym=R.ideal([y for y in [Jreduced.coefficient(vars['Ep'+str(i+1)]) for i in range(len(invol))] if y!=0]);
    asymJ=Jreduced.reduce(Iasym);
    asymJcoefficients=tools.transpose_list([y for y in [[asymJ.coefficient(vars['Em'+str(i+1)]),(vars['D'+str(invol[i][0]+1)]-vars['D'+str(invol[i][1]+1)]).reduce(hyideal)] for i in range(len(invol))] if y[0]!=0]);
    symh11=len(symJcoefficients[0]);
    symh21=((h21-h11)/2)-symh11
    h11split=[symh11,h11-symh11];
    h21split=[symh21,h21-symh21];
    return [h11split,h21split,symJcoefficients,asymJcoefficients];
    
def allbaseshodgesplit(h11,h21,invol,basisinds,dresverts,rwmat):
    bases=[x for x in Combinations(range(len(dresverts)),matrix(rwmat).rank()).list() if matrix([rwmat[i] for i in x]).rank()==matrix(rwmat).rank()];
    result0=[hodgesplit(h11,h21,invol,x,dresverts,rwmat) for x in [basisinds]+bases];
    result1=[[x[0],x[1]] for x in result0];
    result=[all([x==result1[0] for x in result1])];
    result.extend([result0[0][0],result0[0][1],result0[0][2][1],result0[0][3][1]]);
    return result;

def main_one(polyid, geonum, trinum, invnum, h11, h21, invol, basisinds, dresverts, sr, rwmat):
    """Runs the entire routine for a single example."""

    # DATA FROM POLYID, GEONUM, TRINUM
    polykey = "POLYID"
    trikey = "TRIANGN"
    geokey = "GEOMN"
    invkey = "INVOLN"
    origcyseckey = "ORIGCYSECTIONS"
    symcyseckey = "SYMCYSECTIONS"
    #o7key = "O7"
    #o5key = "O5"
    #o3key = "O3"
    #o1key = "O1"
    #otherkey = "OTHER"
    allbaseskey = "ALLBASES"
    h11splitkey = "H11SPLIT"
    h21splitkey = "H21SPLIT"

    # # # First off, we need to reformat the SR ideal, the RWM, and the basis into the appropriate forms
    # # # Let's start with the SR ideal
    # sr = sr.replace("{","[")
    # sr = sr.replace("}", "]")
    # sr = sr.replace("D", "")
    # sr = eval(sr)
    # sr = [str(w).replace(" ", "") for w in sr]
    # sr = [[w[j] for j in range(len(w))] for w in sr]

    # # # Now for the basis - we want to extract the indices of the basis divisors
    # bi = bi.replace("{", "[")
    # bi - bi.replace("}", "]")
    # bi = bi.replace("D", "")
    # bi = bi.replace("J", "")
    # bi = [w[1] for w in bi]

    # # # Finally, reformat the RWM
    # rwmat = rwmat.replace("{", "[")
    # rwmat = rwmat.replace("}", "]")

    # Reindex to start at zero (since Mathematica starts at one)
    #sr = [[w-1 for w in z] for z in sr]
    #invol = [[w-1 for w in z] for z in invol]

    rwmat = np.transpose(np.array(rwmat))

    (m, n) = rwmat.shape
    k = len(invol)
    polys = invariant_monomials(rwmat, invol)
    #print(polys)

    #o7 = []
    #o5 = []
    #o3 = []
    #o1 = []
    #other = []
    #o7red = []
    #o5red = []
    #o3red = []
    #o1red = []
    #otherred = []

    oplanes = {};
    oplanesred = {};

    # Split based on whether there are anti-invariant monomials or not
    q = len(polys)
    if q > k:
        ci, ni, rwn, rwnFull = new_rwmat(rwmat, invol, polys)
        #print(rwn)
        ai = anti_invariant_monomials(rwmat, invol, polys, ni)
        fsets, fsetsx = fixed_flip(rwmat, ni, ai, polys, sr, rwn)
        #print(fsets)
        prx = PolynomialRing(base_ring=CC, names=normalize_names('x+', n))
        pry = PolynomialRing(base_ring=CC, names=names_list(rwn, ni))
        charges = cy_charges(rwmat)
        #charges = [charges[i] for i in range(len(charges)) if i in ci]

        # For now, we are using the most general poly
        # cypoly = general_poly(rwn, charges, pr=pry)
        # cypoly = symm_poly(cypoly, ai, pry)
        # cypoly = poly_ytox(cypoly, polys, prx, pry, ni)
        cypoly = general_poly(rwmat, charges, pr=prx)
        nterms1 = len(cypoly.dict().keys())
        cypoly = symm_poly_swap(cypoly, invol, prx)
        nterms2 = len(cypoly.dict().keys())
        pr = prx

        # Sort the fixed sets into O7, O5, O3, O1 by finding the codimension
        for i in range(len(fsets)):
            fset = fsets[i]
            fsetx = fsetsx[i]
            fset, fr = fset_reduced(prx, fset, sr, cypoly, polys, ni, rwn)

            if (fset, fr) == (None, None):
                continue

            codim = len(fr)
            On = 'O'+str(9-(2*codim))
            #if codim == 1:
            #    o7.append(fsetx)
            #    o7red.append(fr)
            #elif codim == 2:
            #    o5.append(fsetx)
            #    o5red.append(fr)
            #elif codim == 3:
            #    o3.append(fsetx)
            #    o3red.append(fr)
            #elif codim == 4:
            #    o1.append(fsetx)
            #    o1red.append(fr)
            #else:
            #    other.append(fsetx)
            #    otherred.append(fr)
            if On in oplanes.keys():
                oplanes[On].append(fsetx)
            else:
                oplanes[On] = [fsetx]

            if On in oplanesred.keys():
                oplanesred[On].append(fr)
            else:
                oplanesred[On] = [fr]
    else:
        pr = PolynomialRing(base_ring=CC, names=normalize_names('x+', n))
        ni = [w for b in invol for w in b]
        fsets = fixed_swap(rwmat, invol, polys, ni, sr)
        charges = cy_charges(rwmat)
        rwmat = np.matrix(rwmat)

        # Use the most general poly for now
        cypoly = general_poly(rwmat, charges, pr=pr)
        nterms1 = len(cypoly.dict().keys())
        cypoly = symm_poly_swap(cypoly, invol, pr)
        nterms2 = len(cypoly.dict().keys())
        #print(cypoly1)
        #print(cypoly)
        #print(cypoly1 - cypoly)
        rwn = None
        rwnFull = rwmat

        # Sort the fixed sets into O7, O5, O3, O1 by finding the codimension
        for fset in fsets:
            fset, fr = fset_reduced(pr, fset, sr, cypoly, polys, ni, rwn)

            if (fset, fr) == (None, None):
                continue

            codim = len(fr)
            On = 'O'+str(9-(2*codim))
            #if codim == 1:
            #    o7.append(fset)
            #    o7red.append(fr)
            #elif codim == 2:
            #    o5.append(fset)
            #    o5red.append(fr)
            #elif codim == 3:
            #    o3.append(fset)
            #    o3red.append(fr)
            #elif codim == 4:
            #    o1.append(fset)
            #    o1red.append(fr)
            if On in oplanes.keys():
                oplanes[On].append(fset)
            else:
                oplanes[On] = [fset]

            if On in oplanesred.keys():
                oplanesred[On].append(fr)
            else:
                oplanesred[On] = [fr]

    # secs = sectors(sr, pr)
    # for sec in secs:
    #     print(sec)

    # Reindex the fixed sets
    newnames = ['x' + str(i) for i in range(n+1)]
    prn = PolynomialRing(base_ring=CC, names=newnames)
    pt = [prn(w) for w in newnames[1:]] + [0]
    #oplanes = [o1,o3,o5,o7]
    for key in oplanes.keys():
        oplaneval = oplanes[key]
        for i in range(len(oplaneval)):
            fset = oplaneval[i]
            for j in range(len(fset)):
                w = prn(fset[j])
                w = w(pt)
                fset[j] = prn(w)
            oplaneval[i] = fset
        oplanes.update({key:oplaneval})

    for key in oplanesred.keys():
        oplaneredval = oplanesred[key]
        for i in range(len(oplaneredval)):
            fset = oplaneredval[i]
            for j in range(len(fset)):
                w = prn(fset[j])
                w = w(pt)
                fset[j] = prn(w)
            oplaneredval[i] = fset
        oplanesred.update({key:oplaneredval})

    #allbases,h11split,h21split,symbasis,asymbasis=allbaseshodgesplit(h11,h21,invol,basisinds,dresverts,np.transpose(rwmat).tolist())
    h11split,h21split,symJcoeffs,asymJcoeffs=hodgesplit(h11,h21,invol,basisinds,dresverts)
    query = {}
    query[polykey] = polyid
    query[geokey] = geonum
    query[trikey] = trinum
    query[invkey] = invnum

    #output = {}
    #if len(o7)>0:
    #    output[o7key] = py2mat(o7red)
    #if len(o5)>0:
    #    output[o5key] = py2mat(o5red)
    #if len(o3)>0:
    #    output[o3key] = py2mat(o3red)
    #if len(o1)>0:
    #    output[o1key] = py2mat(o1red)
    #if len(other)>0:
    #    output[otherkey] = py2mat(other)
    output = dict([(x[0],py2mat(x[1])) for x in oplanesred.items()])
    #output[allbaseskey] = allbases
    output[h11splitkey] = py2mat(h11split)
    output[h21splitkey] = py2mat(h21split)
    output[origcyseckey] = nterms1
    output[symcyseckey] = nterms2

    # Reindex invol to match the database/Mathematica format
    invol = [[w+1 for w in f] for f in invol]

    # print(o7red)
    # cgs = o7_charges(rwnFull, o7red, pry)
    # cgs = [8*w for w in cgs]
    # print(cgs)
    # print(general_poly(rwmat, cgs))

    return [query,output];


def main_all(filename, tofile):

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

    data = list(set(data))
    data = [eval(w) for w in data]
    num = len(data)
    print(num)

    o1s = []

    cont = []
    with open(tofile, 'w') as f:
        for i in range(1):

            if i in cont:
                continue

            print(i)
            [polyid, geonum, bi, rwmat, trinum, sr, invol] = data[i]
            output = main_one(polyid, geonum, trinum, invol, sr, rwmat)
            output = dict(eval(output))
            if len(list(eval(output["O1"]))) > 0:
                o1s.append(i)
            print(output)
            f.write(str(output))
            f.write('\n')

    print(o1s)
    
involdoc = json.loads(sys.argv[1])
polyid = involdoc['POLYID']
geonum = involdoc['GEOMN']
trinum = involdoc['TRIANGN']
invnum = involdoc['INVOLN']
h11 = involdoc['H11']
h21 = involdoc['H21']
invol = involdoc['INVOL']
basis = involdoc['BASIS']
dresverts = mat2py(involdoc['DRESVERTS'])
sr = involdoc['SRIDEAL']
rwmat = involdoc['RESCWS']

invol = tools.deldup([sorted([y-1 for y in x]) for x in mat2py(re.sub("D([0-9]+)->D([0-9]+)",r"[\1,\2]",invol))])
basisinds = [x-1 for x in tools.transpose_list(mat2py(re.sub("[JD]","",basis)))[1]]
sr = [[y-1 for y in eval(("["+x+"]").replace("D","").replace("*",","))] for x in sr.lstrip("{").rstrip("}").split(",")]
rwmat = np.transpose(np.array(mat2py(rwmat)))

query, output = main_one(polyid, geonum, trinum, invnum, h11, h21, invol, basisinds, dresverts, sr, rwmat)

print "+INVOL."+json.dumps(query)+">"+json.dumps(output)
sys.stdout.flush()
