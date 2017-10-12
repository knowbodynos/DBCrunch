#------------------------------------------------------------------------------

cdef extern from * nogil: # "string.h"
    void *memset(void *, int, size_t)
    void *memcpy(void *, void *, size_t)
    void *memmove(void *, void *, size_t)

cdef extern from * nogil: # "stdio.h"
    ctypedef struct FILE
    FILE *stdin, *stdout, *stderr
    int fprintf(FILE *, char *, ...)
    int fflush(FILE *)

#------------------------------------------------------------------------------
