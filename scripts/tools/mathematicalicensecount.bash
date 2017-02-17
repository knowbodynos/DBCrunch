#!/bin/bash

mathmonitorscript=$(echo $(which math) | sed 's/\(.*\)INSTALL.*/\1monitorlm/g')
licensediff=$(${mathmonitorscript} | grep 'MathKernel' | head -n1 | sed 's/\s\s*/+/g' | cut -d'+' -f3,4 | sed 's/^/-/g' |  head -c -1)
nlicensesleft=$(echo ${licensediff} | bc | head -c -1)
sublicensediff=$(${mathmonitorscript} | grep 'Sub MathKernel' | head -n1 | sed 's/\s\s*/+/g' | cut -d'+' -f4,5 | sed 's/^/-/g' |  head -c -1)
nsublicensesleft=$(echo ${sublicensediff} | bc | head -c -1)
echo "${nlicensesleft},${nsublicensesleft}" | head -c -1
