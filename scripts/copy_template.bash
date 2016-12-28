#!/bin/bash

templatepath=$1
primerpath=$2
primername=$3

cp -r "${templatepath}" "${primerpath}"

files=$(find "${primerpath}/" -mindepth 1 -type f)
for file in ${files}
    do
    	sed -i "s/template/${primername}/g" ${file}
    	newfile=$(echo "${file}" | sed "s/template/${primername}/g")
    	mv ${file} ${newfile} 2>/dev/null
    done