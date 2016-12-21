#!/bin/bash

template=$1
primerpath=$2
primername=$3

cp -r "${template}" "${primerpath}/${primername}"

files=$(find "${primerpath}/${primername}/" -mindepth 1 -type f)
for file in ${files}
    do
    	sed -i "s/template/${primername}/g" ${file}
    	newfile=$(echo "${file}" | sed "s/template/${primername}/g")
    	mv ${file} ${newfile} 
    done