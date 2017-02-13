#!/bin/bash

templatepath=$1
controllerpath=$2
controllername=$3

cp -r "${templatepath}" "${controllerpath}"

files=$(find "${controllerpath}/" -mindepth 1 -type f)
for file in ${files}
    do
    	sed -i "s/template/${controllername}/g" ${file}
    	newfile=$(echo "${file}" | sed "s/template/${controllername}/g")
    	mv ${file} ${newfile} 2>/dev/null
    done