#!/bin/bash

mainpath="/gss_gpfs_scratch/${USER}"
templatepath="${mainpath}/ToricCY/templates"

modname=$1
controllername=$2

modpath="${mainpath}/modules/${modname}"

if [ ! -d "${modpath}" ]
then
  mkdir ${modpath}
fi

controllerpath="${modpath}/${controllername}"

if [ ! -d "${controllerpath}" ]
then
  mkdir ${controllerpath}
fi

cp "${templatepath}/statusstate" "${controllerpath}/"
cp "${templatepath}/skippedstate" "${controllerpath}/"
cp "${templatepath}/counterstate" "${controllerpath}/"
cp "${templatepath}/setup" "${controllerpath}/"
cp "${templatepath}/controller_${modname}_template.job" "${controllerpath}/"

if [ ! -d "${controllerpath}/jobs" ]
then
  mkdir ${controllerpath}/jobs
fi

files=$(find "${controllerpath}/" -mindepth 1 -type f)
for file in ${files}
    do
    	sed -i "s/template/${controllername}/g" ${file}
    	newfile=$(echo "${file}" | sed "s/template/${controllername}/g")
    	mv ${file} ${newfile} 2>/dev/null
    done