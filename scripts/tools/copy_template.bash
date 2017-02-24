#!/bin/bash

templatepath="${SLURMONGO_ROOT}/templates"
toolspath="${SLURMONGO_ROOT}/scripts/tools"

modname=$1
controllername=$2

modpath="${SLURMONGO_ROOT}/modules/${modname}"

if [ ! -d "${modpath}" ]
then
  mkdir ${modpath}
fi

controllerpath="${modpath}/${controllername}"

if [ ! -d "${controllerpath}" ]
then
  mkdir ${controllerpath}
fi

cp "${toolspath}/reset.bash" "${controllerpath}/"
cp "${templatepath}/controller_${modname}_template.job" "${controllerpath}/"
echo "JobStep,ExitCode,Resubmit?" > ${controllerpath}/skippedstate 2>/dev/null
echo -e "BatchCounter,StepCounter\n1,1" > ${controllerpath}/counterstate 2>/dev/null
echo "Pending" > ${controllerpath}/statusstate 2>/dev/null

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