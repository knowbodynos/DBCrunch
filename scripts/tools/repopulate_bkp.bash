#!/bin/bash

modname=$1
controllername=$2
isrunning=$3

modpath="${SLURMONGO_ROOT}/modules/${modname}"

if ! test -d "${modpath}"
then
    echo "No directory \"${modpath}\"."
else
	controllerpath="${modpath}/${controllername}"
	if ! test -d "${controllerpath}"
	then
	    echo "No directory \"${controllerpath}\"."
	else
		if [ "${isrunning}" -eq 1 ]
		then
		    cat "${controllerpath}/jobs/*.stat" | grep -v ",\-1\:0," | sed 's/False/True/g' >> "${controllerpath}/skippedstate"
		else
			cat "${controllerpath}/jobs/*.stat" | sed 's/False/True/g' >> "${controllerpath}/skippedstate"
		fi
		#sed -i 's/False/True/g' "${controllerpath}/skippedstate" 2>/dev/null
		#jobfiles=$(find "${controllerpath}/jobs/" -maxdepth 1 -type f -name "*.job" 2>/dev/null)
		#for jobfile in $jobfiles
		#do
		#	outfile=${jobfile/.job/.out}
		#	outpath=$(echo "${outfile}" | rev | cut -d'/' -f1 --complement | rev)
		#    jobstepnames=$(cat "${jobfile}" | grep -E "^jobstepnames.*?=" | cut -d'=' -f2 | sed 's/"//g')
		#	if test -e $outfile
		#	then
		#	    for jobstepname in ${jobstepnames}
		#	    do
		#	    	logfile="${outpath}/${jobstepname}.log"
		#		    if ! test -e "${logfile}" || ! test -s "${logfile}" || ! grep -q "BSONSize: " "${logfile}"
		#		    then
		#		        echo "${jobstepname},0:0,True" >> "${controllerpath}/skippedstate"
		#		    fi
		#		done
		#	else
		#	    for jobstepname in ${jobstepnames}
		#	    do
		#	        echo "${jobstepname},0:0,True" >> "${controllerpath}/skippedstate"
		#		done
		#	fi
		#done
	fi
fi