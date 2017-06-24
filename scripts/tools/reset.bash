#!/bin/bash

herepath=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#mongolinkpath="${SLURMONGO_ROOT}/packages/python/mongolink"
toolspath="${SLURMONGO_ROOT}/scripts/tools"

rm ${herepath}/*.err ${herepath}/*.out 2>/dev/null
rm -r ${herepath}/jobs/* 2>/dev/null
rm ${herepath}/querystate* 2>/dev/null

echo "JobStep,ExitCode,Resubmit?" > ${herepath}/skippedstate 2>/dev/null
echo -e "BatchCounter,StepCounter\n1,1" > ${herepath}/counterstate 2>/dev/null
echo "Pending" > ${herepath}/statusstate 2>/dev/null

#currdir=$(pwd)
#cd ${mongolinkpath}
#python setup.py install --user --record filespy.txt
#sage --python setup.py install --user --record filessage.txt
#cd ${currdir}

mongouri=$(cat ${herepath}/controller*.job | grep "mongouri=" | cut -d'=' -f2 | sed 's/"//g')
basecollection=$(cat ${herepath}/controller*.job | grep "basecollection=" | cut -d'=' -f2 | sed 's/"//g')
modname=$(cat ${herepath}/controller*.job | grep "modname=" | cut -d'=' -f2 | sed 's/"//g')
markdone=$(cat ${herepath}/controller*.job | grep "markdone=" | cut -d'=' -f2 | sed 's/"//g')
h11=$(cat ${herepath}/controller*.job | grep "h11=" | cut -d'=' -f2 | sed 's/"//g')
python ${toolspath}/unmark.py "${basecollection}" "${modname}" "${markdone}" "{\"H11\":${h11}}"