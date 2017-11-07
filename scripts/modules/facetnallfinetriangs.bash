#!/bin/bash

read line
while [ "${line}" != "" ]
do
    nf=$(echo ${line} | jq -r ".NORMALFORM")
    nallfinetriangs=$(echo ${nf} | sed 's/{0,0,0,0},//g' | sed 's/{/[/g' | sed 's/}/]/g' | timeout 3600 points2nallfinetriangs 2>/dev/null | head -c -1)
    echo "+MAXCONE.{\"NORMALFORM\":\"${nf}\"}>{\"FACETNTRIANG\":${nallfinetriangs}}"
    echo "@"
    sleep 0.01
    read line
done