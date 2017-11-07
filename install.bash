#!/bin/bash

initfile=$(math -noprompt -run "WriteString[\$Output,\$UserBaseDirectory<>\"/Kernel/init.m\"];Exit[];" 2>/dev/null)
echo $(cat $initfile | grep -v "SLURMONGO_ROOT") > $initfile
echo "AppendTo[\$Path, Environment[\"SLURMONGO_ROOT\"]<>\"/packages/Mathematica/cohomCalg\"]" >> $initfile
echo "AppendTo[\$Path, Environment[\"SLURMONGO_ROOT\"]<>\"/packages/Mathematica/MongoLink\"]" >> $initfile

pythonpath="${SLURMONGO_ROOT}/packages/python"

currdir=$(pwd)
cd ${pythonpath}
rm -r ${pythonpath}/pymongo 2>/dev/null
git clone git://github.com/mongodb/mongo-python-driver.git pymongo
wait
cd pymongo
git checkout 280efd2d726aa589b8656031f3453d0a7903e1ff
python setup.py install --user --record filespy.txt
sage --python setup.py install --user --record filessage.txt

cd ${pythonpath}
rm -r ${pythonpath}/mpi4py 2>/dev/null
git clone https://github.com/mpi4py/mpi4py.git mpi4py
wait
cd mpi4py
git checkout edd4ada44f343ffdfee9d09be5de28d9623216d8
python setup.py install --user --record filespy.txt
sage --python setup.py install --user --record filessage.txt

cd ${pythonpath}/mongolink
python setup.py install --user --record filespy.txt
sage --python setup.py install --user --record filessage.txt
cd ${currdir}

cd ${M2_ROOT}
wget http://www2.macaulay2.com/Macaulay2/Downloads/GNU-Linux/Generic/Macaulay2-1.6-x86_64-Linux-Generic.tar.gz
wget http://www2.macaulay2.com/Macaulay2/Downloads/Common/Macaulay2-1.6-common.tar.gz
wait
tar xzfv Macaulay2-1.6-x86_64-Linux-Generic.tar.gz
tar xzfv Macaulay2-1.6-common.tar.gz
rm *.tar.gz
cd ${currdir}