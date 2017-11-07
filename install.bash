#!/bin/bash

initfile=$(math -noprompt -run "WriteString[\$Output,\$UserBaseDirectory<>\"/Kernel/init.m\"];Exit[];" 2>/dev/null)
echo $(cat $initfile | grep -v "SLURMONGO_ROOT") > $initfile
echo "AppendTo[\$Path, Environment[\"SLURMONGO_ROOT\"]<>\"/packages/Mathematica/cohomCalg\"]" >> $initfile
echo "AppendTo[\$Path, Environment[\"SLURMONGO_ROOT\"]<>\"/packages/Mathematica/MongoLink\"]" >> $initfile

localpath="${USER_LOCAL}"
pythonpath="${SLURMONGO_ROOT}/packages/python"

currdir=$(pwd)
cd ${SLURMONGO_ROOT}
git submodule update --init --recursive

if $(python -c "import mpi4py" |& grep -q "ImportError" && echo true || echo false)
then
	cd ${pythonpath}
	rm -r ${pythonpath}/mpi4py 2>/dev/null
	git clone https://github.com/mpi4py/mpi4py.git mpi4py
	wait
	cd mpi4py
	git checkout 2.0.0
	wait
	rm -rf .git
	python setup.py install --user --record filespy.txt
	sage --python setup.py install --user --record filessage.txt
fi

if $(python -c "import pymongo" |& grep -q "ImportError" && echo true || echo false)
then
	cd ${pythonpath}
	rm -r ${pythonpath}/pymongo 2>/dev/null
	git clone https://github.com/mongodb/mongo-python-driver.git pymongo
	wait
	cd pymongo
	git checkout 3.5.1
	wait
	rm -rf .git
	python setup.py install --user --record filespy.txt
	sage --python setup.py install --user --record filessage.txt
fi

cd ${pythonpath}/mongolink
python setup.py install --user --record filespy.txt
sage --python setup.py install --user --record filessage.txt
cd ${currdir}

if [ "$(command -v M2)" != "" ]
then
	cd ${HOME}
	wget http://www2.macaulay2.com/Macaulay2/Downloads/GNU-Linux/Generic/Macaulay2-1.6-x86_64-Linux-Generic.tar.gz
	wget http://www2.macaulay2.com/Macaulay2/Downloads/Common/Macaulay2-1.6-common.tar.gz
	wait
	tar xzfv Macaulay2-1.6-x86_64-Linux-Generic.tar.gz --strip-components 1 --directory ${localpath}/
	tar xzfv Macaulay2-1.6-common.tar.gz --strip-components 1 --directory ${localpath}/
	wait
	rm Macaulay2*.tar.gz
fi

if [ "$(command -v points2ntriangs)" != "" ]
then
	cd ${HOME}
	wget http://www.rambau.wm.uni-bayreuth.de/Software/TOPCOM-0.17.8.tar.gz
	wait
	tar xzfv TOPCOM-0.17.8.tar.gz
	wait
	rm TOPCOM-0.17.8.tar.gz
	cd topcom-0.17.8
	./configure --prefix=${localpath}
	wait
	make
	wait
	make install
	wait
	cd ..
	rm -r topcom-0.17.8
fi

if [ "$(command -v jq)" != "" ]
then
	cd ${localpath}/bin
	wget http://stedolan.github.io/jq/download/linux64/jq
	wait
	chmod +x jq
fi

cd ${currdir}