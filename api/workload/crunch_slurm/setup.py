#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

from setuptools import setup;

setup(name='crunch_slurm',
      version='1.0',
      description='An API that interfaces with the SLURM workload manager.',
      url='https://github.com/knowbodynos/DBCrunch',
      author='Ross Altman',
      author_email='knowbodynos@gmail.com',
      license='GPLv3',
      packages=['crunch_slurm'],
      zip_safe=False);