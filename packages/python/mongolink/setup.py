#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

from setuptools import setup;

setup(name='mongolink',
      version='1.0',
      description='A Python API for streaming from a remote MongoDB database.',
      url='https://github.com/knowbodynos/SLURMongo',
      author='Ross Altman',
      author_email='knowbodynos@gmail.com',
      license='Northeastern University',
      packages=['mongolink'],
      zip_safe=False);