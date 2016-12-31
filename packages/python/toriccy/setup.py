#!/shared/apps/python/Python-2.7.5/INSTALL/bin/python

from setuptools import setup;

setup(name='toriccy',
      version='1.0',
      description='Query the ToricCY MongoDB database.',
      url='https://github.com/knowbodynos/toriccy',
      author='Ross Altman',
      author_email='knowbodynos@gmail.com',
      license='Northeastern University',
      packages=['toriccy'],
      zip_safe=False);