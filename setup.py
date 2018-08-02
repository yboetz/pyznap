"""
Created on Wed Aug 01 2018

@author: yboetz
"""

import os
import re
from setuptools import setup, find_packages


DIRNAME = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(DIRNAME, 'README.md'), 'r') as file:
    readme = file.read()

with open(os.path.join(DIRNAME, 'pyznap/__init__.py'), 'r') as file:
    version = re.search(r'__version__ = \'(.*?)\'', file.read()).group(1)

setup(
    name='pyznap',
    version=version,
    description=' ZFS snapshot tool written in Python',
    long_description=readme,
    url='https://github.com/yboetz/pyznap',
    author='Yannick Boetzel',
    author_email='github@boetzel.ch',
    license='GPLv3',
    #package_dir={'': 'pyznap'},
    packages=['pyznap'],
    include_package_data=True,
    # data_files=[('.', 'logging.ini')],
    install_requires=[
        'configparser>=3.5.0',
        'paramiko>=2.4.1',
    ],
    extras_require={
        'dev': [
            'pytest>=3',
            'pytest-dependency>=0.2',
        ]
    },
    entry_points = {
        'console_scripts': ['pyznap=pyznap.pyznap:main'],
    },
    zip_safe=False
)