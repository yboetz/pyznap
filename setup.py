#!/usr/bin/env python
"""
    setup
    ~~~~~~~~~~~~~~

    pyznap installation using setuptools.

    :copyright: (c) 2018 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import os
import re
from setuptools import setup


DIRNAME = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(DIRNAME, 'README.md'), 'r') as file:
    readme = file.read()

with open(os.path.join(DIRNAME, 'pyznap/__init__.py'), 'r') as file:
    version = re.search(r'__version__ = \'(.*?)\'', file.read()).group(1)

setup(
    name='pyznap',
    version=version,
    description='ZFS snapshot tool written in Python',
    long_description=readme,
    long_description_content_type="text/markdown",
    keywords='zfs snapshot backup',
    url='https://github.com/yboetz/pyznap',
    author='Yannick Boetzel',
    author_email='github@boetzel.ch',
    license='GPLv3',
    packages=['pyznap'],
    include_package_data=True,
    install_requires=[
        'configparser>=3.5.0',
        'paramiko>=2.4.2',
    ],
    extras_require={
        'dev': [
            'pytest==3.3.0',
            'pytest-dependency==0.2',
            'pytest-runner',
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: System :: Filesystems',
    ],
    entry_points = {
        'console_scripts': ['pyznap=pyznap.main:main'],
    },
    zip_safe=False
)
