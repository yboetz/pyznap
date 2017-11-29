#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 2017

@author: yboetz

Tests for pyznap
"""

import sys, os, pytest
import subprocess as sp
from tempfile import NamedTemporaryFile
from time import sleep, time
from datetime import datetime


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../pyznap')
import utils, zfs
from process import DatasetNotFoundError


logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

zpool = '/sbin/zpool'
pool0 = 'pyznap_test_0'
pool1 = 'pyznap_test_1'

@pytest.fixture(scope='module')
def zpools():
    """Creates two temporary zpools to be called from test functions. Yields the two pool names
    and destroys them after testing."""

    # Create temporary files on which the zpools are created
    with NamedTemporaryFile() as file0, NamedTemporaryFile() as file1:
        filename0 = file0.name
        filename1 = file1.name

        # Fix size to 100Mb
        file0.seek(100*1024**2-1)
        file0.write(b'0')
        file0.seek(0)
        file1.seek(100*1024**2-1)
        file1.write(b'0')
        file1.seek(0)
        
        # Create temporary test pools
        for pool, filename in zip([pool0, pool1], [filename0, filename1]):
            try:
                sp.check_call(['sudo', zpool, 'create', pool, filename])
            except sp.CalledProcessError as err:
                print('{:s} ERROR: {}'.format(logtime(), err))
                return

        try:
            fs0 = zfs.open(pool0)
            fs1 = zfs.open(pool1)
        except (DatasetNotFoundError, Exception) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        for pool in [pool0, pool1]:
            try:
                sp.check_call(['sudo', zpool, 'destroy', pool])
            except sp.CalledProcessError as err:
                print('{:s} ERROR: {}'.format(logtime(), err))


class TestSnapshot(object):
    def test_snap0(self, zpools):
        fs0, fs1 = zpools
        assert fs0.name == pool0
        assert fs1.name == pool1
        print('{:s} INFO: {:s}; {:s}'.format(logtime(), fs0.name, fs1.name))
    