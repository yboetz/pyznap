#!/usr/bin/env pytest -v
"""
    pyznap.test_cycle
    ~~~~~~~~~~~~~~

    Test pyznap runs over time

    :copyright: (c) 2018 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import subprocess as sp
import sys
import os
import logging
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
import pytest

import pyznap.pyzfs as zfs
from pyznap.utils import read_config, parse_name
from pyznap.clean import clean_config
from pyznap.take import take_config
from pyznap.send import send_config
from pyznap.process import DatasetNotFoundError


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%b %d %H:%M:%S')
logger = logging.getLogger(__name__)

ZPOOL = '/sbin/zpool'
POOL0 = 'pyznap_test_source'
POOL1 = 'pyznap_test_dest'

N_FREQUENT = 8
N_HOURLY = 24
N_DAILY = 14
N_WEEKLY = 8
N_MONTHLY = 12
N_YEARLY = 3


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
        for pool, filename in zip([POOL0, POOL1], [filename0, filename1]):
            try:
                sp.check_call([ZPOOL, 'create', pool, filename])
            except sp.CalledProcessError as err:
                logger.error(err)
                return

        try:
            fs0 = zfs.open(POOL0)
            fs1 = zfs.open(POOL1)
            assert fs0.name == POOL0
            assert fs1.name == POOL1
        except (DatasetNotFoundError, AssertionError, Exception) as err:
            logger.error(err)
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        for pool in [POOL0, POOL1]:
            try:
                sp.check_call([ZPOOL, 'destroy', pool])
            except sp.CalledProcessError as err:
                logger.error(err)

@pytest.fixture(scope='module')
def config():
    with NamedTemporaryFile('w') as file:
        file.write(f'[{POOL0}]\n'
                   f'frequent = {N_FREQUENT}\n'
                   f'hourly = {N_HOURLY}\n'
                   f'daily = {N_DAILY}\n'
                   f'weekly = {N_WEEKLY}\n'
                   f'monthly = {N_MONTHLY}\n'
                   f'yearly = {N_YEARLY}\n'
                   f'snap = yes\n'
                   f'clean = yes\n'
                   f'dest = {POOL1}\n\n'

                   f'[{POOL1}]\n'
                   f'frequent = {2*N_FREQUENT}\n'
                   f'hourly = {2*N_HOURLY}\n'
                   f'daily = {2*N_DAILY}\n'
                   f'weekly = {2*N_WEEKLY}\n'
                   f'monthly = {2*N_MONTHLY}\n'
                   f'yearly = {2*N_YEARLY}\n'
                   f'clean = yes\n')
        file.seek(0)
        yield file.name


class TestCycle(object):
    def test_3_days(self, zpools, config):
        start_date = datetime(2014, 1, 1)
        day = start_date.day

        dates = [start_date + i * timedelta(minutes=15) for i in range(4*24*3)]
        fs0, fs1 = zpools

        for n,date in enumerate(dates):
            snapshots_ref = {'frequent': N_FREQUENT, 'hourly': N_HOURLY, 'daily': N_DAILY,
                             'weekly': N_WEEKLY, 'monthly': N_MONTHLY, 'yearly': N_YEARLY}

            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']
            pyznap_send = faketime + ['pyznap', '--config', config, 'send']

            # only do one send per day
            if date.day == day:
                day = (date + timedelta(days=1)).day
                _, _ = Popen(pyznap_send).communicate()

            _, _ = Popen(pyznap_snap).communicate()

            # check pool0
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs0.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= snapshots_ref[snap_type]

            # check pool1
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs1.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= 2*snapshots_ref[snap_type]
