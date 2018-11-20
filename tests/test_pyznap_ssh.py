#!/usr/bin/env pytest -v
"""
    pyznap.test_pyznap_ssh
    ~~~~~~~~~~~~~~

    Test pyznap over time (ssh).

    :copyright: (c) 2018 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import subprocess as sp
import sys
import os
import random
import string
import logging
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
import pytest

import pyznap.pyzfs as zfs
from pyznap.utils import open_ssh, exists
from pyznap.process import check_output, DatasetNotFoundError


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%b %d %H:%M:%S')
logger = logging.getLogger(__name__)

assert exists('faketime')

def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# ssh connection to dest
USER = 'root'
HOST = '127.0.0.1'
PORT = 22
KEY = None

ZPOOL = '/sbin/zpool'
POOL0 = 'pyznap_test_source'
POOL1 = 'pyznap_test_dest'

N_FREQUENT = 30
N_HOURLY = 24
N_DAILY = 14
N_WEEKLY = 8
N_MONTHLY = 12
N_YEARLY = 3

SNAPSHOTS_REF = {'frequent': N_FREQUENT, 'hourly': N_HOURLY, 'daily': N_DAILY, 'weekly': N_WEEKLY,
                 'monthly': N_MONTHLY, 'yearly': N_YEARLY}


@pytest.fixture(scope='module')
def zpools():
    """Creates two temporary zpools to be called from test functions, source is local and dest on
    remote ssh location. Yields the two pool names and destroys them after testing."""

    sftp_filename = '/tmp/' + randomword(10)

    # ssh arguments for zfs functions
    ssh = open_ssh(USER, HOST, port=PORT, key=KEY)
    sftp = ssh.open_sftp()

    # Create temporary file on which the source zpool is created. Manually create sftp file
    with NamedTemporaryFile() as file0, sftp.open(sftp_filename, 'w') as file1:
        filename0 = file0.name
        filename1 = sftp_filename

        # Fix size to 100Mb
        file0.seek(100*1024**2-1)
        file0.write(b'0')
        file0.seek(0)
        file1.seek(100*1024**2-1)
        file1.write(b'0')
        file1.seek(0)
        
        # Create temporary test pools
        try:
            check_output([ZPOOL, 'create', POOL0, filename0])
        except sp.CalledProcessError as err:
            logger.error(err)
            return

        try:
            check_output([ZPOOL, 'create', POOL1, filename1], ssh=ssh)
        except sp.CalledProcessError as err:
            logger.error(err)
            return

        try:
            fs0 = zfs.open(POOL0)
            fs1 = zfs.open(POOL1, ssh=ssh)
            assert fs0.name == POOL0
            assert fs1.name == POOL1
        except (DatasetNotFoundError, AssertionError, Exception) as err:
            logger.error(err)
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        try:
            check_output([ZPOOL, 'destroy', POOL0])
        except sp.CalledProcessError as err:
            logger.error(err)

        try:
            check_output([ZPOOL, 'destroy', POOL1], ssh=ssh)
        except sp.CalledProcessError as err:
            logger.error(err)

    # Delete tempfile on dest
    sftp.remove(sftp_filename)
    sftp.close()
    ssh.close()


@pytest.fixture(scope='module')
def config():
    """Creates a temporary config file and yields its filename"""

    with NamedTemporaryFile('w') as file:
        file.write(f'[ssh:{PORT}:{USER}@{HOST}:{POOL1}]\n'
                   f'frequent = {N_FREQUENT}\n'
                   f'hourly = {N_HOURLY}\n'
                   f'daily = {N_DAILY}\n'
                   f'weekly = {N_WEEKLY}\n'
                   f'monthly = {N_MONTHLY}\n'
                   f'yearly = {N_YEARLY}\n'
                   f'snap = yes\n'
                   f'clean = yes\n\n')
        file.seek(0)
        yield file.name


@pytest.fixture(scope='module')
def config_send():
    """Creates a temporary config file and yields its filename"""

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
                   f'dest = ssh:{PORT}:{USER}@{HOST}:{POOL1}\n'

                   f'[ssh:{PORT}:{USER}@{HOST}:{POOL1}]\n'
                   f'frequent = {N_FREQUENT}\n'
                   f'hourly = {N_HOURLY}\n'
                   f'daily = {N_DAILY}\n'
                   f'weekly = {N_WEEKLY}\n'
                   f'monthly = {N_MONTHLY}\n'
                   f'yearly = {N_YEARLY}\n'
                   f'clean = yes\n\n')
        file.seek(0)
        yield file.name


@pytest.mark.slow
class TestCycle(object):
    def test_2_hours(self, zpools, config):
        """Tests pyznap over 2 hours and checks if the correct amount of 'frequent' snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2014, 1, 1)
        dates = [start_date + i * timedelta(minutes=1) for i in range(60*2)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 15min
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']


    def test_2_days(self, zpools, config):
        """Tests pyznap over 2 days and checks if the correct amount of 'frequent' snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2014, 1, 1)
        dates = [start_date + i * timedelta(minutes=15) for i in range(4*24*2)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 15min
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']


    def test_1_week(self, zpools, config):
        """Tests pyznap over 1 week and checks if the correct amount of 'frequent' & hourly'
        snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2014, 1, 1)
        dates = [start_date + i * timedelta(hours=1) for i in range(24*7)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 1h
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N_HOURLY runs there are N_HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']


    def test_8_weeks(self, zpools, config):
        """Tests pyznap over 8 weeks and checks if the correct amount of 'frequent', 'hourly' &
        'daily' snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2014, 1, 1)
        dates = [start_date + i * timedelta(days=1) for i in range(7*8)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 1d
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N-HOURLY runs there are N-HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']
            # check if after N_DAILY runs there are N_DAILY 'daily' snapshots
            if n+1 >= N_DAILY:
                assert len(snapshots['daily']) == SNAPSHOTS_REF['daily']


    def test_6_months(self, zpools, config):
        """Tests pyznap over 6 months and checks if the correct amount of 'frequent', 'hourly',
        'daily' & 'weekly' snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2014, 1, 1)
        dates = [start_date + i * timedelta(days=7) for i in range(4*6)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 7d
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N-HOURLY runs there are N-HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']
            # check if after N_DAILY runs there are N_DAILY 'daily' snapshots
            if n+1 >= N_DAILY:
                assert len(snapshots['daily']) == SNAPSHOTS_REF['daily']
            # check if after N_WEEKLY runs there are N_WEEKLY 'weekly' snapshots
            if n+1 >= N_WEEKLY:
                assert len(snapshots['weekly']) == SNAPSHOTS_REF['weekly']


    def test_3_years(self, zpools, config):
        """Tests pyznap over 3 years and checks if the correct amount of 'frequent', 'hourly',
        'daily', 'weekly' & 'monthly' snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2014, 1, 1)
        dates = [start_date + i * timedelta(days=31) for i in range(12*3)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 31d
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N-HOURLY runs there are N-HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']
            # check if after N_DAILY runs there are N_DAILY 'daily' snapshots
            if n+1 >= N_DAILY:
                assert len(snapshots['daily']) == SNAPSHOTS_REF['daily']
            # check if after N_WEEKLY runs there are N_WEEKLY 'weekly' snapshots
            if n+1 >= N_WEEKLY:
                assert len(snapshots['weekly']) == SNAPSHOTS_REF['weekly']
            # check if after N_MONTHLY runs there are N_MONTHLY 'monthly' snapshots
            if n+1 >= N_MONTHLY:
                assert len(snapshots['monthly']) == SNAPSHOTS_REF['monthly']


    def test_100_years(self, zpools, config):
        """Tests pyznap over 100 years and checks if the correct amount of 'frequent', 'hourly',
        'daily', 'weekly', 'monthly' & 'yearly' snapshots are taken"""

        _, fs = zpools
        fs.destroy(force=True)

        # have to start at 1969 as faketime only goes from 1969 to 2068
        dates = [datetime(1969 + i, 1, 1) for i in range(100)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 1y
            _, _ = Popen(pyznap_snap).communicate()

            # get all snapshots
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N-HOURLY runs there are N-HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']
            # check if after N_DAILY runs there are N_DAILY 'daily' snapshots
            if n+1 >= N_DAILY:
                assert len(snapshots['daily']) == SNAPSHOTS_REF['daily']
            # check if after N_WEEKLY runs there are N_WEEKLY 'weekly' snapshots
            if n+1 >= N_WEEKLY:
                assert len(snapshots['weekly']) == SNAPSHOTS_REF['weekly']
            # check if after N_MONTHLY runs there are N_MONTHLY 'monthly' snapshots
            if n+1 >= N_MONTHLY:
                assert len(snapshots['monthly']) == SNAPSHOTS_REF['monthly']
            # check if after N_YEARLY runs there are N_YEARLY 'yearly' snapshots
            if n+1 >= N_YEARLY:
                assert len(snapshots['yearly']) == SNAPSHOTS_REF['yearly']


@pytest.mark.slow
class TestSend(object):
    def test_50_years(self, zpools, config_send):
        """Tests pyznap over 50 years and checks if snapshots are sent correctly"""

        fs0, fs1 = zpools
        ssh = fs1.ssh
        fs0.destroy(force=True)
        fs1.destroy(force=True)
        zfs.create('{:s}/sub1'.format(fs0.name))

        # have to start at 1969 as faketime only goes from 1969 to 2068
        dates = [datetime(1969 + i, 1, 1) for i in range(50)]

        for n,date in enumerate(dates):
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_take = faketime + ['pyznap', '--config', config_send, 'snap', '--take']
            pyznap_clean = faketime + ['pyznap', '--config', config_send, 'snap', '--clean']
            pyznap_send = faketime + ['pyznap', '--config', config_send, 'send']

            # take, send & clean snaps every 1y
            _, _ = Popen(pyznap_take).communicate()
            _, _ = Popen(pyznap_send).communicate()
            _, _ = Popen(pyznap_clean).communicate()

            # get all snapshots on fs0
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs0.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N-HOURLY runs there are N-HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']
            # check if after N_DAILY runs there are N_DAILY 'daily' snapshots
            if n+1 >= N_DAILY:
                assert len(snapshots['daily']) == SNAPSHOTS_REF['daily']
            # check if after N_WEEKLY runs there are N_WEEKLY 'weekly' snapshots
            if n+1 >= N_WEEKLY:
                assert len(snapshots['weekly']) == SNAPSHOTS_REF['weekly']
            # check if after N_MONTHLY runs there are N_MONTHLY 'monthly' snapshots
            if n+1 >= N_MONTHLY:
                assert len(snapshots['monthly']) == SNAPSHOTS_REF['monthly']
            # check if after N_YEARLY runs there are N_YEARLY 'yearly' snapshots
            if n+1 >= N_YEARLY:
                assert len(snapshots['yearly']) == SNAPSHOTS_REF['yearly']

            # check if filesystem is completely replicated on dest
            fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
            fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
            assert set(fs0_children) == set(fs1_children)


    def test_create_new(self, zpools, config_send):
        """Tests pyznap over 10 years and checks if newly created filesystems are correctly
        replicated"""

        fs0, fs1 = zpools
        ssh = fs1.ssh
        fs0.destroy(force=True)
        fs1.destroy(force=True)

        # have to start at 1969 as faketime only goes from 1969 to 2068
        dates = [datetime(1969 + i, 1, 1) for i in range(10)]

        for n,date in enumerate(dates):
            # at every step create a new subfilesystem
            zfs.create('{:s}/sub{:d}'.format(fs0.name, n))

            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_take = faketime + ['pyznap', '--config', config_send, 'snap', '--take']
            pyznap_clean = faketime + ['pyznap', '--config', config_send, 'snap', '--clean']
            pyznap_send = faketime + ['pyznap', '--config', config_send, 'send']

            # take, send & clean snaps every 1y
            _, _ = Popen(pyznap_take).communicate()
            _, _ = Popen(pyznap_send).communicate()
            _, _ = Popen(pyznap_clean).communicate()

            # get all snapshots on fs0
            snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
            for snap in fs0.snapshots():
                snap_type = snap.name.split('_')[-1]
                snapshots[snap_type].append(snap)
            # check if there are not too many snapshots taken
            for snap_type, snaps in snapshots.items():
                assert len(snaps) <= SNAPSHOTS_REF[snap_type]
            # check if after N_FREQUENT runs there are N_FREQUENT 'frequent' snapshots
            if n+1 >= N_FREQUENT:
                assert len(snapshots['frequent']) == SNAPSHOTS_REF['frequent']
            # check if after N-HOURLY runs there are N-HOURLY 'hourly' snapshots
            if n+1 >= N_HOURLY:
                assert len(snapshots['hourly']) == SNAPSHOTS_REF['hourly']
            # check if after N_DAILY runs there are N_DAILY 'daily' snapshots
            if n+1 >= N_DAILY:
                assert len(snapshots['daily']) == SNAPSHOTS_REF['daily']
            # check if after N_WEEKLY runs there are N_WEEKLY 'weekly' snapshots
            if n+1 >= N_WEEKLY:
                assert len(snapshots['weekly']) == SNAPSHOTS_REF['weekly']
            # check if after N_MONTHLY runs there are N_MONTHLY 'monthly' snapshots
            if n+1 >= N_MONTHLY:
                assert len(snapshots['monthly']) == SNAPSHOTS_REF['monthly']
            # check if after N_YEARLY runs there are N_YEARLY 'yearly' snapshots
            if n+1 >= N_YEARLY:
                assert len(snapshots['yearly']) == SNAPSHOTS_REF['yearly']

            # check if filesystem is completely replicated on dest
            fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
            fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
            assert set(fs0_children) == set(fs1_children)


@pytest.mark.slow
class TestSpecialCases(object):
    def test_winter_time(self, zpools, config):
        """Tests if pyznap does not crash when switching to winter time"""

        _, fs = zpools
        fs.destroy(force=True)

        start_date = datetime(2018, 10, 28, 2, 0, 0)
        dates = [start_date + i * timedelta(minutes=15) for i in range(4)]

        for date in dates:
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 15min
            _, _ = Popen(pyznap_snap).communicate()

        start_date = datetime(2018, 10, 28, 2, 0, 0)
        dates = [start_date + i * timedelta(minutes=15) for i in range(8)]

        for date in dates:
            faketime = ['faketime', date.strftime('%y-%m-%d %H:%M:%S')]
            pyznap_snap = faketime + ['pyznap', '--config', config, 'snap']

            # take snaps every 15min
            _, _ = Popen(pyznap_snap).communicate()
