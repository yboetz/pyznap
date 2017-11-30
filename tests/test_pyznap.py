#!/home/yboetz/.virtualenvs/pyznap/bin/pytest -v
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 2017

@author: yboetz

Tests for pyznap
"""

import subprocess as sp
import sys
import os
from tempfile import NamedTemporaryFile
from datetime import datetime
import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../src')
import utils
import zfs
from process import DatasetNotFoundError


logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')


@pytest.fixture(scope='module')
def zpools():
    """Creates two temporary zpools to be called from test functions. Yields the two pool names
    and destroys them after testing."""

    zpool = '/sbin/zpool'
    pool0 = 'pyznap_test_0'
    pool1 = 'pyznap_test_1'

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
            assert fs0.name == pool0
            assert fs1.name == pool1
        except (DatasetNotFoundError, AssertionError, Exception) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        for pool in [pool0, pool1]:
            try:
                sp.check_call(['sudo', zpool, 'destroy', pool])
            except sp.CalledProcessError as err:
                print('{:s} ERROR: {}'.format(logtime(), err))


class TestUtils(object):
    def test_read_config(self):
        with NamedTemporaryFile('w') as file:
            name = file.name
            file.write('[rpool/data]\n')
            file.write('hourly = 24\n')
            file.write('daily = 7\n')
            file.write('weekly = 4\n')
            file.write('monthly = 12\n')
            file.write('yearly = 2\n')
            file.write('snap = yes\n')
            file.write('clean = no\n')
            file.write('dest = backup/data, tank/data, rpool/data\n')
            file.seek(0)

            config = utils.read_config(name)[0]
            assert config['name'] == 'rpool/data'
            assert config['key'] == None
            assert config['hourly'] == 24
            assert config['daily'] == 7
            assert config['weekly'] == 4
            assert config['monthly'] == 12
            assert config['yearly'] == 2
            assert config['snap'] == True
            assert config['clean'] == False
            assert config['dest'] == ['backup/data', 'tank/data', 'rpool/data']
            assert config['dest_keys'] == None


    def test_parse_name(self):
        _type, fsname, user, host, port = utils.parse_name('ssh:23:user@hostname:rpool/data')
        assert _type == 'ssh'
        assert fsname == 'rpool/data'
        assert user == 'user'
        assert host == 'hostname'
        assert port == 23

        _type, fsname, user, host, port = utils.parse_name('rpool/data')
        assert _type == 'local'
        assert fsname == 'rpool/data'
        assert user == None
        assert host == None
        assert port == None


class TestSnapshot(object):
    def test_take_snapshot(self, zpools):
        fs, _ = zpools
        config = [{'name': fs.name, 'hourly': 1, 'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1,
                  'snap': 'yes'}]
        utils.take_snap(config)

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == 1


    @pytest.mark.dependency(depends=['test_take_snapshot'])
    def test_clean_snapshot(self, zpools):
        fs, _ = zpools
        config = [{'name': fs.name, 'hourly': 0, 'daily': 0, 'weekly': 0, 'monthly': 0, 'yearly': 0,
                  'clean': 'yes'}]
        utils.clean_snap(config)

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]


class TestSending(object):
    def test_send_full(self, zpools):
        """Checks if send_snap totally replicates a filesystem"""
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        # Full stream
        fs0.snapshot('snap0')
        utils.send_snap(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert fs0_children == fs1_children


    @pytest.mark.dependency(depends=['test_send_full'])
    def test_send_incremental(self, zpools):
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        zfs.create('{:s}/sub1'.format(fs0.name))
        fs0.snapshot('snap1', recursive=True)
        utils.send_snap(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert fs0_children == fs1_children

        zfs.create('{:s}/sub2'.format(fs0.name))
        fs0.snapshot('snap2', recursive=True)
        utils.send_snap(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert fs0_children == fs1_children

        zfs.create('{:s}/sub3'.format(fs0.name))
        fs0.snapshot('snap3', recursive=True)
        utils.send_snap(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert fs0_children == fs1_children


    @pytest.mark.dependency(depends=['test_send_incremental'])
    def test_send_delete_old(self, zpools):
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]
        # Delete old snapshot
        fs0.snapshots()[0].destroy(force=True)
        fs0.snapshot('snap4', recursive=True)
        utils.send_snap(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        # This should not be equal. Atm replication (-R) deletes snapshots on dest which were deleted on source
        assert (fs0_children == fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_old'])
    def test_send_delete_recent(self, zpools):
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        # Delete old snapshot
        fs1.snapshots()[-1].destroy(force=True)
        fs1.snapshots()[-1].destroy(force=True)
        utils.send_snap(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert (fs0_children == fs1_children)
