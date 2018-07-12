#!/usr/bin/env pytest -v
"""
Created on Tue Nov 28 2017

@author: yboetz

Tests for pyznap
"""

import subprocess as sp
import sys
import os
import logging
from logging.config import fileConfig
from tempfile import NamedTemporaryFile
from datetime import datetime
import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../src')
import pyzfs as zfs
from utils import read_config, parse_name
from clean import clean_config
from take import take_config
from send import send_config
from process import DatasetNotFoundError


__dirname__ = os.path.dirname(os.path.abspath(__file__))
fileConfig(os.path.join(__dirname__, '../logging.ini'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def zpools():
    """Creates two temporary zpools to be called from test functions. Yields the two pool names
    and destroys them after testing."""

    zpool = '/sbin/zpool'
    pool0 = 'pyznap_test_source'
    pool1 = 'pyznap_test_dest'

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
                sp.check_call([zpool, 'create', pool, filename])
            except sp.CalledProcessError as err:
                logger.error(err)
                return

        try:
            fs0 = zfs.open(pool0)
            fs1 = zfs.open(pool1)
            assert fs0.name == pool0
            assert fs1.name == pool1
        except (DatasetNotFoundError, AssertionError, Exception) as err:
            logger.error(err)
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        for pool in [pool0, pool1]:
            try:
                sp.check_call([zpool, 'destroy', pool])
            except sp.CalledProcessError as err:
                logger.error(err)


class TestUtils(object):
    def test_read_config(self):
        with NamedTemporaryFile('w') as file:
            name = file.name
            file.write('[rpool/data]\n')
            file.write('hourly = 12\n')
            file.write('monthly = 0\n')
            file.write('clean = no\n')
            file.write('dest = backup/data, tank/data, rpool/data\n\n')

            file.write('[rpool]\n')
            file.write('frequent = 4\n')
            file.write('hourly = 24\n')
            file.write('daily = 7\n')
            file.write('weekly = 4\n')
            file.write('monthly = 12\n')
            file.write('yearly = 2\n')
            file.write('snap = yes\n')
            file.write('clean = yes\n')
            file.write('dest = backup, tank\n\n')

            file.write('[rpool/data_2]\n')
            file.write('daily = 14\n')
            file.write('yearly = 0\n')
            file.write('clean = yes\n')
            file.seek(0)

            config = read_config(name)
            conf0, conf1, conf2 = config

            assert conf0['name'] == 'rpool'
            assert conf0['key'] == None
            assert conf0['frequent'] == 4
            assert conf0['hourly'] == 24
            assert conf0['daily'] == 7
            assert conf0['weekly'] == 4
            assert conf0['monthly'] == 12
            assert conf0['yearly'] == 2
            assert conf0['snap'] == True
            assert conf0['clean'] == True
            assert conf0['dest'] == ['backup', 'tank']
            assert conf0['dest_keys'] == None

            assert conf1['name'] == 'rpool/data'
            assert conf1['key'] == None
            assert conf1['frequent'] == 4
            assert conf1['hourly'] == 12
            assert conf1['daily'] == 7
            assert conf1['weekly'] == 4
            assert conf1['monthly'] == 0
            assert conf1['yearly'] == 2
            assert conf1['snap'] == True
            assert conf1['clean'] == False
            assert conf1['dest'] == ['backup/data', 'tank/data', 'rpool/data']
            assert conf1['dest_keys'] == None

            assert conf2['name'] == 'rpool/data_2'
            assert conf2['key'] == None
            assert conf2['frequent'] == 4
            assert conf2['hourly'] == 24
            assert conf2['daily'] == 14
            assert conf2['weekly'] == 4
            assert conf2['monthly'] == 12
            assert conf2['yearly'] == 0
            assert conf2['snap'] == True
            assert conf2['clean'] == True
            assert conf2['dest'] == None
            assert conf2['dest_keys'] == None


    def test_parse_name(self):
        _type, fsname, user, host, port = parse_name('ssh:23:user@hostname:rpool/data')
        assert _type == 'ssh'
        assert fsname == 'rpool/data'
        assert user == 'user'
        assert host == 'hostname'
        assert port == 23

        _type, fsname, user, host, port = parse_name('rpool/data')
        assert _type == 'local'
        assert fsname == 'rpool/data'
        assert user == None
        assert host == None
        assert port == None


class TestSnapshot(object):
    @pytest.mark.dependency()
    def test_take_snapshot(self, zpools):
        fs, _ = zpools
        config = [{'name': fs.name, 'frequent': 1, 'hourly': 1, 'daily': 1, 'weekly': 1,
                   'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)
        take_config(config)

        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == 1


    @pytest.mark.dependency(depends=['test_take_snapshot'])
    def test_clean_snapshot(self, zpools):
        fs, _ = zpools
        config = [{'name': fs.name, 'frequent': 0, 'hourly': 0, 'daily': 0, 'weekly': 0,
                   'monthly': 0, 'yearly': 0, 'clean': True}]
        clean_config(config)

        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]


    @pytest.mark.dependency(depends=['test_clean_snapshot'])
    def test_take_snapshot_recursive(self, zpools):
        fs, _ = zpools
        fs.destroy(force=True)
        config = [{'name': fs.name, 'frequent': 1, 'hourly': 1, 'daily': 1, 'weekly': 1,
                   'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)
        fs.snapshots()[-1].destroy(force=True)
        fs.snapshots()[-1].destroy(force=True)

        sub1 = zfs.create('{:s}/sub1'.format(fs.name))
        abc = zfs.create('{:s}/sub1/abc'.format(fs.name))
        sub1_abc = zfs.create('{:s}/sub1_abc'.format(fs.name))
        config += [{'name': '{}/sub1'.format(fs), 'frequent': 1, 'hourly': 1, 'daily': 1, 'weekly': 1,
                    'monthly': 1, 'yearly': 1, 'snap': False}]
        take_config(config)

        # Check fs
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]

        # Check sub1
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub1.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]

        # Check abc
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in abc.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]

        # Check sub1_abc
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub1_abc.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]


    @pytest.mark.dependency(depends=['test_take_snapshot_recursive'])
    def test_clean_recursive(self, zpools):
        fs, _ = zpools
        fs.destroy(force=True)
        sub1 = zfs.create('{:s}/sub1'.format(fs.name))
        abc = zfs.create('{:s}/sub1/abc'.format(fs.name))
        abc_efg = zfs.create('{:s}/sub1/abc_efg'.format(fs.name))
        sub2 = zfs.create('{:s}/sub2'.format(fs.name))
        efg = zfs.create('{:s}/sub2/efg'.format(fs.name))
        hij = zfs.create('{:s}/sub2/efg/hij'.format(fs.name))
        klm = zfs.create('{:s}/sub2/efg/hij/klm'.format(fs.name))
        sub3 = zfs.create('{:s}/sub3'.format(fs.name))

        config = [{'name': fs.name, 'frequent': 1, 'hourly': 1, 'daily': 1, 'weekly': 1,
                   'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)

        config = [{'name': fs.name, 'frequent': 1, 'hourly': 0, 'daily': 1, 'weekly': 0,
                   'monthly': 0, 'yearly': 0, 'clean': True},
                  {'name': '{}/sub2'.format(fs), 'frequent': 0, 'hourly': 1, 'daily': 0,
                   'weekly': 1, 'monthly': 0, 'yearly': 1, 'clean': True},
                  {'name': '{}/sub3'.format(fs), 'frequent': 1, 'hourly': 0, 'daily': 1,
                   'weekly': 0, 'monthly': 1, 'yearly': 0, 'clean': False},
                  {'name': '{}/sub1/abc'.format(fs), 'frequent': 0, 'hourly': 0, 'daily': 0,
                   'weekly': 1, 'monthly': 1, 'yearly': 1, 'clean': True},
                  {'name': '{}/sub2/efg/hij'.format(fs), 'frequent': 0, 'hourly': 0,
                   'daily': 0, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True}]
        clean_config(config)

        # Check parent filesystem
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]
        # Check sub1
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub1.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]
        # Check sub1/abc
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in abc.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[3][snap_type]
        # Check sub1/abc_efg
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in abc_efg.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]
        # Check sub2
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub2.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[1][snap_type]
        # Check sub2/efg
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in efg.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[1][snap_type]
        # Check sub2/efg/hij
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in hij.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[4][snap_type]
        # Check sub2/efg/hij/klm
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in klm.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[4][snap_type]
        # Check sub3
        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub3.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == 1


class TestSending(object):
    @pytest.mark.dependency()
    def test_send_full(self, zpools):
        """Checks if send_snap totally replicates a filesystem"""
        fs0, fs1 = zpools
        fs0.destroy(force=True)
        fs1.destroy(force=True)
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        fs0.snapshot('snap0')
        zfs.create('{:s}/sub1'.format(fs0.name))
        fs0.snapshot('snap1', recursive=True)
        zfs.create('{:s}/sub2'.format(fs0.name))
        fs0.snapshot('snap2', recursive=True)
        zfs.create('{:s}/sub3'.format(fs0.name))
        fs0.snapshot('snap3', recursive=True)
        fs0.snapshot('snap4', recursive=True)
        fs0.snapshot('snap5', recursive=True)
        zfs.create('{:s}/sub3/abc'.format(fs0.name))
        fs0.snapshot('snap6', recursive=True)
        zfs.create('{:s}/sub3/abc_abc'.format(fs0.name))
        fs0.snapshot('snap7', recursive=True)
        zfs.create('{:s}/sub3/efg'.format(fs0.name))
        fs0.snapshot('snap8', recursive=True)
        fs0.snapshot('snap9', recursive=True)
        send_config(config)

        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_full'])
    def test_send_incremental(self, zpools):
        fs0, fs1 = zpools
        fs0.destroy(force=True)
        fs1.destroy(force=True)
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        fs0.snapshot('snap0', recursive=True)
        zfs.create('{:s}/sub1'.format(fs0.name))
        fs0.snapshot('snap1', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)

        zfs.create('{:s}/sub2'.format(fs0.name))
        fs0.snapshot('snap2', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)

        zfs.create('{:s}/sub3'.format(fs0.name))
        fs0.snapshot('snap3', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_incremental'])
    def test_send_delete_snapshot(self, zpools):
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        # Delete recent snapshots on dest
        fs1.snapshots()[-1].destroy(force=True)
        fs1.snapshots()[-1].destroy(force=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)

        # Delete recent snapshot on source
        fs0.snapshot('snap4', recursive=True)
        send_config(config)
        fs0.snapshots()[-1].destroy(force=True)
        fs0.snapshot('snap5', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_snapshot'])
    def test_send_delete_sub(self, zpools):
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        # Delete subfilesystems
        sub3 = fs1.filesystems()[-1]
        sub3.destroy(force=True)
        fs0.snapshot('snap6', recursive=True)
        sub2 = fs1.filesystems()[-1]
        sub2.destroy(force=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_sub'])
    def test_send_delete_old(self, zpools):
        fs0, fs1 = zpools
        config = [{'name': fs0.name, 'dest': [fs1.name]}]

        # Delete old snapshot on source
        fs0.snapshots()[0].destroy(force=True)
        fs0.snapshot('snap7', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'])[1:]]
        assert not (set(fs0_children) == set(fs1_children))
        # Assert that snap0 was not deleted from fs1
        for child in set(fs1_children) - set(fs0_children):
            assert child.endswith('snap0')
