#!/usr/bin/env pytest -v
"""
Created on Sat Dec 09 2017

@author: yboetz

ssh tests for pyznap
"""

import subprocess as sp
import sys
import os
import random
import string
import logging
from logging.config import fileConfig
from tempfile import NamedTemporaryFile
from datetime import datetime
import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../src')
import pyzfs as zfs
from utils import open_ssh, read_config, parse_name
from clean import clean_config
from take import take_config
from send import send_config
from process import DatasetNotFoundError
import process


__dirname__ = os.path.dirname(os.path.abspath(__file__))
fileConfig(os.path.join(__dirname__, '../logging.ini'), disable_existing_loggers=False)
logger = logging.getLogger(__name__)

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

# ssh connection to dest
USER = 'root'
HOST = '127.0.0.1'
PORT = 22
KEY = None


@pytest.fixture(scope='module')
def zpools():
    """Creates two temporary zpools to be called from test functions, source is local and dest on
    remote ssh locatoin. Yields the two pool names and destroys them after testing."""

    zpool = '/sbin/zpool'
    pool0 = 'pyznap_test_source'
    pool1 = 'pyznap_test_dest'

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
            sp.check_output([zpool, 'create', pool0, filename0])
        except sp.CalledProcessError as err:
            logger.error(err)
            return

        try:
            sp.check_output([zpool, 'create', pool1, filename1], ssh=ssh)
        except sp.CalledProcessError as err:
            logger.error(err)
            return

        try:
            fs0 = zfs.open(pool0)
            fs1 = zfs.open(pool1, ssh=ssh)
            assert fs0.name == pool0
            assert fs1.name == pool1
        except (DatasetNotFoundError, AssertionError, Exception) as err:
            logger.error(err)
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        try:
            sp.check_output([zpool, 'destroy', pool0])
        except sp.CalledProcessError as err:
            logger.error(err)

        try:
            sp.check_output([zpool, 'destroy', pool1], ssh=ssh)
        except sp.CalledProcessError as err:
            logger.error(err)

    # Delete tempfile on dest
    sftp.remove(sftp_filename)
    sftp.close()
    ssh.close()


class TestSnapshot(object):
    @pytest.mark.dependency()
    def test_take_snapshot(self, zpools):
        _, fs = zpools
        ssh = fs.ssh

        config = [{'name': 'ssh:{:d}:{}'.format(PORT, fs), 'key': KEY, 'frequent': 1, 'hourly': 1,
                   'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': True}]
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
        _, fs = zpools
        ssh = fs.ssh

        config = [{'name': 'ssh:{:d}:{}'.format(PORT, fs), 'key': KEY, 'frequent': 0, 'hourly': 0,
                   'daily': 0, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True}]
        clean_config(config)

        snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]


    @pytest.mark.dependency(depends=['test_clean_snapshot'])
    def test_take_snapshot_recursive(self, zpools):
        _, fs = zpools
        ssh = fs.ssh

        fs.destroy(force=True)
        config = [{'name': 'ssh:{:d}:{}'.format(PORT, fs), 'key': KEY, 'frequent': 1, 'hourly': 1,
                   'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)
        fs.snapshots()[-1].destroy(force=True)
        fs.snapshots()[-1].destroy(force=True)

        sub1 = zfs.create('{:s}/sub1'.format(fs.name), ssh=ssh)
        abc = zfs.create('{:s}/sub1/abc'.format(fs.name), ssh=ssh)
        sub1_abc = zfs.create('{:s}/sub1_abc'.format(fs.name), ssh=ssh)
        config += [{'name': 'ssh:{:d}:{}/sub1'.format(PORT, fs), 'key': KEY, 'frequent': 1, 'hourly': 1,
                    'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': False}]
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
        _, fs = zpools
        ssh = fs.ssh

        fs.destroy(force=True)
        sub1 = zfs.create('{:s}/sub1'.format(fs.name), ssh=ssh)
        abc = zfs.create('{:s}/sub1/abc'.format(fs.name), ssh=ssh)
        abc_efg = zfs.create('{:s}/sub1/abc_efg'.format(fs.name), ssh=ssh)
        sub2 = zfs.create('{:s}/sub2'.format(fs.name), ssh=ssh)
        efg = zfs.create('{:s}/sub2/efg'.format(fs.name), ssh=ssh)
        hij = zfs.create('{:s}/sub2/efg/hij'.format(fs.name), ssh=ssh)
        klm = zfs.create('{:s}/sub2/efg/hij/klm'.format(fs.name), ssh=ssh)
        sub3 = zfs.create('{:s}/sub3'.format(fs.name), ssh=ssh)

        config = [{'name': 'ssh:{:d}:{}'.format(PORT, fs), 'key': KEY, 'frequent': 1, 'hourly': 1,
                   'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)

        config = [{'name': 'ssh:{:d}:{}'.format(PORT, fs), 'key': KEY, 'frequent': 1, 'hourly': 0,
                   'daily': 1, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True},
                  {'name': 'ssh:{:d}:{}/sub2'.format(PORT, fs), 'key': KEY, 'frequent': 0,
                   'hourly': 1, 'daily': 0, 'weekly': 1, 'monthly': 0, 'yearly': 1, 'clean': True},
                  {'name': 'ssh:{:d}:{}/sub3'.format(PORT, fs), 'key': KEY, 'frequent': 1,
                   'hourly': 0, 'daily': 1, 'weekly': 0, 'monthly': 1, 'yearly': 0, 'clean': False},
                  {'name': 'ssh:{:d}:{}/sub1/abc'.format(PORT, fs), 'key': KEY, 'frequent': 0,
                   'hourly': 0,'daily': 0, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'clean': True},
                  {'name': 'ssh:{:d}:{}/sub2/efg/hij'.format(PORT, fs), 'key': KEY, 'frequent': 0,
                   'hourly': 0, 'daily': 0, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True}]
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
        ssh = fs1.ssh

        fs0.destroy(force=True)
        fs1.destroy(force=True)

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
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)

        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_full'])
    def test_send_incremental(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh

        fs0.destroy(force=True)
        fs1.destroy(force=True)

        fs0.snapshot('snap0', recursive=True)
        zfs.create('{:s}/sub1'.format(fs0.name))
        fs0.snapshot('snap1', recursive=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)

        zfs.create('{:s}/sub2'.format(fs0.name))
        fs0.snapshot('snap2', recursive=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)

        zfs.create('{:s}/sub3'.format(fs0.name))
        fs0.snapshot('snap3', recursive=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_incremental'])
    def test_send_delete_snapshot(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh

        # Delete recent snapshots on dest
        fs1.snapshots()[-1].destroy(force=True)
        fs1.snapshots()[-1].destroy(force=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)

        # Delete recent snapshot on source
        fs0.snapshot('snap4', recursive=True)
        send_config(config)
        fs0.snapshots()[-1].destroy(force=True)
        fs0.snapshot('snap5', recursive=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_snapshot'])
    def test_send_delete_sub(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh

        # Delete subfilesystems
        sub3 = fs1.filesystems()[-1]
        sub3.destroy(force=True)
        fs0.snapshot('snap6', recursive=True)
        sub2 = fs1.filesystems()[-1]
        sub2.destroy(force=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_sub'])
    def test_send_delete_old(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh

        # Delete old snapshot on source
        fs0.snapshots()[0].destroy(force=True)
        fs0.snapshot('snap7', recursive=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{}'.format(PORT, fs1)], 'dest_keys': [KEY]}]
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert not (set(fs0_children) == set(fs1_children))
        # Assert that snap0 was not deleted from fs1
        for child in set(fs1_children) - set(fs0_children):
            assert child.endswith('snap0')
