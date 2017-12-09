#!/usr/bin/env pytest -v

"""
Created on Sat Dec 09 2017

@author: cythoning

ssh tests for pyznap
"""

import subprocess as sp
import sys
import os
import random
import string
from tempfile import NamedTemporaryFile
from datetime import datetime
import pytest
import paramiko as pm

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../src')
import pyzfs as zfs
from utils import Remote, read_config, parse_name
from clean import clean_config
from take import take_config
from send import send_config
from process import DatasetNotFoundError


logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')


def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))


@pytest.fixture(scope='module')
def zpools():
    """Creates two temporary zpools to be called from test functions, source is local and dest on
    remote ssh locatoin. Yields the two pool names and destroys them after testing."""

    zpool = '/sbin/zpool'
    pool0 = 'pyznap_test_source'
    pool1 = 'pyznap_test_dest'

    # ssh connection to dest
    user = 'user'
    host = 'localhost'
    port = 22
    key = None
    sftp_filename = '/tmp/' + randomword(10)

    # ssh arguments for zfs functions
    ssh = Remote(user, host, port, key=key)

    # sftp connection to create/remove file on dest
    sshclient = pm.SSHClient()
    if not key:
        key = '/home/{:s}/.ssh/id_rsa'.format(user)
    if not os.path.isfile(key):
        raise FileNotFoundError(key)
    try:
        sshclient.load_system_host_keys('/home/{:s}/.ssh/known_hosts'.format(user))
    except FileNotFoundError:
        sshclient.load_system_host_keys()
    sshclient.set_missing_host_key_policy(pm.WarningPolicy())
    sshclient.connect(hostname=host, port=port, username=user, key_filename=key, timeout=5)

    assert sshclient.get_transport().is_active(), 'Failed to connect to server'
    sftp = sshclient.open_sftp()


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
            sp.check_call(['sudo', zpool, 'create', pool0, filename0])
        except sp.CalledProcessError as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            return

        try:
            sp.check_call(ssh.cmd + [zpool, 'create', pool1, filename1])
        except sp.CalledProcessError as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            return

        try:
            fs0 = zfs.open(pool0)
            fs1 = zfs.open(pool1, ssh=ssh)
            assert fs0.name == pool0
            assert fs1.name == pool1
        except (DatasetNotFoundError, AssertionError, Exception) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
        else:
            yield fs0, fs1

        # Destroy temporary test pools
        try:
            sp.check_call(['sudo', zpool, 'destroy', pool0])
        except sp.CalledProcessError as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

        try:
            sp.check_call(ssh.cmd + [zpool, 'destroy', pool1])
        except sp.CalledProcessError as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    # Delete tempfile on dest
    sftp.remove(sftp_filename)
    sftp.close()
    sshclient.close()


class TestSnapshot(object):
    @pytest.mark.dependency()
    def test_take_snapshot(self, zpools):
        _, fs = zpools
        ssh = fs.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        config = [{'name': 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 1, 'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)
        take_config(config)

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == 1


    @pytest.mark.dependency(depends=['test_take_snapshot'])
    def test_clean_snapshot(self, zpools):
        _, fs = zpools
        ssh = fs.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        config = [{'name': 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 0, 'daily': 0, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True}]
        clean_config(config)

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]


    @pytest.mark.dependency(depends=['test_clean_snapshot'])
    def test_take_snapshot_recursive(self, zpools):
        _, fs = zpools
        ssh = fs.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        fs.destroy(force=True)
        config = [{'name': 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 1, 'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)
        fs.snapshots()[-1].destroy(force=True)
        fs.snapshots()[-1].destroy(force=True)

        sub1 = zfs.create('{:s}/sub1'.format(fs.name), ssh=ssh)
        take_config(config)

        # Check fs
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]

        # Check sub1
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub1.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]


    @pytest.mark.dependency(depends=['test_take_snapshot_recursive'])
    def test_clean_recursive(self, zpools):
        _, fs = zpools
        ssh = fs.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        fs.destroy(force=True)
        sub1 = zfs.create('{:s}/sub1'.format(fs.name), ssh=ssh)
        abc = zfs.create('{:s}/sub1/abc'.format(fs.name), ssh=ssh)
        sub2 = zfs.create('{:s}/sub2'.format(fs.name), ssh=ssh)
        efg = zfs.create('{:s}/sub2/efg'.format(fs.name), ssh=ssh)
        hij = zfs.create('{:s}/sub2/efg/hij'.format(fs.name), ssh=ssh)
        sub3 = zfs.create('{:s}/sub3'.format(fs.name), ssh=ssh)

        config = [{'name': 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 1, 'daily': 1, 'weekly': 1, 'monthly': 1, 'yearly': 1, 'snap': True}]
        take_config(config)

        config = [{'name': 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 0, 'daily': 1, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True},
                  {'name': 'ssh:{:d}:{:s}@{:s}:{:s}/sub2'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 1, 'daily': 0, 'weekly': 1, 'monthly': 0, 'yearly': 1, 'clean': True},
                  {'name': 'ssh:{:d}:{:s}@{:s}:{:s}/sub3'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 0, 'daily': 1, 'weekly': 0, 'monthly': 1, 'yearly': 0, 'clean': False},
                  {'name': 'ssh:{:d}:{:s}@{:s}:{:s}/sub2/efg/hij'.format(port, user, host, fs.name), 'key': key,
                   'hourly': 0, 'daily': 0, 'weekly': 0, 'monthly': 0, 'yearly': 0, 'clean': True}]
        clean_config(config)

        # Check parent filesystem
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in fs.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]
        # Check sub1
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub1.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]
        # Check sub1/abc
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in abc.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[0][snap_type]
        # Check sub2
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in sub2.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[1][snap_type]
        # Check sub2/efg
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in efg.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[1][snap_type]
        # Check sub2/efg/hij
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in hij.snapshots():
            snap_type = snap.name.split('_')[-1]
            snapshots[snap_type].append(snap)

        for snap_type, snaps in snapshots.items():
            assert len(snaps) == config[3][snap_type]
        # Check sub3
        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
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
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        fs0.destroy(force=True)
        fs1.destroy(force=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs1.name)],
                   'dest_keys': [key]}]

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
        zfs.create('{:s}/sub3/efg'.format(fs0.name))
        fs0.snapshot('snap7', recursive=True)
        fs0.snapshot('snap8', recursive=True)
        send_config(config)

        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_full'])
    def test_send_incremental(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        fs0.destroy(force=True)
        fs1.destroy(force=True)
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs1.name)],
                   'dest_keys': [key]}]

        fs0.snapshot('snap0', recursive=True)
        zfs.create('{:s}/sub1'.format(fs0.name))
        fs0.snapshot('snap1', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)

        zfs.create('{:s}/sub2'.format(fs0.name))
        fs0.snapshot('snap2', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)

        zfs.create('{:s}/sub3'.format(fs0.name))
        fs0.snapshot('snap3', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_incremental'])
    def test_send_delete_snapshot(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs1.name)],
                   'dest_keys': [key]}]

        # Delete recent snapshots on dest
        fs1.snapshots()[-1].destroy(force=True)
        fs1.snapshots()[-1].destroy(force=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)

        # Delete recent snapshot on source
        fs0.snapshot('snap4', recursive=True)
        send_config(config)
        fs0.snapshots()[-1].destroy(force=True)
        fs0.snapshot('snap5', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_snapshot'])
    def test_send_delete_sub(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key

        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs1.name)],
                   'dest_keys': [key]}]

        # Delete subfilesystems
        sub3 = fs1.filesystems()[-1]
        sub3.destroy(force=True)
        fs0.snapshot('snap6', recursive=True)
        sub2 = fs1.filesystems()[-1]
        sub2.destroy(force=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert set(fs0_children) == set(fs1_children)


    @pytest.mark.dependency(depends=['test_send_delete_sub'])
    def test_send_delete_old(self, zpools):
        fs0, fs1 = zpools
        ssh = fs1.ssh
        user, host, port, key = ssh.user, ssh.host, ssh.port, ssh.key
        
        config = [{'name': fs0.name, 'dest': ['ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, fs1.name)],
                   'dest_keys': [key]}]

        # Delete old snapshot on source
        fs0.snapshots()[0].destroy(force=True)
        fs0.snapshot('snap7', recursive=True)
        send_config(config)
        fs0_children = [child.name.replace(fs0.name, '') for child in zfs.find(fs0.name, types=['all'])[1:]]
        fs1_children = [child.name.replace(fs1.name, '') for child in zfs.find(fs1.name, types=['all'], ssh=ssh)[1:]]
        assert not (set(fs0_children) == set(fs1_children))
        # Assert that snap0 was not deleted from fs1
        for child in set(fs1_children) - set(fs0_children):
            assert child.endswith('snap0')
