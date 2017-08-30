#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS functions
"""

import os
import shutil

from datetime import datetime
from configparser import ConfigParser, NoOptionError
from subprocess import Popen, PIPE, CalledProcessError

import paramiko as pm

import zfs
from process import DatasetNotFoundError


def exists(executable=''):
    """Tests if an executable exists on the system."""

    assert isinstance(executable, str), "Input must be string."
    cmd = ['which', executable]
    out, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()

    return bool(out)

# Use mbuffer if installed on the system
if exists('mbuffer'):
    MBUFFER = ['mbuffer', '-s', '128K', '-m', '1G']
else:
    MBUFFER = ['cat']


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given
    snapshot strategy"""

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean', 'dest', 'key']

    config = ConfigParser()
    config.read(path)

    res = []
    for section in config.sections():
        dic = {}
        res.append(dic)
        dic['name'] = section

        for option in options:
            try:
                value = config.get(section, option)
            except NoOptionError:
                dic[option] = None
            else:
                if option in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
                    dic[option] = int(value)
                elif option in ['snap', 'clean']:
                    dic[option] = True if value == 'yes' else False
                elif option in ['dest', 'key']:
                    dic[option] = [i.strip(' ') for i in value.split(',')]
    return res


def read_dest(value):
    """Split a dest config entry in its parts"""
    if value.startswith(('ssh', 'sftp')):
        _type, options, host, dest = value.split(':', maxsplit=3)
        if not options:
            port, compress = 22, None
        else:
            options = set(options.split('/')) - set([''])
            compress = set(['lzop', 'gzip', 'pigz', 'lbzip2']) & options
            compress = compress.pop() if compress else None
            port = [o for o in options if o.isdigit()]
            port = int(port[0]) if port else 22
        user, host = host.split('@', maxsplit=1)
    elif value.startswith('file'):
        _type, compress, dest = value.split(':', maxsplit=2)
        user, host, port = None, None, None
    else:
        _type, user, host, port, compress = 'local', None, None, None, None
        dest = value
    return _type, dest, user, host, port, compress


def take_snap(config):
    """Takes snapshots according to strategy given in config"""

    now = datetime.now()
    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Taking snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('snap', None):
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            # Ignore snapshots not taken with pyznap
            if not snap.name.split('@')[1].startswith('pyznap'):
                continue
            snap_time = datetime.fromtimestamp(int(snap.getprop('creation')[0]))
            snap_type = snap.name.split('_')[-1]

            try:
                snapshots[snap_type].append((snap, snap_time))
            except KeyError:
                continue

        for snap_type, snaps in snapshots.items():
            snapshots[snap_type] = sorted(snaps, key=lambda x: x[1], reverse=True)

        snapname = 'pyznap_{:s}_'.format(now.strftime('%Y-%m-%d_%H:%M:%S'))

        if conf['yearly'] and (not snapshots['yearly'] or
                               snapshots['yearly'][0][1].year != now.year):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), conf['name'], snapname + 'yearly'))
            filesystem.snapshot(snapname=snapname + 'yearly', recursive=True)

        if conf['monthly'] and (not snapshots['monthly'] or
                                snapshots['monthly'][0][1].month != now.month):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), conf['name'], snapname + 'monthly'))
            filesystem.snapshot(snapname=snapname + 'monthly', recursive=True)

        if conf['weekly'] and (not snapshots['weekly'] or
                               snapshots['weekly'][0][1].isocalendar()[1] != now.isocalendar()[1]):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), conf['name'], snapname + 'weekly'))
            filesystem.snapshot(snapname=snapname + 'weekly', recursive=True)

        if conf['daily'] and (not snapshots['daily'] or
                              snapshots['daily'][0][1].day != now.day):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), conf['name'], snapname + 'daily'))
            filesystem.snapshot(snapname=snapname + 'daily', recursive=True)

        if conf['hourly'] and (not snapshots['hourly'] or
                               snapshots['hourly'][0][1].hour != now.hour):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), conf['name'], snapname + 'hourly'))
            filesystem.snapshot(snapname=snapname + 'hourly', recursive=True)


def clean_snap(config):
    """Deletes old snapshots according to strategy given in config"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Cleaning snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('clean', None):
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            # Ignore snapshots not taken with pyznap
            if not snap.name.split('@')[1].startswith('pyznap'):
                continue
            snap_time = datetime.fromtimestamp(int(snap.getprop('creation')[0]))
            snap_type = snap.name.split('_')[-1]

            try:
                snapshots[snap_type].append((snap, snap_time))
            except KeyError:
                continue

        for snap_type, snaps in snapshots.items():
            snapshots[snap_type] = sorted(snaps, key=lambda x: x[1], reverse=True)

        for snap, _ in snapshots['yearly'][conf['yearly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['monthly'][conf['monthly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['weekly'][conf['weekly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['daily'][conf['daily']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['hourly'][conf['hourly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            snap.destroy(force=True)


def send_snap(config):
    """Syncs filesystems according to strategy given in config"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Sending snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('dest', None):
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        snapshots = filesystem.snapshots()[::-1]
        snapnames = [snap.name.split('@')[1] for snap in snapshots if
                     snap.name.split('@')[1].startswith('pyznap')]
        try:
            snapshot = snapshots[0]
        except IndexError:
            print('{:s} ERROR: No snapshots on {:s}, aborting...'
                  .format(logtime(), filesystem.name))
            continue

        for backup_dest in conf['dest']:
            try:
                _type, dest, user, host, port, compress = read_dest(backup_dest)
            except ValueError as err:
                print('{:s} ERROR: Could not parse destination {:s}: {}...'
                      .format(logtime(), dest, err))
                continue

            if _type == 'local':
                print('{:s} INFO: Local backup of {:s} on {:s}...'
                      .format(logtime(), filesystem.name, dest))

                try:
                    remote_fs = zfs.open(dest)
                except DatasetNotFoundError:
                    print('{:s} ERROR: Destination {:s} does not exist...'.format(logtime(), dest))
                    continue
                except (ValueError, CalledProcessError) as err:
                    print('{:s} ERROR: {}'.format(logtime(), err))
                    continue

                remote_snaps = [snap.name.split('@')[1] for snap in remote_fs.snapshots() if
                                snap.name.split('@')[1].startswith('pyznap')]
                # Find common snapshots between local & remote, then use most recent as base
                common = set(snapnames) & set(remote_snaps)
                base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

                if not base:
                    print('{:s} INFO: No common snapshots on {:s}, sending full stream...'
                          .format(logtime(), dest), flush=True)
                elif base.name != snapshot.name:
                    print('{:s} INFO: Found common snapshot {:s} on {:s}, sending incremental stream...'
                          .format(logtime(), base.name.split('@')[1], dest), flush=True)
                else:
                    print('{:s} INFO: {:s} is up to date...'.format(logtime(), dest))
                    continue
                zfs_send_local(snapshot, dest, base=base)

            elif _type == 'ssh':
                print('{:s} ERROR: Remote ssh backup of {:s} on {:s}:{:s} is not implemented yet...'
                      .format(logtime(), filesystem.name, host, dest))


#------------------------------------------------------------------------------------------


def open_sftp(user, host, key=None, port=22):
    """Opens an sftp connection to host"""
    ssh = pm.SSHClient()
    if not key:
        key = '/home/{:s}/.ssh/id_rsa'.format(user)
    if not os.path.isfile(key):
        raise FileNotFoundError(key)
    try:
        ssh.load_system_host_keys('/home/{:s}/.ssh/known_hosts'.format(user))
    except FileNotFoundError:
        ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pm.WarningPolicy())
    ssh.connect(hostname=host, port=port, username=user, key_filename=key, timeout=10)

    assert ssh.get_transport().is_active(), 'Failed to connect to server'
    return ssh, ssh.open_sftp()


def zfs_send_local(snapshot, dest, base=None):
    """Sends a snapshot to a local zfs filesystem"""
    with snapshot.send(base=base, intermediates=True, replicate=True) as send:
        with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
            zfs.receive(name=dest, stdin=mbuffer.stdout, force=True, nomount=True)


def zfs_send_file(snapshot, dest, base=None, compress='lzop'):
    """Sends a compressed snapshot to a file"""
    if exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    filename = '{:s}@{:s}'.format(dest, snapshot.name.split('@')[1])

    with open(filename, 'w') as file:
        with snapshot.send(base=base, intermediates=True, replicate=True) as send:
            with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
                Popen(cmd_compress, stdin=mbuffer.stdout, stdout=file).communicate()


def zfs_send_ssh(snapshot, dest, ssh, base=None, compress='lzop'):
    """Sends a snapshot to a sftp file, with compression."""
    if exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    filename = '{:s}@{:s}'.format(dest, snapshot.name.split('@')[1])

    with snapshot.send(base=base, intermediates=True, replicate=True) as send:
        with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
            with Popen(cmd_compress, stdin=mbuffer.stdout, stdout=PIPE) as comp:
                ssh_stdin, _, _ = ssh.exec_command('cat - > {:s}'.format(filename))
                shutil.copyfileobj(comp.stdout, ssh_stdin, 128*1024)
