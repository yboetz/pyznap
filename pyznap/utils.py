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
# from getpass import getuser

import paramiko as pm

import zfs
from process import DatasetNotFoundError


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given
    snapshot strategy"""

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean', 'dest']

    config = ConfigParser()
    config.read(path)

    res = []
    for section in config.sections():
        dic = {}
        res.append(dic)
        dic['name'] = section

        for option in options:
            try:
                dic[option] = int(config.get(section, option))
            except NoOptionError:
                dic[option] = None
            except ValueError:
                if option in ['snap', 'clean']:
                    dic[option] = True if config.get(section, option) == 'yes' else False
                elif option in ['dest']:
                    dic[option] = [i.strip(' ') for i in config.get(section, option).split(',')]
                else:
                    dic[option] = None

    return res


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

    if exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    for conf in config:
        if not conf.get('dest', None):
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        snapshots = filesystem.snapshots()
        snapshots.reverse()
        snapnames = [snap.name.split('@')[1] for snap in snapshots if
                     snap.name.split('@')[1].startswith('pyznap')]
        try:
            snapshot = snapshots[0]
        except IndexError:
            print('{:s} ERROR: No snapshots on {:s}, aborting...'.format(logtime(), filesystem.name))
            continue

        for dest in conf['dest']:
            try:
                remote_fs = zfs.open(dest)
            except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))
                continue

            remote_snaps = [snap.name.split('@')[1] for snap in remote_fs.snapshots() if
                            snap.name.split('@')[1].startswith('pyznap')]
            # Find common snapshots between local & remote
            common = set(snapnames) & set(remote_snaps)
            # Get the most recent local common snapshot and use it as base
            base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

            if not base:
                print('{:s} INFO: No common snapshots between {:s} and {:s}, sending full stream...'.format(logtime(), filesystem.name, dest))
                with snapshot.send(replicate=True) as send:
                    with Popen(cmd_mbuffer, stdin=send.stdout, stdout=PIPE) as mbuffer:
                        zfs.receive(name=dest, stdin=mbuffer.stdout, force=True, nomount=True)
            elif base.name != snapshot.name:
                print('{:s} INFO: Found common snapshot {:s} on {:s}, sending incremental stream...'.format(logtime(), snapshot.name.split('@')[1], dest))
                with snapshot.send(base=base, intermediates=True, replicate=True) as send:
                    with Popen(cmd_mbuffer, stdin=send.stdout, stdout=PIPE) as mbuffer:
                        zfs.receive(name=dest, stdin=mbuffer.stdout, nomount=True)
            else:
                print('{:s} INFO: {:s} is up to date with {:s}...'.format(logtime(), dest, filesystem.name))


#------------------------------------------------------------------------------------------

def exists(executable=''):
    """Tests if an executable exists on the system."""

    assert isinstance(executable, str), "Input must be string."
    cmd = ['which', executable]
    out, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()

    return bool(out)


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
    ssh.connect(hostname=host, port=port, username=user, key_filename=key)

    assert ssh.get_transport().is_active(), 'Failed to connect to server'
    return ssh, ssh.open_sftp()


def zfs_send_file(snapshot, base=None, intermediates=False, replicate=False,
                  properties=False, deduplicate=False, outfile='/tmp/pyznap.out',
                  compress='lzop'):
    """Sends a snapshot to a file, with compression."""

    if not os.path.isdir(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))

    if compress in ['lzop', 'gzip', 'pigz', 'lbzip2'] and exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    if exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    with open(outfile, 'w') as file:
        with snapshot.send(base=base, intermediates=intermediates, replicate=replicate,
                           properties=properties, deduplicate=deduplicate) as send:
            with Popen(cmd_mbuffer, stdin=send.stdout, stdout=PIPE) as mbuffer:
                _, err = Popen(cmd_compress, stdin=mbuffer.stdout, stdout=file).communicate()

    if not err:
        return True
    else:
        print(err.decode('utf-8'))
        return False


def zfs_send_ssh(snapshot, user, host, key=None, port=22, base=None,
                 intermediates=False, replicate=False, properties=False,
                 deduplicate=False, outfile='/tmp/pyznap.out', compress='lzop'):
    """Sends a snapshot to a file, with compression."""

    ssh, sftp = open_sftp(user=user, host=host, key=key, port=port)

    if compress in ['lzop', 'gzip', 'pigz', 'lbzip2'] and exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    if exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    with sftp.open(outfile, 'w') as file:
        with snapshot.send(base=base, intermediates=intermediates, replicate=replicate,
                           properties=properties, deduplicate=deduplicate) as send:
            with Popen(cmd_mbuffer, stdin=send.stdout, stdout=PIPE) as mbuffer:
                with Popen(cmd_compress, stdin=mbuffer.stdout, stdout=PIPE) as comp:
                    shutil.copyfileobj(comp.stdout, file)
    ssh.close()

    return True
