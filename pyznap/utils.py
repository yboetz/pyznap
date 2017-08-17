#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS functions
"""

import os

from datetime import datetime
from configparser import ConfigParser, NoOptionError
import zfs
from process import DatasetNotFoundError

from subprocess import Popen, PIPE, CalledProcessError
from getpass import getuser


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given
    snapshot strategy"""

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean']

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
                else:
                    dic[option] = None

    return res


def take_snap(config):
    """Takes snapshots according to strategy given in config"""

    now = datetime.now()
    logtime = now.strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Taking snapshots...'.format(logtime))

    for conf in config:
        if not conf['snap']:
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime, err))
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            # Ignore snapshots not taken with pyznap
            if not snap.name.split('@')[1].startswith('pyznap'):
                continue
            snap_time = datetime.fromtimestamp(int(snap.getprop('creation')['value']))
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
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime, conf['name'], snapname + 'yearly'))
            filesystem.snapshot(snapname=snapname + 'yearly', recursive=True)

        if conf['monthly'] and (not snapshots['monthly'] or
                                snapshots['monthly'][0][1].month != now.month):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime, conf['name'], snapname + 'monthly'))
            filesystem.snapshot(snapname=snapname + 'monthly', recursive=True)

        if conf['weekly'] and (not snapshots['weekly'] or
                               snapshots['weekly'][0][1].isocalendar()[1] != now.isocalendar()[1]):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime, conf['name'], snapname + 'weekly'))
            filesystem.snapshot(snapname=snapname + 'weekly', recursive=True)

        if conf['daily'] and (not snapshots['daily'] or
                              snapshots['daily'][0][1].day != now.day):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime, conf['name'], snapname + 'daily'))
            filesystem.snapshot(snapname=snapname + 'daily', recursive=True)

        if conf['hourly'] and (not snapshots['hourly'] or
                               snapshots['hourly'][0][1].hour != now.hour):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime, conf['name'], snapname + 'hourly'))
            filesystem.snapshot(snapname=snapname + 'hourly', recursive=True)


def clean_snap(config):
    """Deletes old snapshots according to strategy given in config"""

    now = datetime.now()
    logtime = now.strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Cleaning snapshots...'.format(logtime))

    for conf in config:
        if not conf['clean']:
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime, err))
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            # Ignore snapshots not taken with pyznap
            if not snap.name.split('@')[1].startswith('pyznap'):
                continue
            snap_time = datetime.fromtimestamp(int(snap.getprop('creation')['value']))
            snap_type = snap.name.split('_')[-1]

            try:
                snapshots[snap_type].append((snap, snap_time))
            except KeyError:
                continue

        for snap_type, snaps in snapshots.items():
            snapshots[snap_type] = sorted(snaps, key=lambda x: x[1], reverse=True)

        for snap, _ in snapshots['yearly'][conf['yearly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime, snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['monthly'][conf['monthly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime, snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['weekly'][conf['weekly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime, snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['daily'][conf['daily']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime, snap.name))
            snap.destroy(force=True)

        for snap, _ in snapshots['hourly'][conf['hourly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime, snap.name))
            snap.destroy(force=True)


#------------------------------------------------------------------------------------------

def exists(executable=''):
    """Tests if an executable exists on the system."""

    assert isinstance(executable, str), "Input must be string."
    cmd = ['which', executable]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, _ = proc.communicate()

    return bool(out)


def zfs_list(filesystem='', recursive=True):
    """Recusively lists filesystem. Returns false if filesystem does not exist."""

    assert isinstance(filesystem, str), "Input must be string."

    cmd = ['zfs', 'list', '-o', 'name']

    if recursive:
        cmd.append('-r')
    if filesystem:
        cmd.append(filesystem)

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    if not err:
        out = out.splitlines()[1:]
        out = [name.decode("utf-8") for name in out]
        return out
    else:
        return False


def zfs_list_snap(filesystem=''):
    """Recusively lists snapshots. Returns false filesystem does not exist or there
    are no snapshots."""

    assert isinstance(filesystem, str), "Input must be string."

    cmd = ['zfs', 'list', '-o', 'name', '-t', 'snap', '-r']

    if filesystem:
        cmd.append(filesystem)

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    if not err:
        out = out.splitlines()[1:]
        out = [name.decode("utf-8") for name in out]
        return out
    else:
        return False


def zfs_snap(filesystem, snapname='', recursive=True):
    """Takes a snapshot of a given filesystem. Returns false if filesystem does not exist."""

    assert isinstance(filesystem, str), "Input must be string."
    assert isinstance(snapname, str), "Input must be string."

    if not zfs_list(filesystem, recursive):
        return False

    if not snapname:
        today = datetime.today()
        snapname = 'pyznap_{:s}'.format(today.strftime('%Y-%m-%d_%H:%M:%S'))

    cmd = ['zfs', 'snapshot', '{:s}@{:s}'.format(filesystem, snapname)]

    if recursive:
        cmd.append('-r')

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()

    if not err:
        return zfs_list_snap(filesystem)
    else:
        return err


def zfs_destroy(snapname, recursive=True):
    """Destroys a filesystem or snapshot of a given filesystem.
    Returns false if filesystem does not exist."""

    assert isinstance(snapname, str), "Input must be string."

    if not zfs_list(snapname, recursive):
        return False

    cmd = ['zfs', 'destroy', snapname]
    if recursive:
        cmd.append('-r')

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()

    if not err:
        return True
    else:
        return err


def zfs_send_file(snapname, outfile='/tmp/pyznap.out', compress='lzop', mbuffer=True):
    """Sends a snapshot to a file, with compression."""

    if not zfs_list_snap(snapname):
        raise ValueError('Snapshot does not exist.')

    if not os.path.isdir(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))

    if compress in ['lzop', 'gzip', 'pigz', 'lbzip2'] and exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    if mbuffer and exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    cmd_send = ['zfs', 'send', '-R', snapname]

    with open(outfile, 'w') as file:
        proc_send = Popen(cmd_send, stdout=PIPE)
        proc_mbuffer = Popen(cmd_mbuffer, stdin=proc_send.stdout, stdout=PIPE)
        proc_compress = Popen(cmd_compress, stdin=proc_mbuffer.stdout, stdout=file)

        proc_send.stdout.close()
        proc_mbuffer.stdout.close()
        _, err = proc_compress.communicate()

    if not err:
        return True
    else:
        print(err.decode('utf-8'))
        return False


def zfs_send_ssh(snapname, outfile='/tmp/pyznap.out', compress='lzop', mbuffer=True,
                 username=getuser(), hostname='localhost',
                 keyfile=os.path.join(os.path.expanduser('~'), '.ssh/id_rsa')):
    """Sends a snapshot to a file, with compression."""

    if hostname == 'localhost':
        return zfs_send_file(snapname, outfile, compress, mbuffer)

    # Test ssh connection
    cmd_ssh = ['ssh', '-i', keyfile, '{:s}@{:s}'.format(username, hostname)]
    proc = Popen(cmd_ssh + ['exit'], stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()
    if err:
        print('Cannot make ssh connection.')
        print(err.decode('utf-8'))
        return False

    if not zfs_list_snap(snapname):
        raise ValueError('Snapshot does not exist.')

    if compress in ['lzop', 'gzip', 'pigz', 'lbzip2'] and exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    if mbuffer and exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    cmd_send = ['zfs', 'send', '-R', snapname]
    cmd_ssh += ['cat - > {:s}'.format(outfile)]

    # Send snapshot over ssh
    proc_send = Popen(cmd_send, stdout=PIPE)
    proc_mbuffer = Popen(cmd_mbuffer, stdin=proc_send.stdout, stdout=PIPE)
    proc_compress = Popen(cmd_compress, stdin=proc_mbuffer.stdout, stdout=PIPE)
    proc_ssh = Popen(cmd_ssh, stdin=proc_compress.stdout, stdout=PIPE)

    proc_send.stdout.close()
    proc_mbuffer.stdout.close()
    proc_compress.stdout.close()
    _, err = proc_ssh.communicate()

    if not err:
        return True
    else:
        print(err.decode('utf-8'))
        return False
