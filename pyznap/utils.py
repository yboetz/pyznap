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
from subprocess import Popen, PIPE, CalledProcessError
from socket import timeout, gaierror

import paramiko as pm
from paramiko.ssh_exception import (AuthenticationException, BadAuthenticationType,
                                    BadHostKeyException, ChannelException, NoValidConnectionsError,
                                    PasswordRequiredException, SSHException, PartialAuthentication,
                                    ProxyCommandFailure)

import zfs
from process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError


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


class Remote:
    """
    Class to combine all variables necessary for ssh connection
    """
    def __init__(self, user, host, port=22, key=None, proxy=None):
        self.host = host
        self.user = user
        self.port = port

        self.key = key if key else '/home/{:s}/.ssh/id_rsa'.format(self.user)
        if not os.path.isfile(self.key):
            raise FileNotFoundError(self.key)

        self.proxy = proxy
        self.cmd = self.ssh_cmd()

    def ssh_cmd(self):
        """"Returns a command to connect via ssh"""
        hostsfile = '/home/{:s}/.ssh/known_hosts'.format(self.user)
        hostsfile = hostsfile if os.path.isfile(hostsfile) else '/dev/null'
        cmd = ['ssh', '{:s}@{:s}'.format(self.user, self.host),
               '-i', '{:s}'.format(self.key),
               '-o', 'UserKnownHostsFile={:s}'.format(hostsfile)]
        if self.proxy:
            cmd += ['-J', '{:s}'.format(self.proxy)]

        cmd += ['sudo']

        return cmd

    def test(self):
        """Tests if ssh connection can be made"""
        logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
        ssh = pm.SSHClient()
        try:
            ssh.load_system_host_keys('/home/{:s}/.ssh/known_hosts'.format(self.user))
        except FileNotFoundError:
            ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(pm.WarningPolicy())
        try:
            ssh.connect(hostname=self.host, username=self.user, port=self.port,
                        key_filename=self.key, timeout=5)
            ssh.exec_command('ls', timeout=5)
            return True
        except (AuthenticationException, BadAuthenticationType,
                BadHostKeyException, ChannelException, NoValidConnectionsError,
                PasswordRequiredException, SSHException, PartialAuthentication,
                ProxyCommandFailure, timeout, gaierror) as err:
            print('{:s} ERROR: Could not connect to host {:s}: {}...'
                  .format(logtime(), self.host, err))
            return False


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given
    snapshot strategy. If ssh keyfiles do not exist it will take standard
    location in .ssh folder"""

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['key', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean', 'dest', 'dest_keys']

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
                if option in ['key']:
                    dic[option] = value if os.path.isfile(value) else None
                elif option in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
                    dic[option] = int(value)
                elif option in ['snap', 'clean']:
                    dic[option] = True if value == 'yes' else False
                elif option in ['dest']:
                    dic[option] = [i.strip() for i in value.split(',')]
                elif option in ['dest_keys']:
                    dic[option] = [i.strip() if os.path.isfile(i.strip()) else None
                                   for i in value.split(',')]
    return res


def parse_name(value):
    """Split a name/dest config entry in its parts"""
    if value.startswith('ssh'):
        _type, options, host, fsname = value.split(':', maxsplit=3)
        port = int(options) if options else 22
        user, host = host.split('@', maxsplit=1)
    else:
        _type, user, host, port = 'local', None, None, None
        fsname = value
    return _type, fsname, user, host, port


def take_snap(config):
    """Takes snapshots according to strategy given in config"""

    now = datetime.now()
    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Taking snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('snap', None):
            continue

        name = conf['name']
        try:
            _type, fsname, user, host, port = parse_name(name)
        except ValueError as err:
            print('{:s} ERROR: Could not parse {:s}: {}...'
                    .format(logtime(), name, err))
            continue

        if _type == 'ssh':
            name = name.split(':', maxsplit=2)[-1]
            try:
                ssh = Remote(user, host, port, conf['key'])
            except FileNotFoundError as err:
                print('{:s} ERROR: {} is not a valid ssh key file...'.format(logtime(), err))
                continue
            if not ssh.test():
                continue
        else:
            ssh = None

        print('{:s} INFO: Taking snapshots on {:s}...'.format(logtime(), name))


        try:
            filesystem = zfs.open(fsname, ssh=ssh)
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
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), name, snapname + 'yearly'))
            try:
                filesystem.snapshot(snapname=snapname + 'yearly', recursive=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        if conf['monthly'] and (not snapshots['monthly'] or
                                snapshots['monthly'][0][1].month != now.month):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), name, snapname + 'monthly'))
            try:
                filesystem.snapshot(snapname=snapname + 'monthly', recursive=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        if conf['weekly'] and (not snapshots['weekly'] or
                               snapshots['weekly'][0][1].isocalendar()[1] != now.isocalendar()[1]):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), name, snapname + 'weekly'))
            try:
                filesystem.snapshot(snapname=snapname + 'weekly', recursive=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        if conf['daily'] and (not snapshots['daily'] or
                              snapshots['daily'][0][1].day != now.day):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), name, snapname + 'daily'))
            try:
                filesystem.snapshot(snapname=snapname + 'daily', recursive=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        if conf['hourly'] and (not snapshots['hourly'] or
                               snapshots['hourly'][0][1].hour != now.hour):
            print('{:s} INFO: Taking snapshot {:s}@{:s}'.format(logtime(), name, snapname + 'hourly'))
            try:
                filesystem.snapshot(snapname=snapname + 'hourly', recursive=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))


def clean_snap(config):
    """Deletes old snapshots according to strategy given in config"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Cleaning snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('clean', None):
            continue

        name = conf['name']
        try:
            _type, fsname, user, host, port = parse_name(name)
        except ValueError as err:
            print('{:s} ERROR: Could not parse {:s}: {}...'
                    .format(logtime(), name, err))
            continue

        if _type == 'ssh':
            name = name.split(':', maxsplit=2)[-1]
            try:
                ssh = Remote(user, host, port, conf['key'])
            except FileNotFoundError as err:
                print('{:s} ERROR: {} is not a valid ssh key file...'.format(logtime(), err))
                continue
            if not ssh.test():
                continue
        else:
            ssh = None

        print('{:s} INFO: Cleaning snapshots on {:s}...'.format(logtime(), name))


        try:
            filesystem = zfs.open(fsname, ssh=ssh)
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            # Ignore snapshots not taken with pyznap or sanoid
            if not snap.name.split('@')[1].startswith(('pyznap', 'autosnap')):
                continue
            snap_type = snap.name.split('_')[-1]

            try:
                snapshots[snap_type].append(snap)
            except KeyError:
                continue

        for snaps in snapshots.values():
            snaps.reverse()

        for snap in snapshots['yearly'][conf['yearly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            try:
                snap.destroy(force=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        for snap in snapshots['monthly'][conf['monthly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            try:
                snap.destroy(force=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        for snap in snapshots['weekly'][conf['weekly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            try:
                snap.destroy(force=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        for snap in snapshots['daily'][conf['daily']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            try:
                snap.destroy(force=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))

        for snap in snapshots['hourly'][conf['hourly']:]:
            print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
            try:
                snap.destroy(force=True)
            except (DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))


def send_snap(config):
    """Syncs filesystems according to strategy given in config"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Sending snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('dest', None):
            continue

        if conf['name'].startswith('ssh'):
            print('{:s} ERROR: Cannot send from remote location...'.format(logtime()))
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        snapshots = filesystem.snapshots()[::-1]
        snapnames = [snap.name.split('@')[1] for snap in snapshots]
        try:
            snapshot = snapshots[0]
        except IndexError:
            print('{:s} ERROR: No snapshots on {:s}, aborting...'
                  .format(logtime(), filesystem.name))
            continue

        for backup_dest in conf['dest']:
            try:
                _type, fsname, user, host, port = parse_name(backup_dest)
            except ValueError as err:
                print('{:s} ERROR: Could not parse {:s}: {}...'
                      .format(logtime(), backup_dest, err))
                continue

            if _type == 'ssh':
                dest_key = conf['dest_keys'].pop(0) if conf['dest_keys'] else None
                try:
                    ssh = Remote(user, host, port, dest_key)
                except FileNotFoundError as err:
                    print('{:s} ERROR: {} is not a valid ssh key file...'.format(logtime(), err))
                    continue
                dest = '{:s}@{:s}:{:s}'.format(user, host, fsname)
                if not ssh.test():
                    continue
            else:
                ssh = None
                dest = fsname

            print('{:s} INFO: Sending {:s} to {:s}...'
                  .format(logtime(), filesystem.name, dest))

            try:
                dest_fs = zfs.open(fsname, ssh=ssh)
            except DatasetNotFoundError:
                print('{:s} ERROR: Destination {:s} does not exist...'.format(logtime(), dest))
                continue
            except (ValueError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))
                continue

            dest_snaps = [snap.name.split('@')[1] for snap in dest_fs.snapshots()]
            # Find common snapshots between local & dest, then use most recent as base
            common = set(snapnames) & set(dest_snaps)
            base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

            if not base and dest_snaps:
                print('{:s} ERROR: No common snapshots on {:s}, but snapshots exist. Not sending...'
                      .format(logtime(), dest), flush=True)
                continue
            elif not base and not dest_snaps:
                print('{:s} INFO: No common snapshots on {:s}, sending full stream...'
                      .format(logtime(), dest), flush=True)
            elif base.name != snapshot.name:
                print('{:s} INFO: Found common snapshot {:s} on {:s}, sending incremental stream...'
                        .format(logtime(), base.name.split('@')[1], dest), flush=True)
            else:
                print('{:s} INFO: {:s} is up to date...'.format(logtime(), dest))
                continue

            try:
                with snapshot.send(base=base, intermediates=True, replicate=True) as send:
                    with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
                        zfs.receive(name=fsname, stdin=mbuffer.stdout, ssh=ssh, force=True, nomount=True)
            except (DatasetNotFoundError, DatasetExistsError, DatasetBusyError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))
