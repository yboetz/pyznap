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
    """Reads a config file and outputs a list of dicts with the given snapshot strategy. If ssh
    keyfiles do not exist it will take standard location in .ssh folder"""

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['key', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean', 'dest',
               'dest_keys']

    parser = ConfigParser()
    parser.read(path)

    config = []
    for section in parser.sections():
        dic = {}
        config.append(dic)
        dic['name'] = section

        for option in options:
            try:
                value = parser.get(section, option)
            except NoOptionError:
                dic[option] = None
            else:
                if option in ['key']:
                    dic[option] = value if os.path.isfile(value) else None
                elif option in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
                    dic[option] = int(value)
                elif option in ['snap', 'clean']:
                    dic[option] = {'yes': True, 'no': False}.get(value.lower(), None)
                elif option in ['dest']:
                    dic[option] = [i.strip() for i in value.split(',')]
                elif option in ['dest_keys']:
                    dic[option] = [i.strip() if os.path.isfile(i.strip()) else None
                                   for i in value.split(',')]
    # Pass through values recursively
    for parent in config:
        for child in config:
            if parent == child:
                continue
            if child['name'].startswith(parent['name']):
                for option in ['key', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'clean']:
                    child[option] = child[option] if child[option] is not None else parent[option]
                child['snap'] = False

    return config


def parse_name(value):
    """Splits a string of the form 'ssh:port:user@host:rpool/data' into its parts"""
    if value.startswith('ssh'):
        _type, port, host, fsname = value.split(':', maxsplit=3)
        port = int(port) if port else 22
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
            # Ignore snapshots not taken with pyznap or sanoid
            if not snap.name.split('@')[1].startswith(('pyznap', 'autosnap')):
                continue
            snap_time = datetime.fromtimestamp(int(snap.getprop('creation')[0]))
            snap_type = snap.name.split('_')[-1]

            try:
                snapshots[snap_type].append((snap, snap_time))
            except KeyError:
                continue

        # Sort by time taken
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


def clean_config(config):
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

        try:
            filesystem = zfs.open(fsname, ssh=ssh)
            # Children excludes the base filesystem (filesystem)
            children = zfs.find(path=fsname, types=['filesystem', 'volume'], ssh=ssh)[1:]
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        # Clean snapshots of parent filesystem
        clean_snap(filesystem, conf)
        # Clean snapshots of all children that don't have a seperate config entry
        for child in children:
            if ssh:
                child_name = 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, child.name)
            else:
                child_name = child.name
            # Skip if entry already in config
            if child_name in [entry['name'] for entry in config]:
                continue
            else:
                clean_snap(child, conf)


def clean_snap(filesystem, conf):
    """Deletes snapshots of a single filesystem according to conf"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

    ssh = filesystem.ssh
    if ssh:
        name_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, filesystem.name)
    else:
        name_log = filesystem.name

    print('{:s} INFO: Cleaning snapshots on {:s}...'.format(logtime(), name_log))

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
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['monthly'][conf['monthly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['weekly'][conf['weekly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['daily'][conf['daily']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['hourly'][conf['hourly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))


def send_recv(snapshot, dest_name, base=None, ssh=None):
    """Sends snapshot to dest_name, incremental if base is given."""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

    try:
        with snapshot.send(base=base, intermediates=True) as send:
            with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
                zfs.receive(name=dest_name, stdin=mbuffer.stdout, ssh=ssh, force=True, nomount=True)
    except (DatasetNotFoundError, DatasetExistsError, DatasetBusyError, CalledProcessError) as err:
        print('{:s} ERROR: {}'.format(logtime(), err))
        return False
    else:
        return True


def send_snap(source_fs, dest_name, ssh=None):
    """Checks for common snapshots between source and dest.
    If none are found, send the oldest snapshot, then update with the most recent one.
    If there are common snaps, update dest with the most recent one."""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

    if ssh:
        dest_name_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, dest_name)
    else:
        dest_name_log = dest_name

    print('{:s} INFO: Sending {:s} to {:s}...'.format(logtime(), source_fs.name, dest_name_log))

    # Get snapshots on source
    snapshots = source_fs.snapshots()[::-1]
    snapnames = [snap.name.split('@')[1] for snap in snapshots]
    try:
        snapshot = snapshots[0]     # Most recent snapshot
        base = snapshots[-1]        # Oldest snapshot
    except IndexError:
        print('{:s} INFO: No snapshots on {:s}, cannot send...'
              .format(logtime(), source_fs.name))
        return False

    try:
        dest_fs = zfs.open(dest_name, ssh=ssh)
    except DatasetNotFoundError:
        dest_snapnames = []
        common = set()
    else:
        dest_snapnames = [snap.name.split('@')[1] for snap in dest_fs.snapshots()]
        # Find common snapshots between source & dest
        common = set(snapnames) & set(dest_snapnames)

    if not common:
        if dest_snapnames:
            print('{:s} ERROR: No common snapshots on {:s}, but snapshots exist. Not sending...'
                  .format(logtime(), dest_name_log), flush=True)
            return False
        else:
            print('{:s} INFO: Sending oldest snapshot {:s} (~{:s})...'
                  .format(logtime(), base.name, zfs.stream_size(base)), flush=True)
            send_recv(base, dest_name, base=None, ssh=ssh)
    else:
        # If there are common snapshots, get the most recent one
        base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

    if base.name != snapshot.name:
        print('{:s} INFO: Updating with recent snapshot {:s} (~{:s})...'
              .format(logtime(), snapshot.name, zfs.stream_size(snapshot, base)), flush=True)
        send_recv(snapshot, dest_name, base=base, ssh=ssh)

    print('{:s} INFO: {:s} is up to date...'.format(logtime(), dest_name_log))
    return True


def send_config(config):
    """Tries to sync all entries in the config to their dest. Finds all children of the filesystem
    and calls send_snap on each of them."""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Sending snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('dest', None):
            continue

        source_fs_name = conf['name']
        if source_fs_name.startswith('ssh'):
            print('{:s} ERROR: Cannot send from remote location...'.format(logtime()))
            continue

        try:
            # source_fs = zfs.open(source_fs_name, ssh=None)
            # children includes the base filesystem (source_fs)
            source_children = zfs.find(path=source_fs_name, types=['filesystem', 'volume'], ssh=None)
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        for backup_dest in conf['dest']:
            try:
                _type, dest_name, user, host, port = parse_name(backup_dest)
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
                if not ssh.test():
                    continue
                dest_name_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, dest_name)
            else:
                ssh = None
                dest_name_log = dest_name

            # Check if base destination filesystem exists
            try:
                zfs.open(dest_name, ssh=ssh)
            except DatasetNotFoundError:
                print('{:s} ERROR: Destination {:s} does not exist...'
                      .format(logtime(), dest_name_log))
                continue
            except (ValueError, CalledProcessError) as err:
                print('{:s} ERROR: {}'.format(logtime(), err))
                continue

            # Match children on source to children on dest
            dest_children_names = [child.name.replace(source_fs_name, dest_name) for
                                   child in source_children]
            # Send all children to corresponding children on dest
            for source, dest in zip(source_children, dest_children_names):
                send_snap(source, dest, ssh=ssh)
