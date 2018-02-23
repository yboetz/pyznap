"""
Created on Wed Dec 06 2017

@author: yboetz

Take snapshots
"""

from datetime import datetime, timedelta
from subprocess import CalledProcessError
from paramiko.ssh_exception import SSHException
from utils import open_ssh, parse_name
import pyzfs as zfs
from process import DatasetBusyError, DatasetNotFoundError


def take_snap(filesystem, conf):
    """Takes snapshots of a single filesystem according to conf.

    Parameters:
    ----------
    filesystem : {ZFSFilesystem}
        Filesystem to take snapshot of
    conf : {dict}
        Config entry with snapshot strategy
    """

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    now = datetime.now

    # print('{:s} INFO: Taking snapshots on {:s}...'.format(logtime(), name_log))

    snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
    for snap in filesystem.snapshots():
        # Ignore snapshots not taken with pyznap or sanoid
        if not snap.name.split('@')[1].startswith(('pyznap', 'autosnap')):
            continue
        try:
            _date, _time, snap_type = snap.name.split('_')[-3:]
            snap_time =  datetime.strptime('{:s}_{:s}'.format(_date, _time), '%Y-%m-%d_%H:%M:%S')
            snapshots[snap_type].append((snap, snap_time))
        except (ValueError, KeyError):
            continue

    # Reverse sort by time taken
    for snaps in snapshots.values():
        snaps.reverse()

    snapname = lambda _type: 'pyznap_{:s}_{:s}'.format(now().strftime('%Y-%m-%d_%H:%M:%S'), _type)

    if conf['yearly'] and (not snapshots['yearly'] or
                           snapshots['yearly'][0][1].year != now().year):
        print('{:s} INFO: Taking snapshot {}@{:s}...'.format(logtime(), filesystem, snapname('yearly')))
        try:
            filesystem.snapshot(snapname=snapname('yearly'), recursive=True)
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    if conf['monthly'] and (not snapshots['monthly'] or
                            snapshots['monthly'][0][1].month != now().month or
                            now() - snapshots['monthly'][0][1] > timedelta(days=31)):
        print('{:s} INFO: Taking snapshot {}@{:s}...'.format(logtime(), filesystem, snapname('monthly')))
        try:
            filesystem.snapshot(snapname=snapname('monthly'), recursive=True)
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    if conf['weekly'] and (not snapshots['weekly'] or
                           snapshots['weekly'][0][1].isocalendar()[1] != now().isocalendar()[1] or
                           now() - snapshots['weekly'][0][1] > timedelta(days=7)):
        print('{:s} INFO: Taking snapshot {}@{:s}...'.format(logtime(), filesystem, snapname('weekly')))
        try:
            filesystem.snapshot(snapname=snapname('weekly'), recursive=True)
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    if conf['daily'] and (not snapshots['daily'] or
                          snapshots['daily'][0][1].day != now().day or
                          now() - snapshots['daily'][0][1] > timedelta(days=1)):
        print('{:s} INFO: Taking snapshot {}@{:s}...'.format(logtime(), filesystem, snapname('daily')))
        try:
            filesystem.snapshot(snapname=snapname('daily'), recursive=True)
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    if conf['hourly'] and (not snapshots['hourly'] or
                           snapshots['hourly'][0][1].hour != now().hour or
                           now() - snapshots['hourly'][0][1] > timedelta(hours=1)):
        print('{:s} INFO: Taking snapshot {}@{:s}...'.format(logtime(), filesystem, snapname('hourly')))
        try:
            filesystem.snapshot(snapname=snapname('hourly'), recursive=True)
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    if conf['frequent'] and (not snapshots['frequent'] or
                             snapshots['frequent'][0][1].minute//15 != now().minute//15 or
                             now() - snapshots['frequent'][0][1] > timedelta(minutes=15)):
        print('{:s} INFO: Taking snapshot {}@{:s}...'.format(logtime(), filesystem, snapname('frequent')))
        try:
            filesystem.snapshot(snapname=snapname('frequent'), recursive=True)
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))


def take_config(config):
    """Takes snapshots according to strategy given in config.

    Parameters:
    ----------
    config : {list of dict}
        Full config list containing all strategies for different filesytems
    """

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
            try:
                ssh = open_ssh(user, host, port=port, key=conf['key'])
            except (FileNotFoundError, SSHException):
                continue
        else:
            ssh = None

        try:
            # Children includes the base filesystem (filesystem)
            children = zfs.find(path=fsname, types=['filesystem', 'volume'], ssh=ssh)
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        # Take recursive snapshot of parent filesystem
        take_snap(children[0], conf)
        # Take snapshot of all children that don't have all snapshots yet
        for child in children[1:]:
            # Check if any of the parents (but child of base filesystem) have a config entry
            for parent in children[1:]:
                if ssh:
                    parent_name = 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, parent.name)
                else:
                    parent_name = parent.name
                # Skip if any parent entry already in config
                if (child.name.startswith(parent.name) and
                        parent_name in [entry['name'] for entry in config]):
                    break
            else:
                take_snap(child, conf)

        if ssh:
            ssh.close()
