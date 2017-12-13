"""
Created on Wed Dec 06 2017

@author: yboetz

Send snapshots
"""

from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from utils import Remote, parse_name, exists
import pyzfs as zfs
from process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError


# Use mbuffer if installed on the system
if exists('mbuffer'):
    MBUFFER = ['mbuffer', '-s', '128K', '-m', '1G']
else:
    MBUFFER = ['cat']


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
            # Children includes the base filesystem (source_fs)
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
