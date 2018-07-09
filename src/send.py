"""
Created on Wed Dec 06 2017

@author: yboetz

Send snapshots
"""

import logging
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from paramiko.ssh_exception import SSHException
from utils import open_ssh, parse_name, exists
import pyzfs as zfs
from process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError

logger = logging.getLogger(__name__)

# Use mbuffer if installed on the system
if exists('mbuffer'):
    MBUFFER = ['mbuffer', '-s', '128K', '-m', '1G']
else:
    MBUFFER = ['cat']


def send_recv(snapshot, dest_name, base=None, ssh=None):
    """Sends snapshot to destination, incrementally and over ssh if specified.

    Parameters:
    ----------
    snapshot : {ZFSSnapshot}
        Snapshot to send
    dest_name : {str}
        Name of the location to send snapshot
    base : {ZFSSnapshot}, optional
        Base snapshot for incremental stream (the default is None, meaning a full stream)
    ssh : {paramiko.SSHClient}, optional
        Open ssh connection for remote backup (the default is None, meaning local backup)

    Returns
    -------
    bool
        True if success, False if not
    """

    try:
        with snapshot.send(base=base, intermediates=True) as send:
            with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
                zfs.receive(name=dest_name, stdin=mbuffer.stdout, ssh=ssh, force=True, nomount=True)
    except (DatasetNotFoundError, DatasetExistsError, DatasetBusyError, CalledProcessError) as err:
        logger.error(err)
        return False
    else:
        return True


def send_snap(source_fs, dest_name, ssh=None):
    """Checks for common snapshots between source and dest.
    If none are found, send the oldest snapshot, then update with the most recent one.
    If there are common snaps, update destination with the most recent one.

    Parameters:
    ----------
    source_fs : {ZFSFilesystem}
        Source zfs filesystem from where to send
    dest_name : {str}
        Name of the location to send to
    ssh : {paramiko.SSHClient}, optional
        Open ssh connection for remote backup (the default is None, meaning local backup)

    Returns
    -------
    bool
        True if success, False if not
    """

    if ssh:
        user = ssh.get_transport().get_username()
        host, *_ = ssh.get_transport().getpeername()
        dest_name_log = '{:s}@{:s}:{:s}'.format(user, host, dest_name)
    else:
        dest_name_log = dest_name

    logger.debug('Sending {} to {:s}...'.format(source_fs, dest_name_log))

    # Get snapshots on source
    snapshots = source_fs.snapshots()[::-1]
    snapnames = [snap.name.split('@')[1] for snap in snapshots]
    try:
        snapshot = snapshots[0]     # Most recent snapshot
        base = snapshots[-1]        # Oldest snapshot
    except IndexError:
        logger.error('No snapshots on {}, cannot send...'.format(source_fs))
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
            logger.error('No common snapshots on {:s}, but snapshots exist. Not sending...'
                         .format(dest_name_log))
            return False
        else:
            logger.info('No common snapshots on {:s}, sending oldest snapshot {} (~{:s})...'
                        .format(dest_name_log, base, base.stream_size()))
            send_recv(base, dest_name, base=None, ssh=ssh)
    else:
        # If there are common snapshots, get the most recent one
        base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

    if base.name != snapshot.name:
        logger.info('Updating {:s} with recent snapshot {} (~{:s})...'
                    .format(dest_name_log, snapshot, snapshot.stream_size(base)))
        send_recv(snapshot, dest_name, base=base, ssh=ssh)

    logger.info('{:s} is up to date...'.format(dest_name_log))
    return True


def send_config(config):
    """Tries to sync all entries in the config to their dest. Finds all children of the filesystem
    and calls send_snap on each of them.

    Parameters:
    ----------
    config : {list of dict}
        Full config list containing all strategies for different filesystems
    """

    logger.info('Sending snapshots...')

    for conf in config:
        if not conf.get('dest', None):
            continue

        source_fs_name = conf['name']
        if source_fs_name.startswith('ssh'):
            logger.error('Cannot send from remote location...')
            continue

        try:
            # Children includes the base filesystem (source_fs)
            source_children = zfs.find(path=source_fs_name, types=['filesystem', 'volume'])
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            logger.error(err)
            continue

        for backup_dest in conf['dest']:
            try:
                _type, dest_name, user, host, port = parse_name(backup_dest)
            except ValueError as err:
                logger.error('Could not parse {:s}: {}...'.format(backup_dest, err))
                continue

            if _type == 'ssh':
                dest_key = conf['dest_keys'].pop(0) if conf['dest_keys'] else None
                try:
                    ssh = open_ssh(user, host, port=port, key=dest_key)
                except (FileNotFoundError, SSHException):
                    continue
                dest_name_log = '{:s}@{:s}:{:s}'.format(user, host, dest_name)
            else:
                ssh = None
                dest_name_log = dest_name

            # Check if base destination filesystem exists
            try:
                zfs.open(dest_name, ssh=ssh)
            except DatasetNotFoundError:
                logger.error('Destination {:s} does not exist...'.format(dest_name_log))
                continue
            except (ValueError, CalledProcessError) as err:
                logger.error(err)
                continue

            # Match children on source to children on dest
            dest_children_names = [child.name.replace(source_fs_name, dest_name) for
                                   child in source_children]
            # Send all children to corresponding children on dest
            for source, dest in zip(source_children, dest_children_names):
                send_snap(source, dest, ssh=ssh)

            if ssh:
                ssh.close()
