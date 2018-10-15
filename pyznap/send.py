"""
    pyznap.send
    ~~~~~~~~~~~~~~

    Send snapshots.

    :copyright: (c) 2018 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import logging
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from paramiko.ssh_exception import SSHException
from .utils import open_ssh, parse_name, exists, check_recv, bytes_fmt
import pyznap.pyzfs as zfs
from .process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError


# Use mbuffer if installed on the system
if exists('mbuffer'):
    MBUFFER = ['mbuffer', '-q', '-s', '128K', '-m', '512M']
else:
    MBUFFER = ['cat']

# Use pv if installed on the system
if exists('pv'):
    PV = lambda size: ['pv', '-w', '100', '-s', str(size)]
else:
    PV = lambda _: ['cat']


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
    int
        0 if success, 1 if not
    """

    logger = logging.getLogger(__name__)
    dest_name_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, dest_name) if ssh else dest_name

    stream_size = snapshot.stream_size(base=base)

    try:
        with snapshot.send(base=base, intermediates=True) as send:
            with Popen(MBUFFER, stdin=send.stdout, stdout=PIPE) as mbuffer:
                with Popen(PV(stream_size), stdin=mbuffer.stdout, stdout=PIPE) as pv:
                    zfs.receive(name=dest_name, stdin=pv.stdout, ssh=ssh, force=True, nomount=True)
    except (DatasetNotFoundError, DatasetExistsError, DatasetBusyError, OSError, EOFError) as err:
        logger.error('Error while sending to {:s}: {}...'.format(dest_name_log, err))
        return 1
    except CalledProcessError as err:
        logger.error('Error while sending to {:s}: {}...'.format(dest_name_log, err.stderr.rstrip()))
        return 1
    else:
        return 0


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
    int
        0 if success, 1 if not
    """

    logger = logging.getLogger(__name__)
    dest_name_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, dest_name) if ssh else dest_name

    logger.debug('Sending {} to {:s}...'.format(source_fs, dest_name_log))

    # Check if ssh session still active
    if ssh and not ssh.get_transport().is_active():
        logger.error('Error while sending to {:s}: ssh session not active...'.format(dest_name_log))
        return 1

    # Check if dest already has a 'zfs receive' ongoing
    if check_recv(dest_name, ssh=ssh):
        return 1

    # Get snapshots on source
    snapshots = source_fs.snapshots()[::-1]
    snapnames = [snap.name.split('@')[1] for snap in snapshots]
    try:
        snapshot = snapshots[0]     # Most recent snapshot
        base = snapshots[-1]        # Oldest snapshot
    except IndexError:
        logger.error('No snapshots on {}, cannot send...'.format(source_fs))
        return 1

    try:
        dest_fs = zfs.open(dest_name, ssh=ssh)
    except DatasetNotFoundError:
        dest_snapnames = []
        common = set()
    except CalledProcessError as err:
        logger.error('Error while opening dest {:s}: \'{:s}\'...'
                     .format(dest_name_log, err.stderr.rstrip()))
        return 1
    else:
        dest_snapnames = [snap.name.split('@')[1] for snap in dest_fs.snapshots()]
        # Find common snapshots between source & dest
        common = set(snapnames) & set(dest_snapnames)

    if not common:
        if dest_snapnames:
            logger.error('No common snapshots on {:s}, but snapshots exist. Not sending...'
                         .format(dest_name_log))
            return 1
        else:
            logger.info('No common snapshots on {:s}, sending oldest snapshot {} (~{:s})...'
                        .format(dest_name_log, base, bytes_fmt(base.stream_size())))
            if send_recv(base, dest_name, base=None, ssh=ssh):
                return 1
    else:
        # If there are common snapshots, get the most recent one
        base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

    if base.name != snapshot.name:
        logger.info('Updating {:s} with recent snapshot {} (~{:s})...'
                    .format(dest_name_log, snapshot, bytes_fmt(snapshot.stream_size(base))))
        if send_recv(snapshot, dest_name, base=base, ssh=ssh):
            return 1

    logger.info('{:s} is up to date...'.format(dest_name_log))
    return 0


def send_config(config):
    """Tries to sync all entries in the config to their dest. Finds all children of the filesystem
    and calls send_snap on each of them.

    Parameters:
    ----------
    config : {list of dict}
        Full config list containing all strategies for different filesystems
    """

    logger = logging.getLogger(__name__)
    logger.info('Sending snapshots...')

    for conf in config:
        if not conf.get('dest', None):
            continue

        source_name = conf['name']
        if source_name.startswith('ssh'):
            logger.error('Cannot send from remote location ({:s})...'.format(source_name))
            continue

        try:
            # Children includes the base filesystem (named 'source_fs')
            source_children = zfs.find(path=source_name, types=['filesystem', 'volume'])
        except DatasetNotFoundError as err:
            logger.error('Source {:s} does not exist...'.format(source_name))
            continue
        except ValueError as err:
            logger.error(err)
            continue
        except CalledProcessError as err:
            logger.error('Error while opening source {:s}: \'{:s}\'...'
                         .format(source_name, err.stderr.rstrip()))
            continue

        # Send to every backup destination
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

            # Check if base destination filesystem exists, if not do not send
            try:
                zfs.open(dest_name, ssh=ssh)
            except DatasetNotFoundError:
                logger.error('Destination {:s} does not exist...'.format(dest_name_log))
                continue
            except ValueError as err:
                logger.error(err)
                continue
            except CalledProcessError as err:
                logger.error('Error while opening dest {:s}: \'{:s}\'...'
                             .format(dest_name_log, err.stderr.rstrip()))
                continue
            else:
                # Match children on source to children on dest
                dest_children_names = [child.name.replace(source_name, dest_name) for
                                       child in source_children]
                # Send all children to corresponding children on dest
                for source, dest in zip(source_children, dest_children_names):
                    send_snap(source, dest, ssh=ssh)
            finally:
                if ssh:
                    ssh.close()
