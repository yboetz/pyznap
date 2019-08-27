"""
    pyznap.send
    ~~~~~~~~~~~~~~

    Send snapshots.

    :copyright: (c) 2018-2019 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""


import sys
import logging
from io import TextIOWrapper
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from fnmatch import fnmatch
from .ssh import SSH, SSHException
from .utils import parse_name, exists, check_recv, bytes_fmt
import pyznap.pyzfs as zfs
from .process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError


def send_snap(snapshot, dest_name, base=None, ssh_dest=None):
    """Sends snapshot to destination, incrementally and over ssh if specified.

    Parameters:
    ----------
    snapshot : {ZFSSnapshot}
        Snapshot to send
    dest_name : {str}
        Name of the location to send snapshot
    base : {ZFSSnapshot}, optional
        Base snapshot for incremental stream (the default is None, meaning a full stream)
    ssh_dest : {ssh.SSH}, optional
        Open ssh connection for remote backup (the default is None, meaning local backup)

    Returns
    -------
    int
        0 if success, 1 if not
    """

    logger = logging.getLogger(__name__)
    dest_name_log = '{:s}@{:s}:{:s}'.format(ssh_dest.user, ssh_dest.host, dest_name) if ssh_dest else dest_name

    try:
        ssh_source = snapshot.ssh
        stream_size = snapshot.stream_size(base=base)

        send = snapshot.send(ssh_dest=ssh_dest, base=base, intermediates=True)
        recv = zfs.receive(name=dest_name, stdin=send.stdout, ssh=ssh_dest, ssh_source=ssh_source, force=True, nomount=True, stream_size=stream_size)

        # write pv output to stderr / stdout
        for line in TextIOWrapper(send.stderr, newline='\r'):
            if sys.stdout.isatty():
                sys.stderr.write('  ' + line)
                sys.stderr.flush()
            elif line.rstrip():     # is stdout is redirected, write pv to stdout
                sys.stdout.write('  ' + line.rstrip() + '\n')
                sys.stdout.flush()

        send.stdout.close()
        recv.communicate()

    except (DatasetNotFoundError, DatasetExistsError, DatasetBusyError, OSError, EOFError) as err:
        logger.error('Error while sending to {:s}: {}...'.format(dest_name_log, err))
        return 1
    except CalledProcessError as err:
        logger.error('Error while sending to {:s}: {}...'.format(dest_name_log, err.stderr.rstrip().decode()))
        return 1
    except KeyboardInterrupt:
        logger.error('KeyboardInterrupt while sending to {:s}...'.format(dest_name_log))
        raise
    else:
        return 0


def send_filesystem(source_fs, dest_name, ssh_dest=None):
    """Checks for common snapshots between source and dest.
    If none are found, send the oldest snapshot, then update with the most recent one.
    If there are common snaps, update destination with the most recent one.

    Parameters:
    ----------
    source_fs : {ZFSFilesystem}
        Source zfs filesystem from where to send
    dest_name : {str}
        Name of the location to send to
    ssh_dest : {ssh.SSH}, optional
        Open ssh connection for remote backup (the default is None, meaning local backup)

    Returns
    -------
    int
        0 if success, 1 if not
    """

    logger = logging.getLogger(__name__)
    dest_name_log = '{:s}@{:s}:{:s}'.format(ssh_dest.user, ssh_dest.host, dest_name) if ssh_dest else dest_name

    logger.debug('Sending {} to {:s}...'.format(source_fs, dest_name_log))

    # Check if dest already has a 'zfs receive' ongoing
    if check_recv(dest_name, ssh=ssh_dest):
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
        dest_fs = zfs.open(dest_name, ssh=ssh_dest)
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
            if send_snap(base, dest_name, base=None, ssh_dest=ssh_dest):
                return 1
    else:
        # If there are common snapshots, get the most recent one
        base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

    if base.name != snapshot.name:
        logger.info('Updating {:s} with recent snapshot {} (~{:s})...'
                    .format(dest_name_log, snapshot, bytes_fmt(snapshot.stream_size(base))))
        if send_snap(snapshot, dest_name, base=base, ssh_dest=ssh_dest):
            return 1

    logger.info('{:s} is up to date...'.format(dest_name_log))
    return 0


def send_config(config):
    """Tries to sync all entries in the config to their dest. Finds all children of the filesystem
    and calls send_filesystem on each of them.

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

        backup_source = conf['name']
        try:
            _type, source_name, user, host, port = parse_name(backup_source)
        except ValueError as err:
            logger.error('Could not parse {:s}: {}...'.format(backup_source, err))
            continue

        # if source is remote, open ssh connection
        if _type == 'ssh':
            key = conf['key'] if conf.get('key', None) else None
            compress = conf['compress'].pop(0) if conf.get('compress', None) else 'lzop'
            try:
                ssh_source = SSH(user, host, port=port, key=key, compress=compress)
            except (FileNotFoundError, SSHException):
                continue
            source_name_log = '{:s}@{:s}:{:s}'.format(user, host, source_name)
        else:
            ssh_source = None
            source_name_log = source_name

        try:
            # Children includes the base filesystem (named 'source_name')
            source_children = zfs.find(path=source_name, types=['filesystem', 'volume'], ssh=ssh_source)
        except DatasetNotFoundError as err:
            logger.error('Source {:s} does not exist...'.format(source_name_log))
            continue
        except ValueError as err:
            logger.error(err)
            continue
        except CalledProcessError as err:
            logger.error('Error while opening source {:s}: \'{:s}\'...'
                         .format(source_name_log, err.stderr.rstrip()))
            continue

        # Send to every backup destination
        for backup_dest in conf['dest']:
            try:
                _type, dest_name, user, host, port = parse_name(backup_dest)
            except ValueError as err:
                logger.error('Could not parse {:s}: {}...'.format(backup_dest, err))
                continue

            # if dest is remote, open ssh connection
            if _type == 'ssh':
                dest_key = conf['dest_keys'].pop(0) if conf.get('dest_keys', None) else None
                # if 'ssh_source' is set, then 'compress' is already set and we use same compression for both source and dest
                # if not then we take the next entry in config
                if not ssh_source:
                    compress = conf['compress'].pop(0) if conf.get('compress', None) else 'lzop'
                try:
                    ssh_dest = SSH(user, host, port=port, key=dest_key, compress=compress)
                except (FileNotFoundError, SSHException):
                    continue
                dest_name_log = '{:s}@{:s}:{:s}'.format(user, host, dest_name)
            else:
                ssh_dest = None
                dest_name_log = dest_name

            # get exclude rules
            exclude = conf['exclude'].pop(0) if conf.get('exclude', None) else []

            # Check if base destination filesystem exists, if not do not send
            try:
                zfs.open(dest_name, ssh=ssh_dest)
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
                for source_fs, dest_name in zip(source_children, dest_children_names):
                    # exclude filesystems from rules
                    if any(fnmatch(source_fs.name, pattern) for pattern in exclude):
                        logger.debug('Matched {} in exclude rules, not sending...'.format(source_fs))
                        continue
                    # send not excluded filesystems
                    send_filesystem(source_fs, dest_name, ssh_dest=ssh_dest)
            finally:
                if ssh_dest:
                    ssh_dest.close()

        if ssh_source:
            ssh_source.close()
