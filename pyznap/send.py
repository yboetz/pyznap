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
from .ssh import SSH, SSHException, SSHConnectError
from .utils import parse_name, exists, check_recv, bytes_fmt
import pyznap.pyzfs as zfs
from .process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError
import time


def send_snap(snapshot, dest_name, base=None, ssh_dest=None, raw=False, resume=False, receive_resume_token=None):
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
        0 if success, 1 if not, 2 if resume from last transfer
    """

    logger = logging.getLogger(__name__)
    dest_name_log = '{:s}@{:s}:{:s}'.format(ssh_dest.user, ssh_dest.host, dest_name) if ssh_dest else dest_name

    try:
        ssh_source = snapshot.ssh
        stream_size = snapshot.stream_size(base=base, receive_resume_token=receive_resume_token)

        send = snapshot.send(ssh_dest=ssh_dest, base=base, intermediates=True, raw=raw, receive_resume_token=receive_resume_token)
        recv = zfs.receive(name=dest_name, stdin=send.stdout, ssh=ssh_dest, ssh_source=ssh_source,
                           force=True, nomount=True, stream_size=stream_size, raw=raw, resume=resume)
        send.stdout.close()

        # write pv output to stderr / stdout
        for line in TextIOWrapper(send.stderr, newline='\r'):
            if sys.stdout.isatty():
                sys.stderr.write('  ' + line)
                sys.stderr.flush()
            elif line.rstrip():     # is stdout is redirected, write pv to stdout
                sys.stdout.write('  ' + line.rstrip() + '\n')
                sys.stdout.flush()
        send.stderr.close()

        stdout, stderr = recv.communicate()
        if stderr.decode().startswith('cannot receive incremental stream'):
            raise SSHConnectError(stderr)
        # raise any error that occured
        if recv.returncode:
            raise CalledProcessError(returncode=recv.returncode, cmd=recv.args, output=stdout, stderr=stderr)

    except SSHConnectError as err:
        raise
    except (DatasetNotFoundError, DatasetExistsError, DatasetBusyError, OSError, EOFError) as err:
        logger.error('Error while sending to {:s}: {}...'.format(dest_name_log, err))
        return 1
    except CalledProcessError as err:
        logger.error('Error while sending to {:s}: {}...'.format(dest_name_log, err.stderr.rstrip().decode().replace('\n', ' - ')))
        return 1
    except KeyboardInterrupt:
        logger.error('KeyboardInterrupt while sending to {:s}...'.format(dest_name_log))
        raise
    else:
        return 0


def send_filesystem(source_fs, dest_name, ssh_dest=None, raw=False, resume=False):
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

    # get snapshots on source, catch exception if dataset was destroyed since pyznap was started
    try:
        snapshots = source_fs.snapshots()[::-1]
    except (DatasetNotFoundError, DatasetBusyError) as err:
        logger.error('Error while opening source {}: {}...'.format(source_fs, err))
        return 1
    snapnames = [snap.name.split('@')[1] for snap in snapshots]

    try:
        snapshot = snapshots[0]     # Most recent snapshot
        base = snapshots[-1]        # Oldest snapshot
    except IndexError:
        logger.error('No snapshots on {}, cannot send...'.format(source_fs))
        return 1

    receive_resume_token = None
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

        if resume:
            receive_resume_token = dest_fs.getprops().get('receive_resume_token', (None, None))[0]

    if not common:
        if dest_snapnames:
            logger.error('No common snapshots on {:s}, but snapshots exist. Not sending...'
                         .format(dest_name_log))
            return 1
        else:
            if receive_resume_token is not None:
                logger.info('Resume last transfer of {:s} (~{:s})...'
                            .format(dest_name_log,
                                bytes_fmt(snapshot.stream_size(base, receive_resume_token=receive_resume_token))))
                if send_snap(base, dest_name, base=None, ssh_dest=ssh_dest, raw=raw, resume=resume, receive_resume_token=receive_resume_token) == 1:
                    return 1
                receive_resume_token = None
            else:
                logger.info('No common snapshots on {:s}, sending oldest snapshot {} (~{:s})...'
                            .format(dest_name_log, base, bytes_fmt(base.stream_size())))
                if send_snap(base, dest_name, base=None, ssh_dest=ssh_dest, raw=raw, resume=resume):
                    return 1
    else:
        # If there are common snapshots, get the most recent one
        base = next(filter(lambda x: x.name.split('@')[1] in common, snapshots), None)

    if base.name != snapshot.name or receive_resume_token is not None:
        if receive_resume_token is not None:
            # zfs send with receive_resume_token will only resume transfer between town snapshots
            # so we need update it again
            logger.info('Resume last transfer of {:s} (~{:s}), then update again...'
                        .format(dest_name_log,
                            bytes_fmt(snapshot.stream_size(base, receive_resume_token=receive_resume_token))))
        else:
            logger.info('Updating {:s} with recent snapshot {} (~{:s})...'
                        .format(dest_name_log, snapshot,
                            bytes_fmt(snapshot.stream_size(base))))
        if send_snap(snapshot, dest_name, base=base, ssh_dest=ssh_dest, raw=raw, resume=resume, receive_resume_token=receive_resume_token):
            return 1

    if receive_resume_token is not None:
        return 2
    else:
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

        retry = conf.get('retry', 0)
        retry_interval = conf.get('retry_interval', 10)
        if retry:
            ssh_extra_options = {
                'ServerAliveInterval': conf.get('ServerAliveInterval', 60),
                'ServerAliveCountMax': conf.get('ServerAliveCountMax', 3),
            }
        else:
            ssh_extra_options = {}

        # if source is remote, open ssh connection
        if _type == 'ssh':
            key = conf['key'] if conf.get('key', None) else None
            compress = conf['compress'].pop(0) if conf.get('compress', None) else 'lzop'
            try:
                ssh_source = SSH(user, host, port=port, key=key, compress=compress, **ssh_extra_options)
            except (FileNotFoundError, SSHException):
                continue
            source_name_log = '{:s}@{:s}:{:s}'.format(user, host, source_name)
        else:
            ssh_source = None
            source_name_log = source_name

        resume = conf.get('resume', False)

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
                    ssh_dest = SSH(user, host, port=port, key=dest_key, compress=compress, **ssh_extra_options)
                except (FileNotFoundError, SSHException):
                    continue
                dest_name_log = '{:s}@{:s}:{:s}'.format(user, host, dest_name)
            else:
                ssh_dest = None
                dest_name_log = dest_name

            # get exclude rules
            exclude = conf['exclude'].pop(0) if conf.get('exclude', None) else []

            # check if raw send was requested
            raw = conf['raw_send'].pop(0) if conf.get('raw_send', None) else False

            # if dest_auto_create is set, create dest if it not exists, else log error message
            if conf.get('dest_auto_create', False):
                try:
                    zfs.open(dest_name, ssh=ssh_dest)
                except DatasetNotFoundError:
                    logger.info('Destination {:s} does not exist...create'.format(dest_name_log))
                    # only create its parent, the receive process will create itself automatically
                    #   zfs create -p a/b/c can create all the non-existing parent datasets,
                    #   but it will also auto-mount them, here we create them manually
                    to_create = []
                    sub_paths = dest_name.split('/')
                    if len(sub_paths)>1: # no need to 'recreate' the dataset
                        for depth in range(len(sub_paths), 1, -1): # get all non-exists parents
                            _path = '/'.join(sub_paths[:depth]) # the first path is the parent of dest_name
                            try:
                                zfs.open(_path, ssh=ssh_dest)
                            except DatasetNotFoundError:
                                to_create.append(_path)
                            else:
                                break
                        for each in to_create[::-1]:
                            try:
                                zfs.create(each, ssh=ssh_dest)
                                logger.debug('Create {:s} at {:s}'.format(each, dest_name_log))
                            except CalledProcessError as err:
                                errmsg = err.stderr.rstrip()
                                # filter this common error message, it's not a error
                                if 'filesystem successfully created, but' not in errmsg:
                                    logger.error('Error while create {} at {:s}: \'{:s}\'...'
                                                 .format(each, dest_name_log, errmsg))
                            except Exception as err:
                                logger.error('Unknown Error while create {} at {:s}: \'{:s}\'...'
                                             .format(each, dest_name_log, str(err)))
                source_name_length = len(source_name)
                # Match children on source to children on dest
                dest_children_names = [dest_name+child.name[source_name_length:] for
                                       child in source_children]
                # Send all children to corresponding children on dest
                for source_fs, dest_name in zip(source_children, dest_children_names):
                    # exclude filesystems from rules
                    if any(fnmatch(source_fs.name, pattern) for pattern in exclude):
                        logger.debug('Matched {} in exclude rules, not sending...'.format(source_fs))
                        continue
                    # send not excluded filesystems
                    retries = 0
                    while True: # retry when lose SSH connection
                        try:
                            status = send_filesystem(source_fs, dest_name, ssh_dest=ssh_dest, raw=raw, resume=resume)
                            if status==2: # just resume from last transfer, do upate again
                                send_filesystem(source_fs, dest_name, ssh_dest=ssh_dest, raw=raw, resume=resume)
                            break
                        except SSHConnectError as err:
                            if not retry:
                                logger.error("SSH connection error: {:s}, no retries allowed, exit".format(err))
                                raise
                            else:
                                retries += 1
                                if retries > retry:
                                    logger.error("Reach max retry counts, exit".format(err))
                                    raise
                                else:
                                    logger.warn("SSH connection lost, sleep {} and retry {}/{}".format(retry_interval, retries, retry))
                                    time.sleep(retry_interval)
                if ssh_dest:
                    ssh_dest.close()
            else: # report error when dest not exists
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
                        send_filesystem(source_fs, dest_name, ssh_dest=ssh_dest, raw=raw, resume=resume)
                finally:
                    if ssh_dest:
                        ssh_dest.close()

        if ssh_source:
            ssh_source.close()
