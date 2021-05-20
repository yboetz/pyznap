"""
    pyznap.take
    ~~~~~~~~~~~~~~

    Take snapshots.

    :copyright: (c) 2018-2019 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import logging
from datetime import datetime, timedelta
from subprocess import CalledProcessError
from .ssh import SSH, SSHException
from .utils import parse_name
import pyznap.pyzfs as zfs
from .process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError


def take_snap(filesystem, _type):
    """Takes a snapshot of type '_type'

    Parameters
    ----------
    filesystem : {ZFSFilesystem}
        Filesystem to take snapshot of
    _type : {str}
        Type of snapshot to take
    """

    logger = logging.getLogger(__name__)
    now = datetime.now

    snapname = lambda _type: 'pyznap_{:s}_{:s}'.format(now().strftime('%Y-%m-%d_%H:%M:%S'), _type)

    dry_run = filesystem.dry_run == True
    dry_msg = '*** DRY RUN ***' if dry_run else ''
    logger.info('Taking snapshot {}@{:s}... {}'.format(filesystem, snapname(_type), dry_msg))
    try:
        if not dry_run:
          filesystem.snapshot(snapname=snapname(_type), recursive=True)
    except (DatasetBusyError, DatasetExistsError) as err:
        logger.error(err)
    except CalledProcessError as err:
        logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                     .format(filesystem, snapname(_type), err.stderr.rstrip()))
    except KeyboardInterrupt:
        logger.error('KeyboardInterrupt while taking snapshot {}@{:s}...'
                     .format(filesystem, snapname(_type)))
        raise


def take_filesystem(filesystem, conf):
    """Takes snapshots of a single filesystem according to conf.

    Parameters:
    ----------
    filesystem : {ZFSFilesystem}
        Filesystem to take snapshot of
    conf : {dict}
        Config entry with snapshot strategy
    """

    logger = logging.getLogger(__name__)
    logger.debug('Taking snapshots on {}...'.format(filesystem))
    now = datetime.now

    filesystem.dry_run = conf.get('dry_run', None)
    snapshots = {'frequent': [], 'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
    # catch exception if dataset was destroyed since pyznap was started
    try:
        fs_snapshots = filesystem.snapshots()
    except (DatasetNotFoundError, DatasetBusyError) as err:
        logger.error('Error while opening {}: {}...'.format(filesystem, err))
        return 1
    # categorize snapshots
    for snap in fs_snapshots:
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

    if conf['yearly'] and (not snapshots['yearly'] or
                           snapshots['yearly'][0][1].year != now().year):
        take_snap(filesystem, 'yearly')

    if conf['monthly'] and (not snapshots['monthly'] or
                            snapshots['monthly'][0][1].month != now().month or
                            now() - snapshots['monthly'][0][1] > timedelta(days=31)):
        take_snap(filesystem, 'monthly')

    if conf['weekly'] and (not snapshots['weekly'] or
                           snapshots['weekly'][0][1].isocalendar()[1] != now().isocalendar()[1] or
                           now() - snapshots['weekly'][0][1] > timedelta(days=7)):
        take_snap(filesystem, 'weekly')

    if conf['daily'] and (not snapshots['daily'] or
                          snapshots['daily'][0][1].day != now().day or
                          now() - snapshots['daily'][0][1] > timedelta(days=1)):
        take_snap(filesystem, 'daily')

    if conf['hourly'] and (not snapshots['hourly'] or
                           snapshots['hourly'][0][1].hour != now().hour or
                           now() - snapshots['hourly'][0][1] > timedelta(hours=1)):
        take_snap(filesystem, 'hourly')

    if conf['frequent'] and (not snapshots['frequent'] or
                             snapshots['frequent'][0][1].minute != now().minute or
                             now() - snapshots['frequent'][0][1] > timedelta(minutes=1)):
        take_snap(filesystem, 'frequent')


def take_config(config):
    """Takes snapshots according to strategy given in config.

    Parameters:
    ----------
    config : {list of dict}
        Full config list containing all strategies for different filesystems
    """

    logger = logging.getLogger(__name__)
    logger.info('Taking snapshots...')

    for conf in config:
        if not conf.get('snap', None):
            continue

        name = conf['name']
        try:
            _type, fsname, user, host, port = parse_name(name)
        except ValueError as err:
            logger.error('Could not parse {:s}: {}...'.format(name, err))
            continue

        if _type == 'ssh':
            try:
                ssh = SSH(user, host, port=port, key=conf['key'])
            except (FileNotFoundError, SSHException):
                continue
            name_log = '{:s}@{:s}:{:s}'.format(user, host, fsname)
        else:
            ssh = None
            name_log = fsname

        try:
            # Children includes the base filesystem (named 'fsname')
            children = zfs.find(path=fsname, types=['filesystem', 'volume'], ssh=ssh)
        except DatasetNotFoundError as err:
            logger.error('Dataset {:s} does not exist...'.format(name_log))
            continue
        except ValueError as err:
            logger.error(err)
            continue
        except CalledProcessError as err:
            logger.error('Error while opening {:s}: \'{:s}\'...'
                         .format(name_log, err.stderr.rstrip()))
            continue
        else:
            # Take recursive snapshot of parent filesystem
            take_filesystem(children[0], conf)
            # Take snapshot of all children that don't have all snapshots yet
            for child in children[1:]:
                take_filesystem(child, conf)
        finally:
            if ssh:
                ssh.close()
