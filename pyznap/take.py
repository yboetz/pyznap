"""
    pyznap.take
    ~~~~~~~~~~~~~~

    Take snapshots.

    :copyright: (c) 2018 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import logging
from datetime import datetime, timedelta
from subprocess import CalledProcessError
from paramiko.ssh_exception import SSHException
from .utils import open_ssh, parse_name
import pyznap.pyzfs as zfs
from .process import DatasetBusyError, DatasetNotFoundError, DatasetExistsError


def take_snap(filesystem, conf):
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
        logger.info('Taking snapshot {}@{:s}...'.format(filesystem, snapname('yearly')))
        try:
            filesystem.snapshot(snapname=snapname('yearly'), recursive=True)
        except (DatasetBusyError, DatasetExistsError) as err:
            logger.error(err)
        except CalledProcessError as err:
            logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                         .format(filesystem, snapname('yearly'), err.stderr.rstrip()))

    if conf['monthly'] and (not snapshots['monthly'] or
                            snapshots['monthly'][0][1].month != now().month or
                            now() - snapshots['monthly'][0][1] > timedelta(days=31)):
        logger.info('Taking snapshot {}@{:s}...'.format(filesystem, snapname('monthly')))
        try:
            filesystem.snapshot(snapname=snapname('monthly'), recursive=True)
        except (DatasetBusyError, DatasetExistsError) as err:
            logger.error(err)
        except CalledProcessError as err:
            logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                         .format(filesystem, snapname('monthly'), err.stderr.rstrip()))

    if conf['weekly'] and (not snapshots['weekly'] or
                           snapshots['weekly'][0][1].isocalendar()[1] != now().isocalendar()[1] or
                           now() - snapshots['weekly'][0][1] > timedelta(days=7)):
        logger.info('Taking snapshot {}@{:s}...'.format(filesystem, snapname('weekly')))
        try:
            filesystem.snapshot(snapname=snapname('weekly'), recursive=True)
        except (DatasetBusyError, DatasetExistsError) as err:
            logger.error(err)
        except CalledProcessError as err:
            logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                         .format(filesystem, snapname('weekly'), err.stderr.rstrip()))

    if conf['daily'] and (not snapshots['daily'] or
                          snapshots['daily'][0][1].day != now().day or
                          now() - snapshots['daily'][0][1] > timedelta(days=1)):
        logger.info('Taking snapshot {}@{:s}...'.format(filesystem, snapname('daily')))
        try:
            filesystem.snapshot(snapname=snapname('daily'), recursive=True)
        except (DatasetBusyError, DatasetExistsError) as err:
            logger.error(err)
        except CalledProcessError as err:
            logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                         .format(filesystem, snapname('daily'), err.stderr.rstrip()))

    if conf['hourly'] and (not snapshots['hourly'] or
                           snapshots['hourly'][0][1].hour != now().hour or
                           now() - snapshots['hourly'][0][1] > timedelta(hours=1)):
        logger.info('Taking snapshot {}@{:s}...'.format(filesystem, snapname('hourly')))
        try:
            filesystem.snapshot(snapname=snapname('hourly'), recursive=True)
        except (DatasetBusyError, DatasetExistsError) as err:
            logger.error(err)
        except CalledProcessError as err:
            logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                         .format(filesystem, snapname('hourly'), err.stderr.rstrip()))

    if conf['frequent'] and (not snapshots['frequent'] or
                             snapshots['frequent'][0][1].minute != now().minute or
                             now() - snapshots['frequent'][0][1] > timedelta(minutes=1)):
        logger.info('Taking snapshot {}@{:s}...'.format(filesystem, snapname('frequent')))
        try:
            filesystem.snapshot(snapname=snapname('frequent'), recursive=True)
        except (DatasetBusyError, DatasetExistsError) as err:
            logger.error(err)
        except CalledProcessError as err:
            logger.error('Error while taking snapshot {}@{:s}: \'{:s}\'...'
                         .format(filesystem, snapname('frequent'), err.stderr.rstrip()))


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
                ssh = open_ssh(user, host, port=port, key=conf['key'])
            except (FileNotFoundError, SSHException):
                continue
            name_log = '{:s}@{:s}:{:s}'.format(user, host, fsname)
        else:
            ssh = None
            name_log = fsname

        try:
            # Children includes the base filesystem (named 'filesystem')
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
            take_snap(children[0], conf)
            # Take snapshot of all children that don't have all snapshots yet
            for child in children[1:]:
                take_snap(child, conf)
        finally:
            if ssh:
                ssh.close()
