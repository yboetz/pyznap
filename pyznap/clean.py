"""
    pyznap.clean
    ~~~~~~~~~~~~~~

    Clean snapshots.

    :copyright: (c) 2018-2019 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import logging
from datetime import datetime
from subprocess import CalledProcessError
from .ssh import SSH, SSHException
from .utils import parse_name
import pyznap.pyzfs as zfs
from .process import DatasetBusyError, DatasetNotFoundError


def clean_snap(snap):
    """Deletes a snapshot

    Parameters
    ----------
    snap : {ZFSSnapshot}
        Snapshot to destroy
    """

    logger = logging.getLogger(__name__)
    dry_run = snap.dry_run == True
    dry_msg = '*** DRY RUN ***' if dry_run else ''
    logger.info('Deleting snapshot {}... {}'.format(snap, dry_msg))
    try:
      snap.destroy(dry_run=dry_run)
    except DatasetBusyError as err:
        logger.error(err)
    except CalledProcessError as err:
        logger.error('Error while deleting snapshot {}: \'{:s}\'...'
                     .format(snap, err.stderr.rstrip()))
    except KeyboardInterrupt:
        logger.error('KeyboardInterrupt while cleaning snapshot {}...'
                     .format(snap))
        raise


def clean_filesystem(filesystem, conf):
    """Deletes snapshots of a single filesystem according to conf.

    Parameters:
    ----------
    filesystem : {ZFSFilesystem}
        Filesystem to clean
    conf : {dict}
        Config entry with snapshot strategy
    """

    logger = logging.getLogger(__name__)
    logger.debug('Cleaning snapshots on {}...'.format(filesystem))

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
            snap_type = snap.name.split('_')[-1]
            snap.dry_run = conf.get('dry_run', None)
            snapshots[snap_type].append(snap)
        except (ValueError, KeyError):
            continue

    # Reverse sort by time taken
    for snaps in snapshots.values():
        snaps.reverse()

    for snap in snapshots['yearly'][conf['yearly']:]:
        clean_snap(snap)

    for snap in snapshots['monthly'][conf['monthly']:]:
        clean_snap(snap)

    for snap in snapshots['weekly'][conf['weekly']:]:
        clean_snap(snap)

    for snap in snapshots['daily'][conf['daily']:]:
        clean_snap(snap)

    for snap in snapshots['hourly'][conf['hourly']:]:
        clean_snap(snap)

    for snap in snapshots['frequent'][conf['frequent']:]:
        clean_snap(snap)


def clean_config(config):
    """Deletes old snapshots according to strategies given in config. Goes through each config,
    opens up ssh connection if necessary and then recursively calls clean_filesystem.

    Parameters:
    ----------
    config : {list of dict}
        Full config list containing all strategies for different filesystems
    """

    logger = logging.getLogger(__name__)
    logger.info('Cleaning snapshots...')

    for conf in config:
        if not conf.get('clean', None):
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
        else:
            # Clean snapshots of parent filesystem
            clean_filesystem(children[0], conf)
            # Clean snapshots of all children that don't have a seperate config entry
            for child in children[1:]:
                # Check if any of the parents (but child of base filesystem) have a config entry
                for parent in children[1:]:
                    if ssh:
                        child_name = 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, child.name)
                        parent_name = 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, parent.name)
                    else:
                        child_name = child.name
                        parent_name = parent.name
                    # Skip if child has an entry or if any parent entry already in config
                    child_parent = '/'.join(child_name.split('/')[:-1]) # get parent of child filesystem
                    if ((child_name == parent_name or child_parent.startswith(parent_name)) and
                        (parent_name in [entry['name'] for entry in config])):
                        break
                else:
                    clean_filesystem(child, conf)
        finally:
            if ssh:
                ssh.close()
