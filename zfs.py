#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS bindings.
"""

from subprocess import Popen, PIPE
from datetime import datetime
from getpass import getuser
from weir import zfs
import os


def exists(executable=''):
    """Tests if an executable exists on the system."""

    assert isinstance(executable, str), "Input must be string."
    cmd = ['which', executable]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, _ = proc.communicate()

    return bool(out)


def zfs_list(filesystem='', recursive=True):
    """Recusively lists filesystem. Returns false if filesystem does not exist."""

    assert isinstance(filesystem, str), "Input must be string."

    cmd = ['zfs', 'list', '-o', 'name']

    if recursive:
        cmd.append('-r')
    if filesystem:
        cmd.append(filesystem)

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    if not err:
        out = out.splitlines()[1:]
        out = [name.decode("utf-8") for name in out]
        return out
    else:
        return False


def zfs_list_snap(filesystem=''):
    """Recusively lists snapshots. Returns false filesystem does not exist or there
    are no snapshots."""

    assert isinstance(filesystem, str), "Input must be string."

    cmd = ['zfs', 'list', '-o', 'name', '-t', 'snap', '-r']

    if filesystem:
        cmd.append(filesystem)

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    if not err:
        out = out.splitlines()[1:]
        out = [name.decode("utf-8") for name in out]
        return out
    else:
        return False


def zfs_snap(filesystem, snapname='', recursive=True):
    """Takes a snapshot of a given filesystem. Returns false if filesystem does not exist."""

    assert isinstance(filesystem, str), "Input must be string."
    assert isinstance(snapname, str), "Input must be string."

    if not zfs_list(filesystem, recursive):
        return False

    if not snapname:
        today = datetime.today()
        snapname = 'pyznap_{:s}'.format(today.strftime('%Y-%m-%d_%H:%M:%S'))

    cmd = ['zfs', 'snapshot', '{:s}@{:s}'.format(filesystem, snapname)]

    if recursive:
        cmd.append('-r')

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()

    if not err:
        return zfs_list_snap(filesystem)
    else:
        return err


def zfs_destroy(snapname, recursive=True):
    """Destroys a filesystem or snapshot of a given filesystem.
    Returns false if filesystem does not exist."""

    assert isinstance(snapname, str), "Input must be string."

    if not zfs_list(snapname, recursive):
        return False

    cmd = ['zfs', 'destroy', snapname]
    if recursive:
        cmd.append('-r')

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()

    if not err:
        return True
    else:
        return err


def zfs_send_file(snapname, outfile='/tmp/pyznap.out', compress='lzop', mbuffer=True):
    """Sends a snapshot to a file, with compression."""

    if not zfs_list_snap(snapname):
        raise ValueError('Snapshot does not exist.')

    if not os.path.isdir(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))

    if compress in ['lzop', 'gzip', 'pigz', 'lbzip2'] and exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    if mbuffer and exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    cmd_send = ['zfs', 'send', '-R', snapname]

    with open(outfile, 'w') as file:
        proc_send = Popen(cmd_send, stdout=PIPE)
        proc_mbuffer = Popen(cmd_mbuffer, stdin=proc_send.stdout, stdout=PIPE)
        proc_compress = Popen(cmd_compress, stdin=proc_mbuffer.stdout, stdout=file)

        proc_send.stdout.close()
        proc_mbuffer.stdout.close()
        _, err = proc_compress.communicate()

    if not err:
        return True
    else:
        print(err.decode('utf-8'))
        return False


def zfs_send_ssh(snapname, outfile='/tmp/pyznap.out', compress='lzop', mbuffer=True,
                 username=getuser(), hostname='localhost',
                 keyfile=os.path.join(os.path.expanduser('~'), '.ssh/id_rsa')):
    """Sends a snapshot to a file, with compression."""

    if hostname == 'localhost':
        return zfs_send_file(snapname, outfile, compress, mbuffer)

    # Test ssh connection
    cmd_ssh = ['ssh', '-i', keyfile, '{:s}@{:s}'.format(username, hostname)]
    proc = Popen(cmd_ssh + ['exit'], stdout=PIPE, stderr=PIPE)
    _, err = proc.communicate()
    if err:
        print('Cannot make ssh connection.')
        print(err.decode('utf-8'))
        return False

    if not zfs_list_snap(snapname):
        raise ValueError('Snapshot does not exist.')

    if compress in ['lzop', 'gzip', 'pigz', 'lbzip2'] and exists(compress):
        cmd_compress = [compress, '-c']
    else:
        cmd_compress = ['cat']

    if mbuffer and exists('mbuffer'):
        cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    else:
        cmd_mbuffer = ['cat']

    cmd_send = ['zfs', 'send', '-R', snapname]
    cmd_ssh += ['cat - > {:s}'.format(outfile)]

    # Send snapshot over ssh
    proc_send = Popen(cmd_send, stdout=PIPE)
    proc_mbuffer = Popen(cmd_mbuffer, stdin=proc_send.stdout, stdout=PIPE)
    proc_compress = Popen(cmd_compress, stdin=proc_mbuffer.stdout, stdout=PIPE)
    proc_ssh = Popen(cmd_ssh, stdin=proc_compress.stdout, stdout=PIPE)

    proc_send.stdout.close()
    proc_mbuffer.stdout.close()
    proc_compress.stdout.close()
    _, err = proc_ssh.communicate()

    if not err:
        return True
    else:
        print(err.decode('utf-8'))
        return False

def zfs_prune(filesystem, hourly=24, daily=30, monthly=6, yearly=1):
    """Deletes old snapshots. Keeps all snapshots in the past 24h,
    then 30 in the past month, 6 in the past half year and 1 in the
    last year."""

    assert isinstance(filesystem, str), "Input must be string."
    snaps = zfs_list_snap(filesystem)
    snapdate = []
    now = datetime.today()

    for snap in snaps:
        try:
            date = snap.split('@')[1]
            date = datetime.strptime(date, 'pyznap_%Y-%m-%d_%H:%M:%S')
        except ValueError:
            continue
        snapdate.append((snap, date))



    return snapdate