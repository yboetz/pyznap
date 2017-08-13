#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS bindings.
"""

from subprocess import Popen, PIPE
from datetime import datetime
import os


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


def zfs_send(filesystem, compress='lzop', out=''):
    """Sends filesystem to file"""
    if not out:
        out = os.path.join(os.getcwd(), 'pyznap.out')

    cmd_send = ['zfs', 'send', '-R', filesystem]
    cmd_mbuffer = ['mbuffer', '-s', '128K', '-m', '1G']
    cmd_compress = [compress, '-c'] if compress else ['cat']

    with open(out, 'w') as file:
        send = Popen(cmd_send, stdout=PIPE, stderr=PIPE)
        mbuffer = Popen(cmd_mbuffer, stdin=send.stdout, stdout=PIPE, stderr=PIPE)
        compress = Popen(cmd_compress, stdin=mbuffer.stdout, stdout=file, stderr=PIPE)
    out, err = compress.communicate()
    print(out, err)
