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


def zfs_list(filesystem=''):
    """Recusively lists filesystem. Returns false if filesystem does not exist."""
    assert type(filesystem) is str, "Input must be string."

    cmd = ['zfs', 'list', '-r', filesystem,'-o','name']
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()

    if not err:
        out = out.splitlines()[1:]
        out = [name.decode("utf-8") for name in out]
        return out
    else:
        return False


def zfs_snap(filesystem, snapname='', recursive=True):
    """Takes a snapshot of a given filesystem."""
    if not snapname:
        today = datetime.today()
        snapname = 'pyznap_{:s}'.format(today.strftime('%Y-%m-%d_%H:%M:%S'))

    if recursive:
        rec = '-r'
    else:
        rec = ''

    cmd = ['zfs', 'snapshot', rec, '{:s}@{:s}'.format(filesystem, snapname)]
    try:
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    except TypeError:
        print('Cannot convert to string.')
        return

    out, err = proc.communicate()
    if not err:
        return out
    else:
        return err

def zfs_send(filesystem, compress='lzop', out=''):
    """Sends filesystem to file"""
    if not out:
        out = os.path.join(os.getcwd(), 'pyznap.out')
    
    cmd_send = ['zfs', 'send', '-R', filesystem]
    cmd_mbuffer = ['mbuffer', '-s', '128K' , '-m', '1G']
    cmd_compress = [compress,'-c'] if compress else ['cat']

    with open(out,'w') as file:
        send = Popen(cmd_send, stdout=PIPE, stderr=PIPE)
        mbuffer = Popen(cmd_mbuffer, stdin=send.stdout, stdout=PIPE, stderr=PIPE)
        compress = Popen(cmd_compress, stdin=mbuffer.stdout, stdout=file, stderr=PIPE)
    out, err = compress.communicate()
    print(out, err)


