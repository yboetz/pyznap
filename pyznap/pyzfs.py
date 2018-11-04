"""
    pyznap.pyzfs
    ~~~~~~~~~~~~~~

    Python ZFS bindings, forked from https://bitbucket.org/stevedrake/weir/.

    :copyright: (c) 2015-2018 by Stephen Drake, Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""


import subprocess as sp
from .process import check_output, DatasetNotFoundError, DatasetBusyError


def find(path=None, ssh=None, max_depth=None, types=[]):
    """Lists filesystems and snapshots for a given path"""
    cmd = ['zfs', 'list']

    cmd.append('-H')

    if max_depth is None:
        cmd.append('-r')
    elif max_depth >= 0:
        cmd.append('-d')
        cmd.append(str(max_depth))
    else:
        raise TypeError('max_depth must be a non-negative int or None')

    if types:
        cmd.append('-t')
        cmd.append(','.join(types))

    cmd.append('-o')
    cmd.append('name,type')

    if path:
        cmd.append(path)

    out = check_output(cmd, ssh=ssh)

    return [open(name, ssh=ssh, type=type) for name, type in out]


def findprops(path=None, ssh=None, max_depth=None, props=['all'], sources=[], types=[]):
    """Lists all properties of a given filesystem"""
    cmd = ['zfs', 'get']

    cmd.append('-H')
    cmd.append('-p')

    if max_depth is None:
        cmd.append('-r')
    elif max_depth >= 0:
        cmd.append('-d')
        cmd.append(str(max_depth))
    else:
        raise TypeError('max_depth must be a non-negative int or None')

    if types:
        cmd.append('-t')
        cmd.append(','.join(types))

    if sources:
        cmd.append('-s')
        cmd.append(','.join(sources))

    cmd.append(','.join(props))

    if path:
        cmd.append(path)

    out = check_output(cmd, ssh=ssh)

    names = set(map(lambda x: x[0], out))

    # return [dict(name=n, property=p, value=v, source=s) for n, p, v, s in out]
    return {name: {i[1]: (i[2], i[3]) for i in out if i[0] == name} for name in names}


# Factory function for dataset objects
def open(name, ssh=None, type=None):
    """Opens a volume, filesystem or snapshot"""
    if type is None:
        type = findprops(name, ssh=ssh, max_depth=0, props=['type'])[name]['type'][0]

    if type == 'volume':
        return ZFSVolume(name, ssh)

    if type == 'filesystem':
        return ZFSFilesystem(name, ssh)

    if type == 'snapshot':
        return ZFSSnapshot(name, ssh)

    raise ValueError('invalid dataset type %s' % type)


def roots(ssh=None):
    return find(ssh=ssh, max_depth=0)

# note: force means create missing parent filesystems
def create(name, ssh=None, type='filesystem', props={}, force=False):
    cmd = ['zfs', 'create']

    if type == 'volume':
        raise NotImplementedError()
    elif type != 'filesystem':
        raise ValueError('invalid type %s' % type)

    if force:
        cmd.append('-p')

    for prop, value in props.items():
        cmd.append('-o')
        cmd.append(prop + '=' + str(value))

    cmd.append(name)

    check_output(cmd, ssh=ssh)
    return ZFSFilesystem(name, ssh=ssh)


def receive(name, stdin, ssh=None, append_name=False, append_path=False,
            force=False, nomount=False):
    cmd = ['zfs', 'receive']

    # cmd.append('-v')

    if append_name:
        cmd.append('-e')
    elif append_path:
        cmd.append('-d')

    if force:
        cmd.append('-F')
    if nomount:
        cmd.append('-u')

    cmd.append(name)

    return check_output(cmd, stdin=stdin, ssh=ssh)


class ZFSDataset(object):
    def __init__(self, name, ssh=None):
        self.name = name
        self.ssh = ssh

    def __str__(self):
        return '{:s}@{:s}:{:s}'.format(self.ssh.user, self.ssh.host, self.name) if self.ssh else self.name

    def __repr__(self):
        name = self.__str__()
        return '{0}({1!r})'.format(self.__class__.__name__, name)

    def parent(self):
        parent_name, _, _ = self.name.rpartition('/')
        return open(parent_name, ssh=self.ssh) if parent_name else None

    def filesystems(self):
        return find(self.name, ssh=self.ssh, max_depth=1, types=['filesystem'])[1:]

    def snapshots(self):
        return find(self.name, ssh=self.ssh, max_depth=1, types=['snapshot'])

    def children(self):
        return find(self.name, ssh=self.ssh, max_depth=1, types=['all'])[1:]

    def clones(self):
        raise NotImplementedError()

    def dependents(self):
        raise NotImplementedError()

    # TODO: split force to allow -f, -r and -R to be specified individually
    # TODO: remove or ignore defer option for non-snapshot datasets
    def destroy(self, defer=False, force=False):
        cmd = ['zfs', 'destroy']

        cmd.append('-v')

        if defer:
            cmd.append('-d')

        if force:
            cmd.append('-f')
            cmd.append('-R')

        cmd.append(self.name)

        check_output(cmd, ssh=self.ssh)

    def snapshot(self, snapname, recursive=False, props={}):
        cmd = ['zfs', 'snapshot']

        if recursive:
            cmd.append('-r')

        for prop, value in props.items():
            cmd.append('-o')
            cmd.append(prop + '=' + str(value))

        name = self.name + '@' + snapname
        cmd.append(name)

        check_output(cmd, ssh=self.ssh)
        return ZFSSnapshot(name, ssh=self.ssh)

    # TODO: split force to allow -f, -r and -R to be specified individually
    def rollback(self, snapname, force=False):
        raise NotImplementedError()

    def promote(self):
        raise NotImplementedError()

    # TODO: split force to allow -f and -p to be specified individually
    def rename(self, name, recursive=False, force=False):
        raise NotImplementedError()

    def getprops(self):
        return findprops(self.name, ssh=self.ssh, max_depth=0)[self.name]

    def getprop(self, prop):
        return findprops(self.name, ssh=self.ssh, max_depth=0, props=[prop])[self.name].get(prop, None)

    def getpropval(self, prop, default=None):
        value = self.getprop(prop)['value']
        return default if value == '-' else value

    def setprop(self, prop, value):
        cmd = ['zfs', 'set']

        cmd.append(prop + '=' + str(value))
        cmd.append(self.name)

        check_output(cmd, ssh=self.ssh)

    def delprop(self, prop, recursive=False):
        cmd = ['zfs', 'inherit']

        if recursive:
            cmd.append('-r')

        cmd.append(prop)
        cmd.append(self.name)

        check_output(cmd, ssh=self.ssh)

    def userspace(self, *args, **kwargs):
        raise NotImplementedError()

    def groupspace(self, *args, **kwargs):
        raise NotImplementedError()

    def share(self, *args, **kwargs):
        raise NotImplementedError()

    def unshare(self, *args, **kwargs):
        raise NotImplementedError()

    def allow(self, *args, **kwargs):
        raise NotImplementedError()

    def unallow(self, *args, **kwargs):
        raise NotImplementedError()

class ZFSVolume(ZFSDataset):
    pass

class ZFSFilesystem(ZFSDataset):
    def upgrade(self, *args, **kwargs):
        raise NotImplementedError()

    def mount(self, *args, **kwargs):
        raise NotImplementedError()

    def unmount(self, *args, **kwargs):
        raise NotImplementedError()

class ZFSSnapshot(ZFSDataset):
    def snapname(self):
        snapname = self.name.split('@')[-1]
        return snapname

    def parent(self):
        parent_path = self.name.split('@')[0]
        return open(name=parent_path, ssh=self.ssh)

    # note: force means create missing parent filesystems
    def clone(self, name, props={}, force=False):
        raise NotImplementedError()

    def send(self, base=None, intermediates=False, replicate=False,
             properties=False, deduplicate=False):
        if self.ssh:
            raise NotImplementedError()

        cmd = ['zfs', 'send']

        # cmd.append('-v')
        # cmd.append('-P')

        if replicate:
            cmd.append('-R')
        if properties:
            cmd.append('-p')
        if deduplicate:
            cmd.append('-D')

        if base is not None:
            if intermediates:
                cmd.append('-I')
            else:
                cmd.append('-i')
            cmd.append(base.name)

        cmd.append(self.name)

        return sp.Popen(cmd, stdout=sp.PIPE)

    def stream_size(self, base=None):
        if self.ssh:
            raise NotImplementedError()

        cmd = ['zfs', 'send', '-nP']

        if base is not None:
            cmd.append('-I')
            cmd.append(base.name)

        cmd.append(self.name)

        try:
            out = check_output(cmd)
        except (DatasetNotFoundError, DatasetBusyError,
                sp.CalledProcessError):
            return 0

        try:
            out = out[-1][-1]
        except IndexError:
            return 0

        return int(out.split(' ')[-1])

    def hold(self, tag, recursive=False):
        cmd = ['zfs', 'hold']

        if recursive:
            cmd.append('-r')

        cmd.append(tag)
        cmd.append(self.name)

        check_output(cmd, ssh=self.ssh)

    def holds(self):
        cmd = ['zfs', 'holds']

        cmd.append('-H')

        cmd.append(self.name)

        out = check_output(cmd, ssh=self.ssh)

        # return hold tag names only
        return [hold[1] for hold in out]

    def release(self, tag, recursive=False):
        cmd = ['zfs', 'release']

        if recursive:
            cmd.append('-r')

        cmd.append(tag)
        cmd.append(self.name)

        check_output(cmd, ssh=self.ssh)
