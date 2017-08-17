"""
ZFS bindings, forked from weir
"""

import subprocess as sp


def find(path=None, max_depth=None, types=[]):
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

    out = sp.check_output(cmd, universal_newlines=True)
    out = [line.split('\t') for line in out.splitlines()]

    return [open(name, type) for name, type in out]


def findprops(path=None, max_depth=None, props=['all'], sources=[], types=[]):
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

    out = sp.check_output(cmd, universal_newlines=True)
    out = [line.split('\t') for line in out.splitlines()]

    names = set(map(lambda x: x[0], out))

    # return [dict(name=n, property=p, value=v, source=s) for n, p, v, s in out]
    return {name: {i[1]: (i[2], i[3]) for i in out if i[0] == name} for name in names}


# Factory function for dataset objects
def open(name, type=None):
    """Opens a volume, filesystem or snapshot"""
    if type is None:
        type = findprops(name, max_depth=0, props=['type'])[name]['type'][0]

    if type == 'volume':
        return ZFSVolume(name)

    if type == 'filesystem':
        return ZFSFilesystem(name)

    if type == 'snapshot':
        return ZFSSnapshot(name)

    raise ValueError('invalid dataset type %s' % type)


def roots():
    return find(max_depth=0)

# note: force means create missing parent filesystems
def create(name, type='filesystem', props={}, force=False):
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

    sp.check_output(cmd)
    return ZFSFilesystem(name)


def receive(name, append_name=False, append_path=False,
        force=False, nomount=False):
    raise NotImplementedError()

    # url = _urlsplit(name)

    # cmd = ['zfs', 'receive']

    # cmd.append('-v')

    # if append_name:
    #     cmd.append('-e')
    # elif append_path:
    #     cmd.append('-d')

    # if force:
    #     cmd.append('-F')
    # if nomount:
    #     cmd.append('-u')

    # cmd.append(url.path)

    # return process.popen(cmd, mode='wb', netloc=url.netloc)

class ZFSDataset(object):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return '{0}({1!r})'.format(self.__class__.__name__, self.name)

    def parent(self):
        parent_name, _, _ = self.name.rpartition('/')
        return open(parent_name) if parent_name else None

    def filesystems(self):
        return find(self.name, max_depth=1, types=['filesystem'])[1:]

    def snapshots(self):
        return find(self.name, max_depth=1, types=['snapshot'])

    def children(self):
        return find(self.name, max_depth=1, types=['all'])[1:]

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

        sp.check_output(cmd)

    def snapshot(self, snapname, recursive=False, props={}):
        cmd = ['zfs', 'snapshot']

        if recursive:
            cmd.append('-r')

        for prop, value in props.items():
            cmd.append('-o')
            cmd.append(prop + '=' + str(value))

        name = self.name + '@' + snapname
        cmd.append(name)

        sp.check_output(cmd)
        return ZFSSnapshot(name)

    # TODO: split force to allow -f, -r and -R to be specified individually
    def rollback(self, snapname, force=False):
        raise NotImplementedError()

    def promote(self):
        raise NotImplementedError()

    # TODO: split force to allow -f and -p to be specified individually
    def rename(self, name, recursive=False, force=False):
        raise NotImplementedError()

    def getprops(self):
        return findprops(self.name, max_depth=0)[self.name]

    def getprop(self, prop):
        return findprops(self.name, max_depth=0, props=[prop])[self.name].get(prop, None)

    def getpropval(self, prop, default=None):
        value = self.getprop(prop)['value']
        return default if value == '-' else value

    def setprop(self, prop, value):
        cmd = ['zfs', 'set']

        cmd.append(prop + '=' + str(value))
        cmd.append(self.name)

        sp.check_output(cmd)

    def delprop(self, prop, recursive=False):
        cmd = ['zfs', 'inherit']

        if recursive:
            cmd.append('-r')

        cmd.append(prop)
        cmd.append(self.name)

        sp.check_output(cmd)

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
        return open(name=parent_path)

    # note: force means create missing parent filesystems
    def clone(self, name, props={}, force=False):
        raise NotImplementedError()

    def send(self, base=None, intermediates=False, replicate=False,
            properties=False, deduplicate=False):
        raise NotImplementedError()

        # cmd = ['zfs', 'send']

        # cmd.append('-v')
        # cmd.append('-P')

        # if replicate:
        #     cmd.append('-R')
        # if properties:
        #     cmd.append('-p')
        # if deduplicate:
        #     cmd.append('-D')

        # if base is not None:
        #     base = _urlsplit(base)
        #     if base.netloc and base.netloc != self._url.netloc:
        #         raise ValueError('snapshots must be on same host')
        #     if intermediates:
        #         cmd.append('-I')
        #     else:
        #         cmd.append('-i')
        #     cmd.append(base.path)

        # cmd.append(self._url.path)

        # return process.popen(cmd, mode='rb', netloc=self._url.netloc)

    def hold(self, tag, recursive=False):
        cmd = ['zfs', 'hold']

        if recursive:
            cmd.append('-r')

        cmd.append(tag)
        cmd.append(self.name)

        sp.check_output(cmd)

    def holds(self):
        cmd = ['zfs', 'holds']

        cmd.append('-H')

        cmd.append(self.name)

        out = sp.check_output(cmd, universal_newlines=True)
        out = [tuple(line.split('\t')) for line in out.splitlines()]

        # return hold tag names only
        return [hold[1] for hold in out]

    def release(self, tag, recursive=False):
        cmd = ['zfs', 'release']

        if recursive:
            cmd.append('-r')

        cmd.append(tag)
        cmd.append(self.name)

        sp.check_output(cmd)
