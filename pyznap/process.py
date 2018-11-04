"""
    pyznap.process
    ~~~~~~~~~~~~~~

    Catch ZFS subprocess errors, forked from https://bitbucket.org/stevedrake/weir/.

    :copyright: (c) 2015-2018 by Stephen Drake, Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import re
import shutil
import errno as _errno
import subprocess as sp
import socket
from paramiko import SSHException

PIPE = sp.PIPE


class ZFSError(OSError):
    def __init__(self, dataset):
        super(ZFSError, self).__init__(self.errno, self.strerror, dataset)

class DatasetNotFoundError(ZFSError):
    errno = _errno.ENOENT
    strerror = 'dataset does not exist'

class DatasetExistsError(ZFSError):
    errno = _errno.EEXIST
    strerror = 'dataset already exists'

class DatasetBusyError(ZFSError):
    errno = _errno.EBUSY
    strerror = 'dataset is busy'

class HoldTagNotFoundError(ZFSError):
    errno = _errno.ENOENT
    strerror = 'no such tag on this dataset'

class HoldTagExistsError(ZFSError):
    errno = _errno.EEXIST
    strerror = 'tag already exists on this dataset'

class CompletedProcess(sp.CompletedProcess):
    def check_returncode(self):
        """Check for known errors of the form "cannot <action> <dataset>: <reason>"

        Raises
        ------
        DatasetNotFoundError, DatasetExistsError, DatasetBusyError, HoldTagNotFoundError, HoldTagExistsError
            Raises corresponding ZFS error
        """

        if self.returncode == 1:
            pattern = r"^cannot ([^ ]+(?: [^ ]+)*?) ([^ ]+): (.+)$"
            # only use first line of stderr to match zfs errors
            match = re.search(pattern, self.stderr.splitlines()[0])
            if match:
                _, dataset, reason = match.groups()
                if dataset[0] == dataset[-1] == "'":
                    dataset = dataset[1:-1]
                for error in (DatasetNotFoundError,
                              DatasetExistsError,
                              DatasetBusyError,
                              HoldTagNotFoundError,
                              HoldTagExistsError):
                    if reason == error.strerror:
                        raise error(dataset)

        # did not match known errors, defer to superclass
        super(CompletedProcess, self).check_returncode()


# ssh is a connected instance of paramiko.client.SSHClient
def check_output(*popenargs, timeout=None, ssh=None, **kwargs):
    """Run command with arguments and return its output. Works over ssh.

    Parameters:
    ----------
    *popenargs : {}
        Variable length argument list, same as Popen constructor
    **kwargs : {}
        Arbitrary keyword arguments, same as Popen constructor
    timeout : {float}, optional
        Timeout in seconds, if process takes too long TimeoutExpired will be raised (the default is
        None, meaning no timeout)
    ssh : {paramiko.SSHClient}, optional
        Open ssh connection for remote execution (the default is None)

    Raises
    ------
    ValueError
        Raise ValueError for forbidden kwargs

    Returns
    -------
    None or list of lists
        List of all lines from the output, seperated at '\t' into lists
    """

    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    if 'universal_newlines' in kwargs:
        raise ValueError('universal_newlines argument not allowed, it will be overridden.')
    if 'input' in kwargs:
        raise ValueError('input argument not allowed, it will be overridden.')

    ret = run(*popenargs, stdout=PIPE, stderr=PIPE, timeout=timeout,
              universal_newlines=True, ssh=ssh, **kwargs)
    ret.check_returncode()
    out = ret.stdout

    return None if out is None else [line.split('\t') for line in out.splitlines()]


def run(*popenargs, timeout=None, check=False, ssh=None, **kwargs):
    """Run command with ZFS arguments and return a CompletedProcess instance. Works over ssh.

    Parameters:
    ----------
    *popenargs : {}
        Variable length argument list, same as Popen constructor
    **kwargs : {}
        Arbitrary keyword arguments, same as Popen constructor
    timeout : {float}, optional
        Timeout in seconds, if process takes too long TimeoutExpired will be raised (the default is
        None, meaning no timeout)
    check : {bool}, optional
        Check return code (the default is False, meaning return code will not be checked)
    ssh : {paramiko.SSHClient}, optional
        Open ssh connection for remote execution (the default is None)

    Raises
    ------
    sp.TimeoutExpired
        Raised if process takes longer than given timeout
    sp.CalledProcessError
        Raised if check=True and return code != 0

    Returns
    -------
    subprocess.CompletedProcess
        Return instance of CompletedProcess with given return code, stdout and stderr
    """

    if ssh is None:
        with sp.Popen(*popenargs, **kwargs) as process:
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except sp.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                raise sp.TimeoutExpired(process.args, timeout, output=stdout, stderr=stderr)
            except:
                process.kill()
                process.wait()
                raise
            retcode = process.poll()
    else:
        args = ' '.join(popenargs[0]) if not isinstance(popenargs[0], str) else popenargs[0]

        try:
            stdin, stdout, stderr = ssh.exec_command(args, *popenargs[1:], timeout=timeout)
            if kwargs.get('stdin', None):
                shutil.copyfileobj(kwargs['stdin'], stdin, 128*1024)
            stdin.close()
            retcode = stdout.channel.recv_exit_status()
            stdout, stderr = ''.join(stdout.readlines()), ''.join(stderr.readlines())
        except socket.timeout:
            stdout, stderr = None, None
            raise sp.TimeoutExpired(popenargs[0], timeout, output=stdout, stderr=stderr)

    if check and retcode:
        raise sp.CalledProcessError(retcode, popenargs[0], output=stdout, stderr=stderr)

    return CompletedProcess(popenargs[0], retcode, stdout, stderr)
