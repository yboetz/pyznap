"""
    pyznap.ssh
    ~~~~~~~~~~~~~~

    ssh connection.

    :copyright: (c) 2018-2019 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import os
import logging
import subprocess as sp
import pyznap.utils
from datetime import datetime
from .process import run


class SSHException(Exception):
    """General ssh exception to be raised if anything fails"""
    pass


class SSH:
    """SSH class.

    Attributes
    ------
    logger : {logging.logger}
        logger to use
    user : {str}
        User to use
    host : {str}
        Host to connect to
    key : {str}
        Path to keyfile
    port : {int}
        Port number to connect to
    socket : {str}
        Path to socket file (used with '-o ControlPath')
    cmd : {list of str}
        ssh command to use with subprocess
    """

    def __init__(self, user, host, key=None, port=22, compress=None):
        """Initializes SSH class.

        Parameters
        ----------
        user : {str}
            User to use
        host : {str}
            Host to connect to
        key : {str}, optional
            Path to keyfile (the default is None, meaning the standard location
            '~/.ssh/id_rsa' will be checked)
        port : {int}, optional
            Port number to connect to (the default is 22)

        Raises
        ------
        FileNotFoundError
            If keyfile does not exist
        SSHException
            General exception raised if anything goes wrong during ssh connection        
        """

        self.logger = logging.getLogger(__name__)

        self.user = user
        self.host = host
        self.port = port
        self.socket = '/tmp/pyznap_{:s}@{:s}:{:d}_{:s}'.format(self.user, self.host, self.port, 
                      datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))
        self.key = key or os.path.expanduser('~/.ssh/id_rsa')

        if not os.path.isfile(self.key):
            self.logger.error('{} is not a valid ssh key file...'.format(self.key))
            raise FileNotFoundError(self.key)

        self.cmd = ['ssh', '-i', self.key, '-o', 'ControlMaster=auto', '-o', 'ControlPersist=1m',
                    '-o', 'ControlPath={:s}'.format(self.socket), '-p', str(self.port), 
                    '{:s}@{:s}'.format(self.user, self.host)]

        # setup ControlMaster. Process will hang if we call Popen with stderr=sp.PIPE, see
        # https://lists.mindrot.org/pipermail/openssh-unix-dev/2014-January/031976.html
        try:
            run(['exit'], timeout=10, ssh=self)
        except (sp.CalledProcessError, sp.TimeoutExpired):
            pass

        # check if ssh connection is up
        try:
            run(['exit'], timeout=5, check=True, stdout=sp.PIPE, stderr=sp.PIPE, ssh=self)
        except (sp.CalledProcessError, sp.TimeoutExpired) as err:
            message = err.stderr.rstrip().decode() if hasattr(err, 'stderr') else err

            self.logger.error('Error while connecting to {:s}@{:s}: {}...'
                              .format(self.user, self.host, message))
            self.close()
            raise SSHException(message)

        # set up compression
        self.compress, self.decompress = self.check_compression(compress)
        # set up mbuffer
        self.mbuffer = self.check_mbuffer()


    def check_compression(self, _type):
        """Checks if compression algo is available on source and dest.

        Parameters
        ----------
        _type : {str}
            Type of compression to use

        Returns
        -------
        tuple(List(str))
            Tuple of compress/decompress commands to use, (None, None) if compression is not available
        """

        if _type == None or _type.lower() == 'none':
            return None, None

        # compress/decompress commands of different compression tools
        algos = {'gzip': (['gzip', '-3'], ['gzip', '-dc']),
                 'lzop': (['lzop'], ['lzop', '-dfc']),
                 'bzip2': (['bzip2'], ['bzip2', '-dfc']),
                 'pigz': (['pigz'], ['pigz', '-dc']),
                 'xz': (['xz'], ['xz', '-d']),
                 'lz4': (['lz4'], ['lz4', '-dc'])}

        if _type not in algos:
            self.logger.warning('Compression method {:s} not supported. Will continue without...'.format(_type))
            return None, None

        from pyznap.utils import exists
        # check if compression is available on source and dest
        if not exists(_type):
            self.logger.warning('{:s} does not exist, continuing without compression...'
                                .format(_type))
            return None, None
        if not exists(_type, ssh=self):
            self.logger.warning('{:s} does not exist on {:s}@{:s}, continuing without compression...'
                                .format(_type, self.user, self.host))
            return None, None

        return algos[_type]


    def check_mbuffer(self):
        """Checks if mbuffer is available on dest

        Returns
        -------
        List(str)
            mbuffer command to use on dest
        """

        from pyznap.utils import exists

        if not exists('mbuffer', ssh=self):
            return None
        else:
            return ['mbuffer', '-q', '-s', '128K', '-m', '512M']


    def close(self):
        """Closes the ssh connection by invoking '-O exit' (deletes socket file)"""

        try:
            run(['-O', 'exit'], timeout=5, stderr=sp.PIPE, ssh=self)
        except (sp.CalledProcessError, sp.TimeoutExpired):
            pass


    def __del__(self):
        self.close()
