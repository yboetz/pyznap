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
from datetime import datetime


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

    def __init__(self, user, host, key=None, port=22):
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

        # try to connect to set up ssh connection
        try:
            sp.check_output(self.cmd + ['ls'], timeout=10, stderr=sp.PIPE)
        except sp.CalledProcessError as err:
            self.logger.error('Error while connecting to {:s}@{:s}: {}...'
                              .format(self.user, self.host, err.stderr.decode()))
            self.close()
            raise SSHException(err.stderr.decode())
        except sp.TimeoutExpired as err:
            self.logger.error('Error while connecting to {:s}@{:s}: {}...'
                              .format(self.user, self.host, err))
            self.close()
            raise SSHException(err)

    def close(self):
        """Closes the ssh connection by invoking '-O exit' (deletes socket file)"""

        try:
            sp.check_output(self.cmd + ['-O', 'exit'], timeout=5, stderr=sp.PIPE)
        except (sp.CalledProcessError, sp.TimeoutExpired):
            pass

    def __del__(self):
        self.close()
