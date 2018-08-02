"""
Created on Sat Aug 12 2017

@author: yboetz

Helper functions
"""

import os
import logging
from datetime import datetime
from configparser import ConfigParser, NoOptionError
from subprocess import Popen, PIPE
from socket import timeout, gaierror

import paramiko as pm
from paramiko.ssh_exception import (AuthenticationException, BadAuthenticationType,
                                    BadHostKeyException, ChannelException, NoValidConnectionsError,
                                    PasswordRequiredException, SSHException, PartialAuthentication,
                                    ProxyCommandFailure)


def exists(executable=''):
    """Tests if an executable exists on the system.

    Parameters:
    ----------
    executable : {str}, optional
        Name of the executable to test (the default is an empty string)

    Returns
    -------
    bool
        True if executable exists, False if not
    """

    assert isinstance(executable, str), "Input must be string."
    cmd = ['which', executable]
    out, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()

    return bool(out)


def open_ssh(user, host, key=None, port=22):
    """Opens an ssh connection to host.

    Parameters:
    ----------
    user : {str}
        Username to use
    host : {str}
        Host to connect to
    key : {str}, optional
        Path to ssh keyfile (the default is None, meaning the standard location
        '/home/user/.ssh/id_rsa' will be checked)
    port : {int}, optional
        Port number to connect to (the default is 22)

    Raises
    ------
    FileNotFoundError
        If keyfile does not exist
    SSHException
        General exception raised if anything goes wrong during ssh connection

    Returns
    -------
    paramiko.SSHClient
        Open ssh connection.
    """

    logger = logging.getLogger(__name__)

    if not key:
        key = os.path.expanduser('~/.ssh/id_rsa')
    if not os.path.isfile(key):
        logger.error('{} is not a valid ssh key file...'.format(key))
        raise FileNotFoundError(key)

    ssh = pm.SSHClient()
    try:
        ssh.load_system_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    except FileNotFoundError:
        ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pm.WarningPolicy())

    try:
        ssh.connect(hostname=host, port=port, username=user, key_filename=key, timeout=5)
        # Test connection
        ssh.exec_command('ls', timeout=5)
    except (AuthenticationException, BadAuthenticationType,
            BadHostKeyException, ChannelException, NoValidConnectionsError,
            PasswordRequiredException, SSHException, PartialAuthentication,
            ProxyCommandFailure, timeout, gaierror) as err:
        logger.error('Could not connect to host {:s}: {}...'.format(host, err))
        # Raise general exception to be catched outside
        raise SSHException(err)

    return ssh


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given snapshot strategy.

    Parameters:
    ----------
    path : {str}
        Path to the config file

    Raises
    ------
    FileNotFoundError
        If path does not exist

    Returns
    -------
    list of dict
        Full config list containing all strategies for different filesystems
    """

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['key', 'frequent', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean',
               'dest', 'dest_keys']

    parser = ConfigParser()
    parser.read(path)

    config = []
    for section in parser.sections():
        dic = {}
        config.append(dic)
        dic['name'] = section

        for option in options:
            try:
                value = parser.get(section, option)
            except NoOptionError:
                dic[option] = None
            else:
                if option in ['key']:
                    dic[option] = value if os.path.isfile(value) else None
                elif option in ['frequent', 'hourly', 'daily', 'weekly', 'monthly', 'yearly']:
                    dic[option] = int(value)
                elif option in ['snap', 'clean']:
                    dic[option] = {'yes': True, 'no': False}.get(value.lower(), None)
                elif option in ['dest']:
                    dic[option] = [i.strip() for i in value.split(',')]
                elif option in ['dest_keys']:
                    dic[option] = [i.strip() if os.path.isfile(i.strip()) else None
                                   for i in value.split(',')]
    # Pass through values recursively
    for parent in config:
        for child in config:
            if parent == child:
                continue
            child_parent = '/'.join(child['name'].split('/')[:-1])  # get parent of child filesystem
            if child_parent.startswith(parent['name']):
                for option in ['key', 'frequent', 'hourly', 'daily', 'weekly', 'monthly', 'yearly',
                               'snap', 'clean']:
                    child[option] = child[option] if child[option] is not None else parent[option]
    # Sort by pathname
    config = sorted(config, key=lambda entry: entry['name'].split('/'))

    return config


def parse_name(value):
    """Splits a string of the form 'ssh:port:user@host:rpool/data' into its parts.

    Parameters:
    ----------
    value : {str}
        String to split up

    Returns
    -------
    (str, str, str, str, int)
        Tuple containing the different parts of the string
    """

    if value.startswith('ssh'):
        _type, port, host, fsname = value.split(':', maxsplit=3)
        port = int(port) if port else 22
        user, host = host.split('@', maxsplit=1)
    else:
        _type, user, host, port = 'local', None, None, None
        fsname = value
    return _type, fsname, user, host, port
