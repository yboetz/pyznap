"""
    pyznap.utils
    ~~~~~~~~~~~~~~

    Helper functions.

    :copyright: (c) 2018 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import os
import re
import logging
from subprocess import Popen, PIPE, TimeoutExpired, CalledProcessError
from .process import run

from datetime import datetime
from configparser import (ConfigParser, NoOptionError, MissingSectionHeaderError,
                          DuplicateSectionError, DuplicateOptionError)
from socket import timeout, gaierror
from pkg_resources import resource_string

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
        '~/.ssh/id_rsa' will be checked)
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
    # Append username & hostname attributes to ssh class
    ssh.user, ssh.host = user, host
    try:
        ssh.load_system_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    except (IOError, FileNotFoundError):
        ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pm.WarningPolicy())

    try:
        ssh.connect(hostname=host, port=port, username=user, key_filename=key, timeout=5,
                    look_for_keys=False)
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

    logger = logging.getLogger(__name__)

    if not os.path.isfile(path):
        logger.error('Error while loading config: File {:s} does not exist.'.format(path))
        return None

    parser = ConfigParser()
    try:
        parser.read(path)
    except (MissingSectionHeaderError, DuplicateSectionError, DuplicateOptionError) as e:
        logger.error('Error while loading config: {}'.format(e))
        return None

    config = []
    options = ['key', 'frequent', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean',
               'dest', 'dest_keys']

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
    """Splits a string of the form 'ssh:port:user@host:rpool/data' into its parts separated by ':'.

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


def create_config(path):
    """Initial configuration: Creates dir 'path' and puts sample config there

    Parameters
    ----------
    path : str
        Path to dir where to store config file

    """

    logger = logging.getLogger(__name__)

    CONFIG_FILE = os.path.join(path, 'pyznap.conf')
    config = resource_string(__name__, 'config/pyznap.conf').decode("utf-8")

    logger.info('Initial setup...')

    if not os.path.isdir(path):
        logger.info('Creating directory {:s}...'.format(path))
        try:
            os.mkdir(path, mode=int('755', base=8))
        except (PermissionError, FileNotFoundError, OSError) as e:
            logger.error('Could not create {:s}: {}'.format(path, e))
            logger.error('Aborting setup...')
            return 1
    else:
        logger.info('Directory {:s} does already exist...'.format(path))

    if not os.path.isfile(CONFIG_FILE):
        logger.info('Creating sample config {:s}...'.format(CONFIG_FILE))
        try:
            with open(CONFIG_FILE, 'w') as file:
                file.write(config)
        except (PermissionError, FileNotFoundError, IOError, OSError) as e:
            logger.error('Could not write to file {:s}: {}'.format(CONFIG_FILE, e))
        else:
            try:
                os.chmod(CONFIG_FILE, mode=int('644', base=8))
            except (PermissionError, IOError, OSError) as e:
                logger.error('Could not set correct permissions on file {:s}. Please do so manually...'
                             .format(CONFIG_FILE))
    else:
        logger.info('File {:s} does already exist...'.format(CONFIG_FILE))

    return 0


def check_recv(fsname, ssh=None):
    """Checks if there is already a 'zfs receive' for that dataset ongoing

    Parameters
    ----------
    fsname : str
        Name of the dataset
    ssh : paramiko.SSHClient, optional
        Open ssh connection (the default is None, which means check is done locally)

    Returns
    -------
    bool
        True if there is a 'zfs receive' ongoing or if an error is raised during checking. False if
        there is no 'zfs receive'.
    """

    logger = logging.getLogger(__name__)
    fsname_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, fsname) if ssh else fsname

    try:
        out = run(['ps', '-Ao', 'args='], stdout=PIPE, stderr=PIPE, timeout=5,
                  universal_newlines=True, ssh=ssh).stdout
    except (TimeoutExpired, SSHException) as err:
        logger.error('Error while checking \'zfs receive\' on {:s}: \'{}\'...'
                     .format(fsname_log, err))
        return True
    except CalledProcessError as err:
        logger.error('Error while checking \'zfs receive\' on {:s}: \'{:s}\'...'
                     .format(fsname_log, err.stderr.rstrip()))
        return True
    else:
        match = re.search(r'zfs (receive|recv).*({:s})(?=\n)'.format(fsname), out)
        if match:
            logger.error('Cannot send to {:s}, process \'{:s}\' already running...'
                         .format(fsname_log, match.group()))
            return True

    return False


def bytes_fmt(num):
    """Converts bytes to a human readable format

    Parameters
    ----------
    num : int,float
        Number of bytes

    Returns
    -------
    float
        Human readable format with binary prefixes
    """

    for x in ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if num < 1024:
            return "{:3.1f}{:s}".format(num, x)
        num /= 1024
    else:
        return "{:3.1f}{:s}".format(num, 'Y')
