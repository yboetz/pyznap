"""
Created on Sat Aug 12 2017

@author: yboetz

Helper functions
"""

import os
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
    """Tests if an executable exists on the system."""

    assert isinstance(executable, str), "Input must be string."
    cmd = ['which', executable]
    out, _ = Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()

    return bool(out)


def open_ssh(user, host, key=None, port=22):
    """Opens an ssh connection to host"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

    if not key:
        key = '/home/{:s}/.ssh/id_rsa'.format(user)
    if not os.path.isfile(key):
        print('{:s} ERROR: {} is not a valid ssh key file...'.format(logtime(), key))
        raise FileNotFoundError(key)

    ssh = pm.SSHClient()
    try:
        ssh.load_system_host_keys('/home/{:s}/.ssh/known_hosts'.format(user))
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
        print('{:s} ERROR: Could not connect to host {:s}: {}...'.format(logtime(), host, err))
        # Raise general exception to be catched outside
        raise SSHException(err)

    return ssh


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given snapshot strategy. If ssh
    keyfiles do not exist it will take standard location in .ssh folder"""

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
    """Splits a string of the form 'ssh:port:user@host:rpool/data' into its parts"""
    if value.startswith('ssh'):
        _type, port, host, fsname = value.split(':', maxsplit=3)
        port = int(port) if port else 22
        user, host = host.split('@', maxsplit=1)
    else:
        _type, user, host, port = 'local', None, None, None
        fsname = value
    return _type, fsname, user, host, port
