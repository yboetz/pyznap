"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS functions
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


class Remote:
    """
    Class to combine all variables necessary for ssh connection
    """
    def __init__(self, user, host, port=22, key=None, proxy=None):
        self.host = host
        self.user = user
        self.port = port

        self.key = key if key else '/home/{:s}/.ssh/id_rsa'.format(self.user)
        if not os.path.isfile(self.key):
            raise FileNotFoundError(self.key)

        self.proxy = proxy
        self.cmd = self.ssh_cmd()

    def ssh_cmd(self):
        """"Returns a command to connect via ssh"""
        hostsfile = '/home/{:s}/.ssh/known_hosts'.format(self.user)
        hostsfile = hostsfile if os.path.isfile(hostsfile) else '/dev/null'
        cmd = ['ssh', '{:s}@{:s}'.format(self.user, self.host),
               '-i', '{:s}'.format(self.key),
               '-o', 'UserKnownHostsFile={:s}'.format(hostsfile)]
        if self.proxy:
            cmd += ['-J', '{:s}'.format(self.proxy)]

        cmd += ['sudo']

        return cmd

    def test(self):
        """Tests if ssh connection can be made"""
        logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
        ssh = pm.SSHClient()
        try:
            ssh.load_system_host_keys('/home/{:s}/.ssh/known_hosts'.format(self.user))
        except FileNotFoundError:
            ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(pm.WarningPolicy())
        try:
            ssh.connect(hostname=self.host, username=self.user, port=self.port,
                        key_filename=self.key, timeout=5)
            ssh.exec_command('ls', timeout=5)
            return True
        except (AuthenticationException, BadAuthenticationType,
                BadHostKeyException, ChannelException, NoValidConnectionsError,
                PasswordRequiredException, SSHException, PartialAuthentication,
                ProxyCommandFailure, timeout, gaierror) as err:
            print('{:s} ERROR: Could not connect to host {:s}: {}...'
                  .format(logtime(), self.host, err))
            return False


def read_config(path):
    """Reads a config file and outputs a list of dicts with the given snapshot strategy. If ssh
    keyfiles do not exist it will take standard location in .ssh folder"""

    if not os.path.isfile(path):
        raise FileNotFoundError('File does not exist.')

    options = ['key', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean', 'dest',
               'dest_keys']

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
                elif option in ['hourly', 'daily', 'weekly', 'monthly', 'yearly']:
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
            if child['name'].startswith(parent['name']):
                for option in ['key', 'hourly', 'daily', 'weekly', 'monthly', 'yearly', 'clean']:
                    child[option] = child[option] if child[option] is not None else parent[option]
                child['snap'] = False
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
