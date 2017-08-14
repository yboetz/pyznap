#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS snapshot tool written in python.
"""

import os
from datetime import datetime
from configparser import ConfigParser, NoOptionError
from weir import zfs
#from subprocess import Popen, PIPE
#from time import time, sleep


def read_config(path):
    """Reads a config file and outputs a dict with the given
    snapshot strategy"""

    if not os.path.isfile(path):
        return False

    options = ['hourly', 'daily', 'weekly', 'monthly', 'yearly', 'snap', 'clean']

    config = ConfigParser()
    config.read(path)

    res = []
    for section in config.sections():
        dic = {}
        res.append(dic)
        dic['name'] = section

        for option in options:
            try:
                dic[option] = int(config.get(section,option))
            except NoOptionError:
                dic[option] = None
            except ValueError:
                if option in ['snap', 'clean']:
                    dic[option] = True if option == 'yes' else False
                else:
                    dic[option] = None

    return res

def take_snap(config):
    for conf in config:
        fs_name = conf['name']
        hourly = conf['hourly']
        daily = conf['daily']
        weekly = conf['weekly']
        monthly = conf['monthly']
        yearly = conf['yearly']
        snap = conf['snap']
        clean = conf['clean']

        try:
            filesystem = zfs.open(fs_name)
        except ValueError as err:
            print(err)
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            snap_name = snap.name
            snap_date = datetime.fromtimestamp(int(snap.getprop['creation']['value']))
            for snap_type in snapshots.keys():
                if snap_name.endswith(snap_type):
                    snapshots[snap_type].append((snap_name, snap_date))
        
        if hourly:
