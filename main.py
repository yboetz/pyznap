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
from weir.process import DatasetNotFoundError
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
                    dic[option] = True if config.get(section,option) == 'yes' else False
                else:
                    dic[option] = None

    return res

def take_snap(config):
    for conf in config:
        if not conf['snap']:
            continue

        try:
            filesystem = zfs.open(conf['name'])
        except (ValueError, DatasetNotFoundError) as err:
            print(err)
            continue

        snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
        for snap in filesystem.snapshots():
            # Ignore snapshots not taken with pyznap
            if not snap.name.split('@')[1].startswith('pyznap'):
                continue
            snap_time = datetime.fromtimestamp(int(snap.getprop('creation')['value']))
            snap_type = snap.name.split('_')[-1]

            try:
                snapshots[snap_type].append((snap.name, snap_time))
            except KeyError:
                continue

        for snap_type, snaps in snapshots.items():
            snapshots[snap_type] = sorted(snaps, key=lambda x: x[1], reverse=True)

        now = datetime.today()
        snapname = 'pyznap_{:s}_'.format(now.strftime('%Y-%m-%d_%H:%M:%S'))

        if conf['hourly'] and (not snapshots['hourly'] or
                               snapshots['hourly'][0][1].hour != now.hour):
            print('Taking snapshot {:s}@{:s}'.format(conf['name'], snapname + 'hourly'))
            filesystem.snapshot(snapname=snapname + 'hourly', recursive=True)

        if conf['daily'] and (not snapshots['daily'] or
                              snapshots['daily'][0][1].day != now.day):
            print('Taking snapshot {:s}@{:s}'.format(conf['name'], snapname + 'daily'))
            filesystem.snapshot(snapname=snapname + 'daily', recursive=True)

        if conf['weekly'] and (not snapshots['weekly'] or
                               snapshots['weekly'][0][1].isocalendar()[1] != now.isocalendar()[1]):
            print('Taking snapshot {:s}@{:s}'.format(conf['name'], snapname + 'weekly'))
            filesystem.snapshot(snapname=snapname + 'weekly', recursive=True)

        if conf['monthly'] and (not snapshots['monthly'] or
                                snapshots['monthly'][0][1].month != now.month):
            print('Taking snapshot {:s}@{:s}'.format(conf['name'], snapname + 'monthly'))
            filesystem.snapshot(snapname=snapname + 'monthly', recursive=True)

        if conf['yearly'] and (not snapshots['yearly'] or
                               snapshots['yearly'][0][1].year != now.year):
            print('Taking snapshot {:s}@{:s}'.format(conf['name'], snapname + 'yearly'))
            filesystem.snapshot(snapname=snapname + 'yearly', recursive=True)
