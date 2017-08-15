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
        hourly = now.replace(microsecond=0, second=0, minute=0, hour=now.hour + (now.minute >= 30))
        daily = hourly.replace(hour=8)
        weekly = daily.replace(day=daily.day - daily.weekday())
        monthly = daily.replace(day=1)
        yearly = monthly.replace(month=1)

        snapname = 'pyznap_{:s}_'.format(now.strftime('%Y-%m-%d_%H:%M:%S'))

        if conf['hourly'] and (not snapshots['hourly'] or
                               (now - snapshots['hourly'][0][1]).total_seconds() > 3599 or
                               abs((now - hourly).total_seconds()) <= 120):
            filesystem.snapshot(snapname=snapname + 'hourly', recursive=True)

        if conf['daily'] and (not snapshots['daily'] or
                              (now - snapshots['daily'][0][1]).total_seconds() > 3599 or
                              abs((now - daily).total_seconds()) <= 120):
            filesystem.snapshot(snapname=snapname + 'daily', recursive=True)

        if conf['weekly'] and (not snapshots['weekly'] or
                               (now - snapshots['weekly'][0][1]).total_seconds() > 3599 or
                               abs((now - weekly).total_seconds()) <= 120):
            filesystem.snapshot(snapname=snapname + 'weekly', recursive=True)

        if conf['monthly'] and (not snapshots['monthly'] or
                                (now - snapshots['monthly'][0][1]).total_seconds() > 3599 or
                                abs((now - monthly).total_seconds()) <= 120):
            filesystem.snapshot(snapname=snapname + 'monthly', recursive=True)

        if conf['yearly'] and (not snapshots['yearly'] or
                               (now - snapshots['yearly'][0][1]).total_seconds() > 3599 or
                               abs((now - yearly).total_seconds()) <= 120):
            filesystem.snapshot(snapname=snapname + 'yearly', recursive=True)
