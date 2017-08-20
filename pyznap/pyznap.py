#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS snapshot tool written in python.
"""

import sys
from argparse import ArgumentParser
from datetime import datetime
from configparser import MissingSectionHeaderError
from utils import take_snap, clean_snap, read_config, send_snap

if __name__ == "__main__":
    parser = ArgumentParser(prog='pyznap', description='ZFS snapshot tool written in python')
    parser.add_argument('--config', action="store",
                         dest="config", help='path to config file')
    subparsers = parser.add_subparsers(dest='command')

    parser_snap = subparsers.add_parser('snap', help='snapshot tools')
    parser_snap.add_argument('--take', action="store_true",
                             help='take snapshots according to config file')
    parser_snap.add_argument('--clean', action="store_true",
                             help='clean old snapshots according to config file')
    parser_snap.add_argument('--full', action="store_true",
                             help='take snapshots then clean old according to config file')

    parser_send = subparsers.add_parser('send', help='zfs send/receive tools')
    parser_send.add_argument('-s', '--source', action="store",
                             dest='source', help='source filesystem')
    parser_send.add_argument('-d', '--dest', action="store",
                             dest='dest', help='destination filesystem')

    args = parser.parse_args(sys.argv[1:])

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

    try:
        config_path = args.config if args.config else '/etc/pyznap/pyznap.conf'
        config = read_config(config_path)
    except FileNotFoundError as err:
        print('{:s} ERROR: Config file does not exist...'.format(logtime()))
        sys.exit(1)
    except MissingSectionHeaderError as err:
        print('{:s} ERROR: Config file contains no section headers...'.format(logtime()))
        sys.exit(1)

    if args.command == 'snap':
        if args.full:
            take_snap(config)
            clean_snap(config)
            sys.exit(0)

        if args.take:
            take_snap(config)
            sys.exit(0)

        if args.clean:
            clean_snap(config)
            sys.exit(0)

    elif args.command == 'send':
        send_snap(config)
        sys.exit(0)
