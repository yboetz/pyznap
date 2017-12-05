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
from utils import read_config, take_snap, clean_config, send_config

__version__ = '0.1.0'

if __name__ == "__main__":
    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Starting pyznap...'.format(logtime()))

    parser = ArgumentParser(prog='pyznap', description='ZFS snapshot tool written in python')
    parser.add_argument('--config', action="store",
                         dest="config", help='path to config file')
    parser.add_argument('--version', action="store_true", help='prints version and exits')
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
    parser_send.add_argument('-i', '--key', action="store",
                             dest='key', help='ssh key for destination')

    args = parser.parse_args(sys.argv[1:])

    try:
        config_path = args.config if args.config else '/etc/pyznap/pyznap.conf'
        config = read_config(config_path)
    except FileNotFoundError as err:
        print('{:s} ERROR: Config file does not exist...'.format(logtime()))
        sys.exit(1)
    except MissingSectionHeaderError as err:
        print('{:s} ERROR: Config file contains no section headers...'.format(logtime()))
        sys.exit(1)

    if args.version:
        print('{:s} INFO: pyznap version: {:s}'.format(logtime(), __version__))

    elif args.command == 'snap':
        # Default if no args are given
        if not args.take and not args.clean:
            args.full = True

        if args.take or args.full:
            take_snap(config)

        if args.clean or args.full:
            clean_config(config)

    elif args.command == 'send':
        if args.source and args.dest:
            key = [args.key] if args.key else None
            send_config([{'name': args.source, 'dest': [args.dest], 'dest_keys': key}])
        elif args.source and not args.dest:
            print('{:s} ERROR: Missing dest...'.format(logtime()))
        elif args.dest and not args.source:
            print('{:s} ERROR: Missing source...'.format(logtime()))
        else:
            send_config(config)

    print('{:s} INFO: Finished successfully...\n'.format(logtime()))
    sys.exit(0)
