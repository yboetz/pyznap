#!/home/yboetz/.virtualenvs/pyznap/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 12 2017

@author: yboetz

ZFS snapshot tool written in python.
"""

import sys
from argparse import ArgumentParser
from zfs import take_snap, clean_snap, read_config

if __name__ == "__main__":
    parser = ArgumentParser(prog='pyznap', description='ZFS snapshot tool written in python')
    subparsers = parser.add_subparsers(dest='command')

    parser_snap = subparsers.add_parser('snap', help='Snapshot tools')
    parser_snap.add_argument('-c', '--config', action="store",
                             dest="config", help='Path to config file')
    parser_snap.add_argument('--take', action="store_true",
                             help='Take snapshots according to config file')
    parser_snap.add_argument('--clean', action="store_true",
                             help='Clean old snapshots according to config file')
    parser_snap.add_argument('--full', action="store_true",
                             help='Take snapshots then clean old according to config file')

    parser_send = subparsers.add_parser('send', help='ZFS send/receive tools')
    parser_send.add_argument('-s', '--source', action="store",
                             dest='source', help='Source filesystem')
    parser_send.add_argument('-d', '--dest', action="store",
                             dest='dest', help='Destination filesystem')

    args = parser.parse_args(sys.argv[1:])


    if args.command == 'snap':
        config_path = args.config if args.config else '/etc/pyznap/pyznap.conf'

        try:
            config = read_config(config_path)
        except FileNotFoundError:
            print('Could not read config file...')
            sys.exit(1)

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
        print('sending...')
        sys.exit(0)
