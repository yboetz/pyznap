#!/usr/bin/env python
"""
    pyznap.main
    ~~~~~~~~~~~~~~

    ZFS snapshot tool written in python.

    :copyright: (c) 2018-2019 by Yannick Boetzel.
    :license: GPLv3, see LICENSE for more details.
"""

import sys
import os
import logging
from logging.config import fileConfig
from argparse import ArgumentParser
from datetime import datetime
from .utils import read_config, create_config
from .clean import clean_config
from .take import take_config
from .send import send_config


DIRNAME = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = '/etc/pyznap/'

def _main():
    """pyznap main function. Parses arguments and calls snap/clean/send functions accordingly.

    Returns
    -------
    int
        Exit code
    """

    parser = ArgumentParser(prog='pyznap', description='ZFS snapshot tool written in python')
    parser.add_argument('-v', '--verbose', action="store_true",
                        dest="verbose", help='print more verbose output')
    parser.add_argument('--config', action="store",
                        dest="config", help='path to config file')
    subparsers = parser.add_subparsers(dest='command')

    parser_setup = subparsers.add_parser('setup', help='initial setup')
    parser_setup.add_argument('-p', '--path', action='store',
                              dest='path', help='pyznap config dir. default is {:s}'.format(CONFIG_DIR))

    parser_snap = subparsers.add_parser('snap', help='zfs snapshot tools')
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
                             dest='key', help='ssh key if only source or dest is remote')
    parser_send.add_argument('-j', '--source-key', action="store",
                             dest='source_key', help='ssh key for source if both are remote')
    parser_send.add_argument('-k', '--dest-key', action="store",
                             dest='dest_key', help='ssh key for dest if both are remote')
    parser_send.add_argument('-c', '--compress', action="store",
                             dest='compress', help='compression to use for ssh transfer. default is lzop')
    parser_send.add_argument('-e', '--exclude', nargs = '+',
                             dest='exclude', help='datasets to exclude')
    parser_send.add_argument('-w', '--raw', action="store_true",
                             dest='raw', help='raw zfs send. default is false')
    parser_send.add_argument('-r', '--resume', action="store_true",
                             dest='resume', help='resumable send. default is false')
    parser_send.add_argument('--dest-auto-create', action="store_true",
                             dest='dest_auto_create',
                             help='create destination if it does not exist. default is false')
    parser_send.add_argument('--retries', action="store", type=int,
                             dest='retries', default=0,
                             help='number of retries on error. default is 0')
    parser_send.add_argument('--retry-interval', action="store", type=int,
                             dest='retry_interval', default=10,
                             help='interval in seconds between retries. default is 10')

    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args = parser.parse_args()

    loglevel = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=loglevel, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%b %d %H:%M:%S', stream=sys.stdout)
    logger = logging.getLogger(__name__)

    logger.info('Starting pyznap...')

    if args.command in ('snap', 'send'):
        config_path = args.config if args.config else os.path.join(CONFIG_DIR, 'pyznap.conf')
        config = read_config(config_path)
        if config == None:
            return 1

    if args.command == 'setup':
        path = args.path if args.path else CONFIG_DIR
        create_config(path)

    elif args.command == 'snap':
        # Default if no args are given
        if not args.take and not args.clean:
            args.full = True

        if args.take or args.full:
            take_config(config)

        if args.clean or args.full:
            clean_config(config)

    elif args.command == 'send':
        if args.source and args.dest:
            # use args.key if either source or dest is remote
            source_key, dest_key = None, None
            if args.dest.startswith('ssh'):
                dest_key = [args.key] if args.key else None
            elif args.source.startswith('ssh'):
                source_key = args.key if args.key else None
            # if source_key and dest_key are given, overwrite previous value
            source_key = args.source_key if args.source_key else source_key
            dest_key = [args.dest_key] if args.dest_key else dest_key
            # get exclude rules
            exclude = [args.exclude] if args.exclude else None
            # check if raw send was requested
            raw = [args.raw] if args.raw else None
            # compress ssh zfs send/receive
            compress = [args.compress] if args.compress else None
            # use receive resume token
            resume = [args.resume] if args.resume else None
            # retry zfs send/receive
            retries = [args.retries] if args.retries else None
            # wait interval for retry
            retry_interval = [args.retry_interval] if args.retry_interval else None
            # automatically create dest dataset if it does not exist
            dest_auto_create = [args.dest_auto_create] if args.dest_auto_create else None

            send_config([{'name': args.source, 'dest': [args.dest], 'key': source_key,
                          'dest_keys': dest_key, 'compress': compress, 'exclude': exclude,
                          'raw_send': raw, 'resume': resume, 'dest_auto_create': dest_auto_create,
                          'retries': retries, 'retry_interval': retry_interval}])

        elif args.source and not args.dest:
            logger.error('Missing dest...')
        elif args.dest and not args.source:
            logger.error('Missing source...')
        else:
            send_config(config)

    logger.info('Finished successfully...\n')
    return 0


def main():
    """Wrapper around _main function to catch KeyboardInterrupt

    Returns
    -------
    int
        Exit code
    """

    logger = logging.getLogger(__name__)
    try:
        return _main()
    except KeyboardInterrupt:
        logger.error('KeyboardInterrupt - exiting gracefully...\n')
        return 1


if __name__ == "__main__":
    sys.exit(main())
