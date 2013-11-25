#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals


import argparse
import logging
import os
import sys


def add_parser_pipeline(subparsers):
    parser = subparsers.add_parser(
        'pipeline', help='Run the given pipeline name')

    parser.add_argument(
        '-t', '--threads', type=int, help='The thread pool size', default=10)

    parser.set_defaults(command='pipeline')
    return parser


def get_install_command(args):
    print "COOOL"


def main():
    parser = argparse.ArgumentParser(
        description='Curdles your cheesy code and extracts its binaries')

    # General arguments. All the commands have access to the following options

    levels = filter(lambda x: not isinstance(x, int), logging._levelNames.keys())
    parser.add_argument(
        '-l', '--log-level', default='CRITICAL', choices=levels,
        help='Log verbosity level (for nerds): {0}'.format(', '.join(levels)))

    parser.add_argument(
        '--log-file', type=argparse.FileType('w'), default=sys.stderr,
        help='File to write the log')

    parser.add_argument(
        '--log-name', default=None,
        help=(
            'Name of the logger you want to set the level with '
            '`-l` (for the nerdests)'
        ))

    parser.add_argument(
        '-q', '--quiet', action='store_true', default=False,
        help='No output unless combined with `-l\'')

    parser.add_argument(
        '-v', '--version', action='version',
        version='%(prog)s {0}'.format(__version__))

    subparsers = parser.add_subparsers()
    add_parser_pipeline(subparsers)

    args = parser.parse_args()

    if not args.quiet:
        # Set the log level for the requested logger
        handler = logging.StreamHandler(stream=args.log_file)
        handler.setLevel(args.log_level)
        handler.setFormatter(logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s'))
        logging.getLogger(args.log_name).setLevel(level=args.log_level)
        logging.getLogger(args.log_name).addHandler(handler)

    # Here we choose which function will be called to setup the command
    # instance that will be ran. Notice that all the `add_parser_*` functions
    # *MUST* set the variable `command` using `parser.set_defaults` otherwise
    # we'll get an error here.
    command = {
        'pipeline': get_pipeline_command,
    }[args.command](args)

    try:
        return command.run()
    except KeyboardInterrupt:
        raise SystemExit(0)
