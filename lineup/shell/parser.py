#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa

from __future__ import unicode_literals
import sys
import argparse
import logging
import coloredlogs


logger = logging.getLogger('lineup.cli')

from lineup.shell.commands import (
    Command,
    RunPipeline,
    StopPipeline,
    PushToPipeline,
    PipelinesCmd,
)


def main():
    parser = argparse.ArgumentParser(
        description='Distributed Pipeline Framework for python')

    parser.add_argument(
        '-t', '--threads',
        type=int,
        help=(
            'The number of threads that will consume and/or produce '
            'for each queue, minus the last queue (which is made '
            'just for output'),
        default=1,
    )

    parser.add_argument(
        '-W', '--working-dir',
        type=str,
        help=("A path where lineup will scan for pipelines recursively"),
        default=".",
    )

    parser.add_argument(
        '-l', '--log-level',
        type=str,
        default='info',
        help=('choose between: DEBUG, INFO, WARNING, ERROR; case insensitive.'),
    )

    parser.add_argument(
        '-f', '--file',
        metavar='PIPELINE_FILE',
        type=str,
        nargs=1,
        help='path to the python file containing the pipeline',
    )

    parser.add_argument(
        '-p', '--pipeline',
        metavar='PIPELINE_NAME',
        type=str,
        nargs=1,
        help='What pipeline to work with',
    )

    rargs = parser.parse_known_args()[0]
    if rargs.pipeline_name:
        pipeline_name = rargs.pipeline_name[0]
    else:
        pipeline_name = 'UNKNOWN'

    subparsers = parser.add_subparsers()

    push = subparsers.add_parser(
        'push', help='Pushes JSON data to the given pipeline')

    push.add_argument(
        'json_data',
        metavar='JSON_DATA',
        type=str,
        nargs="+",
        help=('the json data in which the given pipeline will '
              'be fed with. Accepts more than one json payload'
              ', in that case the data will be enqueued in '
              'that same order.'),
    )

    run = subparsers.add_parser(
        'run', help='Runs the given pipeline in foreground')

    run.add_argument('-D', '--daemon',
                     action='store_true',
                     help=('Runs the given pipeline as a '
                           'daemon, you might want this '
                           'option when running in production'),
    )

    run.add_argument('--output',
                     metavar='OUTPUT_URI',
                     type=str,
                     default='rpush@{0}-done'.format(pipeline_name),
                     help=('Output to a given list'),
    )

    stop = subparsers.add_parser(
        'stop', help='Stops the given pipeline')

    stop.add_argument('--output',
                     metavar='OUTPUT_URI',
                     type=str,
                     default='rpush@{0}-done'.format(pipeline_name),
                     help=('Output to a given list'),
    )
    pipelines = subparsers.add_parser(
        'pipelines', help='Special command for handling pipelines')

    Command.execute_from_root_parser(parser, subparsers=[
        ('run', run, RunPipeline),
        ('stop', stop, StopPipeline),
        ('push', push, PushToPipeline),
        ('pipelines', pipelines, PipelinesCmd),
    ])



if __name__ == '__main__':
    main()
