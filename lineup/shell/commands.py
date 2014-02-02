#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import sys
import json
import logging
import coloredlogs
from lineup.utils import PipelineScanner

logger = logging.getLogger('lineup.cli')


class CLIError(Exception):
    pass


class Command(object):
    def __init__(self, root_parser, sub_parser):
        self.root_parser = root_parser
        self.sub_parser = sub_parser
        self.scanner = PipelineScanner(
            lookup_path=".")
        self.args = self.root_parser.parse_known_args()[0]

        log_level = getattr(logging, self.args.log_level.upper(), None)
        if log_level is None:
            raise CLIError('invalid log level: {0}'.format(log_level))

        coloredlogs.install(level=log_level)

    @property
    def pipeline(self):
        choices = self.scanner.get_pipelines()

        if not self.args.pipeline_name:
            raise CLIError('Missing pipeline name')

        name = self.args.pipeline_name[0]

        found = choices.get(name)
        if not found:
            raise CLIError('No such pipeline: {0}, options '
                           'are \033[1;32m{1}\033[0m'.format(name, choices.keys()))

        return choices[name]

    def execute(self, args, remainder):

        self.run_context = locals()
        try:
            self.when_executed(args, remainder)
        except KeyboardInterrupt:
            print "You Control-C'd. Kthankx bye"
            sys.exit(1)

        except CLIError as e:
            print "\033[1;31m", unicode(e), "\033[0m"

        except Exception as e:
            logger.exception(
                "Failed to run the command %s with args %s",
                self.name, str(args))

    @classmethod
    def execute_from_root_parser(self, root_parser, subparsers):
        for name, sub, CreateCommand in subparsers:
            command_args, remainder_args = sub.parse_known_args()
            if name in sys.argv:
                cmd = CreateCommand(root_parser, sub)
                return cmd.execute(command_args, remainder_args)


class RunPipeline(Command):
    name = 'run'

    def when_executed(self, arguments, remainder):
        logger.warning("Ran successfully!")


class PushToPipeline(Command):
    name = 'push'

    def get_json_data(self, arguments):
        arguments.json_data.pop(0)
        arguments.json_data.pop(0)
        return [json.loads(x) for x in arguments.json_data]

    def when_executed(self, arguments, remainder):
        data = self.get_json_data(arguments)
        logger.warning("Pushing %s", data)
