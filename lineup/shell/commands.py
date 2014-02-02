#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import sys
import json
import logging
import coloredlogs
from lineup import JSONRedisBackend

from lineup.utils import PipelineScanner

logger = logging.getLogger('lineup.cli')


class CLIError(Exception):
    pass


class Command(object):
    def __init__(self, root_parser, sub_parser):
        self.root_parser = root_parser
        self.sub_parser = sub_parser
        self.args = self.root_parser.parse_known_args()[0]

        log_level = getattr(logging, self.args.log_level.upper(), None)
        if log_level is None:
            raise CLIError('invalid log level: {0}'.format(log_level))

        coloredlogs.install(level=log_level)
        self._pipeline = None
        self.scanner = PipelineScanner(
            lookup_path=".")

    def get_pipeline_class(self):
        choices = self.scanner.get_pipelines()

        if not self.args.pipeline_name:
            raise CLIError('Missing pipeline name')

        name = self.args.pipeline_name[0]

        found = choices.get(name)
        if not found:
            raise CLIError('No such pipeline: \033[1;33m{0}\033[0m, options '
                           'are: \033[1;32m{1}\033[0m'.format(name, b", ".join(choices.keys())))

        return choices[name]

    @property
    def pipeline(self):
        if self._pipeline:
            return self._pipeline

        CreatePipeline = self.get_pipeline_class()
        self._pipeline = CreatePipeline(JSONRedisBackend)
        return self._pipeline

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


class RedisOutputProxy(object):
    backend_class = JSONRedisBackend

    def __init__(self, pipeline, directive, key):
        self.pipeline = pipeline
        self.key = key
        self.backend = self.backend_class()
        self.directive = getattr(self.backend, directive)
        self.status_key = ':'.join(
            [
                'manage',
                pipeline.name
            ]
        )

    def open(self):
        self.backend.set(self.status_key, 'open')
        return self

    def close(self):
        self.backend.set(self.status_key, 'close')

    def write(self, value):
        return self.directive(self.key, value)

    def is_open(self):
        value = self.backend.get(self.status_key) or 'close'
        value = value.lower()
        return value == 'open'


class RunPipeline(Command):
    name = 'run'

    def get_output(self, arguments):
        directive, key = arguments.output.split("@", 2)
        output = RedisOutputProxy(self.pipeline, directive, key)
        return output.open()

    def when_executed(self, arguments, remainder):
        self.pipeline.run_daemon()
        output = self.get_output(arguments)

        while output.is_open():
            result = self.pipeline.output.get(wait=False)
            if result:
                output.write(result)

        logger.warning("%s has stopped gracefully",
                       self.pipeline)


class StopPipeline(RunPipeline):
    name = 'stop'

    def when_executed(self, arguments, remainder):
        output = self.get_output(arguments)
        output.close()
        logger.warning("Stopping %s", self.pipeline)


class PushToPipeline(Command):
    name = 'push'

    def get_json_data(self, arguments):
        arguments.json_data.pop(0)
        arguments.json_data.pop(0)
        return [json.loads(x) for x in arguments.json_data]

    def when_executed(self, arguments, remainder):
        data = self.get_json_data(arguments)
        for item in data:
            self.pipeline.feed(item)
