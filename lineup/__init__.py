# #!/usr/bin/env python
# -*- coding: utf-8 -*-
# <lineup - python distributed pipeline framework>
# Copyright (C) <2013>  Gabriel Falcão <gabriel@nacaolivre.org>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from __future__ import unicode_literals

import sys
import time
import json
import logging
import traceback

from pprint import pformat
from redis import StrictRedis
from threading import RLock, Thread


class KeyMaker(object):
    def __init__(self, step):
        self.step = step

        for name in ['logging', 'alive', 'error']:
            setattr(self, name, self.make_key(name))

    def make_key(self, suffix, prefix=None):
        prefix = prefix or getattr(self, 'prefix', 'lineup')
        return ":".join([prefix, self.step.name, suffix])


class Step(Thread):
    def __init__(self, consume_queue, produce_queue, parent):
        super(Worker, self).__init__()
        self.consume_queue = consume_queue
        self.produce_queue = produce_queue
        self.logger = logging.getLogger(self.key.logging)
        self.daemon = False
        self.parent = parent
        if hasattr(self.__class__, 'name'):
            previous_name = name
        else:
            previous_name = self.__class__.__name__
        self.name = ':'.join([parent.name, self.__class__.__module__, previous_name])

    def __str__(self):
        return '<{0}>'.format(self.name)

    @property
    def alive(self):
        return self.redis.get_json(self.key.alive)

    def log(self, message, *args, **kw):
        self.logger.info(message, *args, **kw)
        return self.redis.rpush_json(self.key.logging, {
            'message': message % args % kwargs,
            'when': time.time()
        })

    def fail(self, exception):
        error = traceback.format_exc(exception)
        self.logger.exception('The worker %s failed while being processed at the pipeline "%s"', self.name, self.parent.name)
        self.parent.enqueue_error(source_class=self.__class__, instructions=instructions, exception=exception)
        return self.redis.rpush_json(self.key.logging, {
            'message': message % args % kwargs,
            'when': time.time()
        })

    def consume(self, instructions):
        raise NotImplemented("You must implement the consume method by yourself")

    def produce(self, payload):
        return self.produce_queue.put(payload)

    def before_consume(self):
        self.log("%s is about to consume its queue", self, with_redis=False)

    def after_consume(self, instructions):
        self.log("%s is done", self)

    def do_rollback(self, instructions):
        try:
            self.rollback(instructions)
        except Exception as e:
            self.fail(e, instructions)

    def run(self):
        self.redis.set_json(self.key.alive, True)

        while self.alive:
            self.before_consume()
            instructions = self.consume_queue.get()
            if not instructions:
                sys.exit(1)
            try:
                self.consume(instructions)
            except Exception as e:
                error = traceback.format_exc(e)
                self.log(error)
                instructions.update({
                    'success': False,
                    'error': error
                })
                self.produce(instructions)
                self.do_rollback(instructions)
                continue

            self.after_consume(instructions)
