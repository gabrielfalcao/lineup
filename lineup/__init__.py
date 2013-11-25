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
from lineup.datastructures import Queue
from lineup.backends.redis import JSONRedisBackend

class KeyMaker(object):
    def __init__(self, step):
        self.step = step

        for name in ['logging', 'alive', 'error']:
            setattr(self, name, self.make_key(name))

    def make_key(self, suffix, prefix=None):
        prefix = prefix or getattr(self, 'prefix', 'lineup')
        return ":".join([prefix, self.step.name, suffix])


class Step(Thread):
    def __init__(self, consume_queue, produce_queue, parent, backend=JSONRedisBackend):
        super(Step, self).__init__()
        self.key = KeyMaker(self)
        self.consume_queue = consume_queue
        self.produce_queue = produce_queue
        self.logger = logging.getLogger(self.key.logging)
        self.daemon = False
        self.parent = parent
        self.backend = backend()
        self.name = getattr(self.__class__, 'label', self.taxonomy)
        consume_queue.adopt_consumer(self)
        produce_queue.adopt_producer(self)

    def get_name(self):
        return getattr(self, 'name', None) or self.taxonomy

    def __str__(self):
        return '<{0}>'.format(self.ancestry)

    @property
    def taxonomy(self):
        class_name = self.__class__.__name__
        module_name = self.__class__.__module__
        return '.'.join([module_name, class_name])

    @property
    def ancestry(self):
        return '|'.join([self.get_name(), self.parent.get_name()])

    @property
    def id(self):
        return '|'.join([self.parent.id, str(self.ident), self.ancestry])

    @property
    def alive(self):
        return self.backend.get(self.key.alive)

    def log(self, message, *args, **kw):
        self.logger.info(message, *args, **kw)
        return self.backend.rpush(self.key.logging, {
            'message': message % args % kw,
            'when': time.time()
        })

    def fail(self, exception):
        error = traceback.format_exc(exception)
        message = 'The worker %s failed while being processed at the pipeline "%s"'
        args = (self.name, self.parent.name)
        self.logger.exception(message, *args)
        self.parent.enqueue_error(source_class=self.__class__, instructions=instructions, exception=exception)
        return self.backend.rpush(self.key.logging, {
            'message': message % args,
            'when': time.time()
        })

    def consume(self, instructions):
        raise NotImplemented("You must implement the consume method by yourself")

    def produce(self, payload):
        return self.produce_queue.put(payload)

    def before_consume(self):
        self.log("%s is about to consume its queue", self)

    def after_consume(self, instructions):
        self.log("%s is done", self)

    def do_rollback(self, instructions):
        try:
            self.rollback(instructions)
        except Exception as e:
            self.fail(e, instructions)

    def run(self):
        self.backend.set(self.key.alive, True)

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
