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
import time
from lineup.backends.redis import JSONRedisBackend

DEFAULT_BACKEND = JSONRedisBackend


class Queue(object):
    prefix = 'lineup'
    def __init__(self, name, maxsize=None, backend=DEFAULT_BACKEND):
        self.name = ':'.join([self.prefix, name])
        self.maxsize = maxsize
        self.backend = backend()
        self.producers = set()
        self.consumers = set()

    def adopt_producer(self, producer):
        self.producers.add(producer.id)
        return self.report()

    def report(self):
        return self.backend.report_steps(self.name, self.consumers, self.producers)

    def adopt_consumer(self, consumer):
        self.consumers.add(consumer.id)
        return self.report()

    def get_active_key(self):
        return '@'.join([self.name, 'active'])

    def active(self):
        key = self.get_active_key()
        time.sleep(0)
        return self.backend.get(key)

    def activate(self):
        key = self.get_active_key()
        return self.backend.set(key, True)

    def deactivate(self):
        key = self.get_active_key()
        return self.backend.set(key, False)

    def put(self, payload, wait=lambda name, total, current: total < current):
        self.activate()
        total = self.backend.llen(self.name)
        self.backend.rpush(self.name, payload)

        current = self.backend.llen(self.name)
        while self.active() and total <= current:
            current = self.backend.llen(self.name)
            if not wait(self.name, total, current):
                return

    def get(self):
        result = None
        while self.active() and not result:
            result = self.backend.lpop(self.name)

        return result
