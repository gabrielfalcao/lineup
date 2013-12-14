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
from functools import wraps
from lineup.backends.redis import JSONRedisBackend
from threading import RLock

DEFAULT_BACKEND = JSONRedisBackend


def io_operation(method):
    """decorator for methods of a queue, allowing the redis
    operations to run in a lock per thread.
    """
    @wraps(method)
    def decorator(queue, *args, **kwargs):
        queue.lock.acquire()
        result = method(queue, *args, **kwargs)
        queue.lock.release()
        return result

    return decorator


class Queue(object):
    prefix = 'lineup'
    def __init__(self, name, maxsize=None, backend=DEFAULT_BACKEND, timeout=-1):
        self.name = ':'.join([self.prefix, name])
        self.maxsize = maxsize
        self.timeout = timeout
        self.backend = backend()
        self.producers = set()
        self.consumers = set()
        self.lock = RLock()

    def __repr__(self):
        return b'<lineup.Queue({0}, backend={1})>'.format(
            self.name, self.backend)

    def adopt_producer(self, producer):
        self.producers.add(producer.id)
        return self.report()

    def report(self):
        return self.backend.report_steps(self.name, self.consumers, self.producers)

    def adopt_consumer(self, consumer):
        self.consumers.add(consumer.id)
        return self.report()

    @io_operation
    def put(self, payload):
        previous = self.backend.llen(self.name)
        self.backend.rpush(self.name, payload)
        current = self.backend.llen(self.name)
        return previous, current

    @io_operation
    def get(self):
        started = time.time()
        done = None
        def wait():
            delta = time.time() - started
            if delta > self.timeout:
                return intern(b"stop")
            if done is not None:
                return intern(b"stop")

            return delta

        loop = iter(wait, intern(b"stop"))

        for delta in loop:
            done = self.backend.lpop(self.name)
            if done:
                return done
