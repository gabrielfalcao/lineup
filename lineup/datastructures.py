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


class Queue(object):
    prefix = 'lineup'

    def __init__(self, name, backend_class, maxsize=None, timeout=-1):
        self.name = ':'.join([self.prefix, name])
        self.maxsize = maxsize
        self.timeout = timeout
        self.backend = backend_class()
        self.producers = set()
        self.consumers = set()

    def __repr__(self):
        return b'<lineup.Queue({0}, backend={1})>'.format(
            self.name, self.backend)

    def adopt_producer(self, producer):
        self.producers.add(producer.id)
        return self.report()

    def report(self):
        return self.backend.report_steps(
            self.name, self.consumers, self.producers)

    def adopt_consumer(self, consumer):
        self.consumers.add(consumer.id)
        return self.report()

    def put(self, payload):
        self.backend.rpush(self.name, payload)

    def get(self, wait=False):
        done = self.backend.lpop(self.name)
        while wait and done is None:
            done = self.backend.lpop(self.name)

        return done

    def get_size(self):
        return self.backend.llen(self.name)
