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

from __future__ import unicode_literals, absolute_import
import json

from lineup.backends.base import BaseBackend, io_operation

from redis import StrictRedis


class JSONRedisBackend(BaseBackend):
    def initialize(self):
        self.redis = StrictRedis()

    def serialize(self, value):
        return json.dumps(value)

    def deserialize(self, value):
        return value and json.loads(value) or None

    # read operations
    @io_operation
    def get(self, key):
        value = self.redis.get(key)
        result = self.deserialize(value)
        return result

    @io_operation
    def lpop(self, key):
        value = self.redis.lpop(key)
        result = self.deserialize(value)
        return result

    @io_operation
    def llen(self, key):
        return self.redis.llen(key)

    @io_operation
    def lrange(self, key, start, stop):
        return map(self.deserialize, self.redis.lrange(key, start, stop))

    @io_operation
    def rpop(self, key):
        value = self.redis.rpop(key)
        result = self.deserialize(value)
        return result

    # Write operations
    @io_operation
    def set(self, key, value):
        product = self.serialize(value)
        return self.redis.set(key, product)

    @io_operation
    def rpush(self, key, value):
        product = self.serialize(value)
        return self.redis.rpush(key, product)

    @io_operation
    def lpush(self, key, value):
        product = self.serialize(value)
        return self.redis.lpush(key, product)

    # Pipeline operations
    @io_operation
    def report_steps(self, name, consumers, producers):
        pipeline = self.redis.pipeline()
        producers_key = ':'.join([name, 'producers'])
        consumers_key = ':'.join([name, 'consumers'])

        for consumer in consumers:
            pipeline.sadd(consumers_key, consumer)

        for producer in producers:
            pipeline.sadd(producers_key, producer)

        pipeline.smembers(consumers_key)
        pipeline.smembers(producers_key)

        result = pipeline.execute()
        all_consumers = result[-2]
        all_producers = result[-1]

        return all_consumers, all_producers
