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
import time
import os
import hashlib
import json
from itertools import izip
from milieu import Environment

from lineup.backends.base import BaseBackend, io_operation

from redis import StrictRedis

env = Environment()
os.environ.setdefault('LINEUP_REDIS_URI', 'redis://0@localhost:6379')

put_lua_script = '''
local first_item_key = KEYS[1]
local idle_items_key = KEYS[2]
local given_item_key = KEYS[3]

local uuid = ARGV[1]
local data = ARGV[2]

redis.call("HMSET", given_item_key, "uuid", uuid, "data", data, "status", "idle")
redis.call("LPUSH", idle_items_key, given_item_key)

if redis.call("GET", first_item_key) == false then
    redis.call("SET", first_item_key, given_item_key)
end
'''.strip()

pop_lua_script = '''
local first_item_key = KEYS[1]
local active_items_key = KEYS[2]
local idle_items_key = KEYS[3]

local ack_timeout = ARGV[1]
local timestamp = ARGV[2]

local next_key = redis.call("LINDEX", idle_items_key, -2)
local dictionary_key = redis.call("GET", first_item_key)
redis.call("LPUSH", active_items_key, dictionary_key)
redis.call("HMSET", dictionary_key, "status", "active", "ack_timeout", ack_timeout, "last_ack", timestamp)
redis.call("LREM", idle_items_key, 0, dictionary_key)
redis.call("SET", first_item_key, next_key)

return redis.call("HGETALL", dictionary_key)
'''.strip()


class JSONRedisBackend(BaseBackend):
    def initialize(self):
        conf = env.get_uri("LINEUP_REDIS_URI")

        self.redis = StrictRedis(
            db=conf.username or 0,
            host=conf.host,
            port=conf.port,
            # using `path` as password to support the URI like:
            # redis://dbindex@hostname:port/veryverylongpasswordhash
            password=conf.path,
        )
        self.lua_pop = self.redis.register_script(pop_lua_script)
        self.lua_put = self.redis.register_script(put_lua_script)

    @io_operation
    def serialize(self, value):
        return json.dumps(value)

    def deserialize(self, value):
        return value and json.loads(value) or None

    # read operations
    def get(self, key):
        value = self.redis.get(key)
        result = self.deserialize(value)
        return result

    def lpop(self, key):
        value = self.redis.lpop(key)
        result = self.deserialize(value)
        return result

    def llen(self, key):
        return self.redis.llen(key)

    def lrange(self, key, start, stop):
        return map(self.deserialize, self.redis.lrange(key, start, stop))

    def rpop(self, key):
        value = self.redis.rpop(key)
        result = self.deserialize(value)
        return result

    # Write operations
    def set(self, key, value):
        product = self.serialize(value)
        return self.redis.set(key, product)

    def rpush(self, key, value):
        product = self.serialize(value)
        return self.redis.rpush(key, product)

    def lpush(self, key, value):
        product = self.serialize(value)
        return self.redis.lpush(key, product)

    # Pipeline operations
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

    def put(self, queue_name, item):
        product = self.serialize(item)
        uuid = hashlib.sha1(product).hexdigest()
        key_prefix = ':'.join(['lineup', 'queue', queue_name])
        first_item_key = ':'.join([key_prefix, 'first'])
        idle_items_key = ':'.join([key_prefix, 'idle-items'])
        given_item_key = ':'.join([key_prefix, uuid])

        self.lua_put(
            keys=[
                first_item_key,
                idle_items_key,
                given_item_key,
            ],
            args=[
                uuid,
                product,
            ]
        )

        return given_item_key

    def pop(self, queue_name, owner, ack_timeout, wait=True):
        key_prefix = ':'.join(['lineup', 'queue', queue_name])
        idle_items_key = ':'.join([key_prefix, 'idle-items'])
        active_items_key = ':'.join([key_prefix, 'active-items'])
        first_item_key = ':'.join([key_prefix, 'first'])

        redis_data = self.lua_pop(
            keys=[
                first_item_key,
                active_items_key,
                idle_items_key,
            ],
            args=[
                ack_timeout,
                time.time()
            ]
        )
        i = iter(redis_data)
        data = dict(izip(i, i))
        return data
