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

redis.call("HMSET", given_item_key, "uuid", uuid, "data", data, "status", "idle", "key", given_item_key)
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

if dictionary_key == false then
    return ""
else
    return redis.call("HGETALL", dictionary_key)
end
'''.strip()

update_ack_lua_script = '''
local item_key = KEYS[1]
local timestamp = ARGV[1]

redis.call("HMSET", item_key, "last_ack", timestamp)
return redis.call("HGETALL", item_key)
'''.strip()

queue_heartbeat_lua_script = '''
local idle_items_key = KEYS[1]
local active_items_key = KEYS[2]
local first_item_key = KEYS[3]

local timestamp = tonumber(ARGV[1])

local active_items = redis.call("LRANGE", active_items_key, 0, -1)
for k, item_key in pairs(active_items) do
    local last_ack = tonumber(redis.call("HGET", item_key, "last_ack"))
    local ack_timeout = tonumber(redis.call("HGET", item_key, "ack_timeout"))
    if (timestamp - last_ack) > ack_timeout then
        redis.call("HSET", item_key, "status", "idle")
        redis.call("LREM", active_items_key, 0, item_key)
        redis.call("LPUSH", idle_items_key, item_key)
    end
end
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
        self.lua_update_ack = self.redis.register_script(update_ack_lua_script)
        self.lua_heartbeat = self.redis.register_script(queue_heartbeat_lua_script)

    @io_operation
    def serialize(self, value):
        return json.dumps(value)

    def deserialize(self, value):
        return value and json.loads(value) or None

    def put(self, queue_name, item):
        self.heartbeat(queue_name)
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

    def pop(self, queue_name, owner, ack_timeout):
        self.heartbeat(queue_name)
        key_prefix = ':'.join(['lineup', 'queue', queue_name])
        idle_items_key = ':'.join([key_prefix, 'idle-items'])
        active_items_key = ':'.join([key_prefix, 'active-items'])
        first_item_key = ':'.join([key_prefix, 'first'])

        redis_data = None
        while not redis_data:
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

        data = self.list_to_dictionary(redis_data)
        return data

    def ack_activity(self, item_key):
        redis_data = self.lua_update_ack(keys=[item_key], args=[time.time()])
        data = self.list_to_dictionary(redis_data)
        return data

    def heartbeat(self, queue_name):
        key_prefix = ':'.join(['lineup', 'queue', queue_name])
        idle_items_key = ':'.join([key_prefix, 'idle-items'])
        active_items_key = ':'.join([key_prefix, 'active-items'])
        first_item_key = ':'.join([key_prefix, 'first'])

        self.lua_heartbeat(keys=[
            idle_items_key,
            active_items_key,
            first_item_key,
        ], args=[time.time()])

    def list_to_dictionary(self, redis_data):
        i = iter(redis_data)
        data = dict(izip(i, i))
        return data

    def get_stats(self, queue_name):
        key_prefix = ':'.join(['lineup', 'queue', queue_name])
        idle_items_key = ':'.join([key_prefix, 'idle-items'])
        active_items_key = ':'.join([key_prefix, 'active-items'])

        idle = self.redis.llen(idle_items_key)
        active = self.redis.llen(active_items_key)
        return {
            'idle': idle,
            'active': active,
        }
