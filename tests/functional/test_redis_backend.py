#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals


from lineup.backends.redis import JSONRedisBackend
from .base import redis_test


@redis_test
def test_backend_get(context):
    ("JSONRedisBackend#get should deserialize value")

    context.redis.set("FOO", '{"Bar": "BAZ"}')

    backend = JSONRedisBackend()
    result = backend.get("FOO")

    result.should.equal({'Bar': 'BAZ'})


@redis_test
def test_backend_lrange(context):
    ("JSONRedisBackend#lrange should deserialize value")

    context.redis.lpush("FOO", '{"side": "left"}')
    context.redis.rpush("FOO", '{"side": "right"}')

    backend = JSONRedisBackend()
    result = backend.lrange("FOO", 0, -1)
    result.should.equal([
        {'side': 'left'},
        {'side': 'right'},
    ])


@redis_test
def test_backend_rpop(context):
    ("JSONRedisBackend#rpop should deserialize value")

    context.redis.lpush("FOO", '{"side": "left"}')
    context.redis.rpush("FOO", '{"side": "right"}')

    backend = JSONRedisBackend()
    result = backend.rpop("FOO")
    result.should.equal(
        {'side': 'right'})


@redis_test
def test_backend_set(context):
    ("JSONRedisBackend#set should serialize value")

    backend = JSONRedisBackend()
    backend.set("FOO", {'a dict,': 'how convenient !'})

    context.redis.get("FOO").should.equal(
        '{"a dict,": "how convenient !"}')


@redis_test
def test_backend_rpush(context):
    ("JSONRedisBackend#rpush should serialize value")

    backend = JSONRedisBackend()
    backend.rpush("FOO", {'a dict,': 'how convenient !'})

    context.redis.lrange("FOO", 0, -1).should.equal(
        ['{"a dict,": "how convenient !"}'])


@redis_test
def test_backend_lpush(context):
    ("JSONRedisBackend#lpush should serialize value")

    backend = JSONRedisBackend()
    backend.lpush("FOO", {'a dict,': 'how convenient !'})

    context.redis.lrange("FOO", 0, -1).should.equal(
        ['{"a dict,": "how convenient !"}'])
