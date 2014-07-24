#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
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


@redis_test
def test_backend_put(context):
    ("JSONRedisBackend#put should the json data in a hash key, "
     "and it's metadata in sibling keys")

    # Given a backend instance
    backend = JSONRedisBackend()

    # When I put some data
    item_key = backend.put("nice-name", {'a dict,': 'how convenient !'})
    item_id = item_key.split(':')[-1]

    # Then there is a key holding the json with the value
    raw_data = context.redis.hget(item_key, 'data')
    data = json.loads(raw_data)
    data.should.equal({'a dict,': 'how convenient !'})

    # And its id should match the returned one
    context.redis.hget(item_key, 'uuid').should.equal(item_id)

    # And its status is "idle"
    context.redis.hget(item_key, 'status').should.equal('idle')

    # And its owner should be "None"
    context.redis.hget(item_key, 'owner').should.be.none

    # And it should be the in the "idle" list
    context.redis.lrange(
        'lineup:queue:nice-name:idle-items', 0, -1).should.equal([item_key])

    # And it should be the first item in the queue
    context.redis.get('lineup:queue:nice-name:first').should.equal(item_key)

    # And therefore the active list should be empty
    context.redis.lrange(
        'lineup:queue:nice-name:active-items', 0, -1).should.be.empty


@redis_test
def test_backend_pop(context):
    ("JSONRedisBackend#pop should borrow the item by setting the consumption "
     "timeout, and identifying the owner name")

    # Given a backend instance
    backend = JSONRedisBackend()

    # And that there are 2 items in the queue
    item1_key = backend.put("borrowees", {'sweet': 'item 1'})
    item2_key = backend.put("borrowees", {'sour': 'item 2'})

    # When I get an item
    item = backend.pop('borrowees', owner='some-name', ack_timeout=10)

    # Then the item should be the first one of the queue, because it's FIFO
    item.should.have.key('data').being.equal('{"sweet": "item 1"}')
    item.should.have.key('status').being.equal('active')
    item.should.have.key('uuid').being.match(r'^\w+$')
    item.should.have.key('ack_timeout').being.equal('10')
    item.should.have.key('last_ack').being.match(r'^\d+[.]\d+$')

    # And it should be in the "active" list
    context.redis.lrange(
        'lineup:queue:borrowees:active-items', 0, -1).should.equal([item1_key])

    # And its status is "active"
    context.redis.hget(item1_key, 'status').should.equal('active')

    # And the idle list should only contain the other item
    context.redis.lrange(
        'lineup:queue:borrowees:idle-items', 0, -1).should.equal([item2_key])

    # And the 2nd item should be the first item in the queue
    context.redis.get('lineup:queue:borrowees:first').should.equal(item2_key)


@redis_test
def test_backend_ack(context):
    ("JSONRedisBackend#ack_activity() should update the last_ack of the item")

    # Given a backend instance
    backend = JSONRedisBackend()

    # And that there is an active item
    item_key = backend.put("borrowees", {'sweet': 'item 1'})
    old_item = backend.pop('borrowees', owner='some-name', ack_timeout=10)
    old_ack = float(old_item.pop('last_ack'))

    # When I update its ack
    new_item = backend.ack_activity(item_key)
    last_ack = float(new_item.pop('last_ack'))

    # Then the ack should have been updated
    last_ack.should.be.greater_than(old_ack)

    # And all the other items should be the same
    old_item.should.equal(new_item)


@redis_test
def test_backend_heartbeat(context):
    ("JSONRedisBackend#heartbeat() should check for timed out items and put them back into the idle queue")

    # Given a backend instance
    backend = JSONRedisBackend()

    # And that there is an active item with last_ack 0
    item_key = backend.put("borrowees", {'sweet': 'item 1'})
    backend.pop('borrowees', owner='some-name', ack_timeout=1)
    context.redis.hset(item_key, 'last_ack', 0)

    # And that the item is in the active list
    context.redis.lrange(
        'lineup:queue:borrowees:active-items', 0, -1).should.equal([item_key])

    # And that the idle list is empty
    context.redis.lrange(
        'lineup:queue:borrowees:idle-items', 0, -1).should.be.empty

    # When I call heartbeat()
    backend.heartbeat('borrowees')

    # Then the list of active items should be empty
    # And it should be in the "active" list
    context.redis.lrange(
        'lineup:queue:borrowees:active-items', 0, -1).should.be.empty

    context.redis.lrange(
        'lineup:queue:borrowees:idle-items', 0, -1).should.equal([item_key])
