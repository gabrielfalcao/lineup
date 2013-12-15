#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from mock import MagicMock, patch, call
from lineup.backends.redis import JSONRedisBackend

operation_test = patch('lineup.backends.redis.io_operation', lambda x: x)


class IsolatedTestBackend(JSONRedisBackend):
    def initialize(self):
        self.redis = MagicMock(name='IsolatedTestBackend.redis')
        self.json = MagicMock(name='IsolatedTestBackend.json')

    def serialize(self, value):
        self.json.serialize(value)
        return {'serialized': value}

    def deserialize(self, value):
        self.json.deserialize(value)
        return {'deserialized': value}


@patch('lineup.backends.redis.StrictRedis')
def test_redis_instance(StrictRedis):
    ("JSONRedisBackend should create a redis instance "
     "upon initialization")

    # Given I create an instance of JSONRedisBackend
    instance = JSONRedisBackend()

    # Then it should have a redis instance
    instance.redis.should.equal(StrictRedis.return_value)


def test_get_name():
    ("JSONRedisBackend#get_name should return class name by default")

    class MyBackend(JSONRedisBackend):
        pass

    # Given an instance of a backend
    backend = MyBackend()

    # When I call get_name()
    result = backend.get_name()

    # Then it should be the class name
    result.should.equal("JSONRedisBackend()")


@patch('lineup.backends.redis.json')
def test_serialize(json):
    ("JSONRedisBackend#serialize should return the content json serialized")

    # Given an instance of a backend
    backend = JSONRedisBackend()

    # When I call serialize()
    result = backend.serialize({"foo": "bar"})

    # Then it should have called json.dumps
    result.should.equal(json.dumps.return_value)

    # And json.dumps should have been called appropriately
    json.dumps.assert_called_once_with({"foo": "bar"})



@patch('lineup.backends.redis.json')
def test_deserialize(json):
    ("JSONRedisBackend#deserialize should return the content json deserialized")

    # Given an instance of a backend
    backend = JSONRedisBackend()

    # When I call deserialize()
    result = backend.deserialize("WHAAAAT")

    # Then it should have called json.loads
    result.should.equal(json.loads.return_value)

    # And json.loads should have been called appropriately
    json.loads.assert_called_once_with("WHAAAAT")


@operation_test
def test_get():
    ("JSONRedisBackend#get should return the content json deserialized")

    # Given an instance of a backend that mocks redis.get
    backend = IsolatedTestBackend()
    backend.redis.get.return_value = "COMING FROM redis.get"

    # When I call get()
    result = backend.get("some-key")

    # Then it should return the value deserialized
    result.should.equal({'deserialized': 'COMING FROM redis.get'})

    # And redis.get should have been called with the key
    backend.redis.get.assert_called_once_with("some-key")


@operation_test
def test_rpop():
    ("JSONRedisBackend#rpop should return the content json deserialized")

    # Given an instance of a backend that mocks redis.rpop
    backend = IsolatedTestBackend()
    backend.redis.rpop.return_value = "COMING FROM redis.rpop"

    # When I call get()
    result = backend.rpop("some-key")

    # Then it should return the value deserialized
    result.should.equal({'deserialized': 'COMING FROM redis.rpop'})

    # And redis.rpop should have been called with the key
    backend.redis.rpop.assert_called_once_with("some-key")


@operation_test
def test_lpop():
    ("JSONRedisBackend#lpop should return the content json deserialized")

    # Given an instance of a backend that mocks redis.lpop
    backend = IsolatedTestBackend()
    backend.redis.lpop.return_value = "COMING FROM redis.lpop"

    # When I call get()
    result = backend.lpop("some-key")

    # Then it should return the value deserialized
    result.should.equal({'deserialized': 'COMING FROM redis.lpop'})

    # And redis.lpop should have been called with the key
    backend.redis.lpop.assert_called_once_with("some-key")


@operation_test
def test_llen():
    ("JSONRedisBackend#llen should return the content json deserialized")

    # Given an instance of a backend that mocks redis.llen
    backend = IsolatedTestBackend()
    backend.redis.llen.return_value = 1000

    # When I call get()
    result = backend.llen("some-key")

    # Then it should return the value straight from redis
    result.should.equal(1000)

    # And redis.llen should have been called with the key
    backend.redis.llen.assert_called_once_with("some-key")


@operation_test
def test_lrange():
    ("JSONRedisBackend#lrange should return the content json deserialized")

    # Given an instance of a backend that mocks redis.lrange
    backend = IsolatedTestBackend()
    backend.redis.lrange.return_value = 1000

    # When I call get()
    result = backend.lrange("some-key", 0, -1)

    # Then it should return the value straight from redis
    result.should.equal(1000)

    # And redis.lrange should have been called with the key
    backend.redis.lrange.assert_called_once_with("some-key", 0, -1)


@operation_test
def test_rpush():
    ("JSONRedisBackend#rpush should send the serialized data to redis")

    # Given an instance of a backend that mocks redis.get
    backend = IsolatedTestBackend()
    backend.redis.rpush.return_value = "COMING FROM redis.set"

    # When I call get()
    result = backend.rpush("some-key", {"SOME": "VALUE"})

    # Then it should return the result from redis.rpush
    result.should.equal(backend.redis.rpush.return_value)

    # And redis.rpush should have been called appropriately
    backend.redis.rpush.assert_called_once_with(
        "some-key",
        {'serialized':  {
            'SOME': 'VALUE'
        }
    })


@operation_test
def test_lpush():
    ("JSONRedisBackend#lpush should send the serialized data to redis")

    # Given an instance of a backend that mocks redis.get
    backend = IsolatedTestBackend()
    backend.redis.lpush.return_value = "COMING FROM redis.set"

    # When I call get()
    result = backend.lpush("some-key", {"SOME": "VALUE"})

    # Then it should return the result from redis.lpush
    result.should.equal(backend.redis.lpush.return_value)

    # And redis.lpush should have been called appropriately
    backend.redis.lpush.assert_called_once_with(
        "some-key",
        {'serialized':  {
            'SOME': 'VALUE'
        }
    })


@operation_test
def test_set():
    ("JSONRedisBackend#set should send the serialized data to redis")

    # Given an instance of a backend that mocks redis.get
    backend = IsolatedTestBackend()
    backend.redis.set.return_value = "COMING FROM redis.set"

    # When I call get()
    result = backend.set("some-key", {"SOME": "VALUE"})

    # Then it should return the result from redis.set
    result.should.equal(backend.redis.set.return_value)

    # And redis.set should have been called appropriately
    backend.redis.set.assert_called_once_with(
        "some-key",
        {'serialized':  {
            'SOME': 'VALUE'
        }
    })


@operation_test
def test_report_steps():
    ("JSONRedisBackend#report_steps should keep track of producers and consumers ")

    # Given an instance of a backend that mocks redis.get
    backend = IsolatedTestBackend()

    pipeline = backend.redis.pipeline.return_value
    pipeline.execute.return_value = (
        "consumers",
        "producers",
    )

    # When I call get()
    all_consumers, all_producers = backend.report_steps(
        "some-name", ["c1", "c2"], ["p1", "p2"])

    all_consumers.should.equal("consumers")
    all_producers.should.equal("producers")

    pipeline.sadd.assert_has_calls([
        call(u'some-name:consumers', u'c1'),
        call(u'some-name:consumers', u'c2'),
        call(u'some-name:producers', u'p1'),
        call(u'some-name:producers', u'p2')
    ])
