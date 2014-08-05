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
