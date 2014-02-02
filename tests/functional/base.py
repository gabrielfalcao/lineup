#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from sure import scenario
from lineup import JSONRedisBackend

from threading import RLock


test_lock = RLock()


def prepare_redis(context):
    context.backend = JSONRedisBackend()
    context.redis = context.backend.redis
    context.redis.flushall()
    test_lock.acquire()


def unlock(context):
    test_lock.release()


redis_test = scenario([prepare_redis], [unlock])
