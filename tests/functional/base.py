#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

from sure import scenario
from redis import StrictRedis
from threading import RLock


test_lock = RLock()

def prepare_redis(context):
    context.redis = StrictRedis()
    context.redis.flushall()
    test_lock.acquire()

def unlock(context):
    test_lock.release()


redis_test = scenario([prepare_redis], [unlock])
