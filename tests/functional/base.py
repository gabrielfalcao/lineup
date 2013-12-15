#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

from sure import scenario
from redis import StrictRedis


def prepare_redis(context):
    context.redis = StrictRedis()
    context.redis.flushall()


redis_test = scenario([prepare_redis], [])
