#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os
import socket
from lineup import Step, Queue
from lineup.framework import Pipeline, Node
from .base import redis_test


class DummyStep(Step):
    def consume(self, instructions):
        self.produce({'cool': instructions})


@redis_test
def test_queue_adopt_producer_step(context):
    ("Queue#adopt_producer should take a step and know about its id and")

    # Given a dummy manager
    manager = Node()

    # Given a Queue with maximum size of 10
    queue = Queue('test-queue', maxsize=10)
    q1 = Queue('name1')
    q2 = Queue('name2')
    # And a running step (so it gains a thread id)
    step = DummyStep(q1, q2, manager)
    step.start()

    # When that queue adopts the step as a producer
    consumers, producers = queue.adopt_producer(step)

    # And the redis key should contain the step id
    hostname = socket.gethostname()
    pid = os.getpid()
    tid = step.ident
    value = '{hostname}|{pid}|{tid}|tests.functional.test_queue.DummyStep|lineup.framework.Node'.format(**locals())

    members = context.redis.smembers('lineup:test-queue:producers')
    members.should.contain(value)


@redis_test
def test_put_waits_to_consume(context):
    ("Queue#put should wait until someone consumes and returns the previous queue size and the current queue size")

    class CoolStep(Step):
        def consume(self, instructions):
            self.produce({'cool': instructions})

    class CoolFooBar(Pipeline):
        steps = [CoolStep]

    manager = CoolFooBar()
    previous, current = manager.feed({'foo': 'Bar'})
    manager.get_result().should.equal({'cool': {'foo': 'Bar'}})

    previous.should.equal(0)
    current.should.equal(1)
