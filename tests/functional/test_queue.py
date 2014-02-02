#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os
import socket
from lineup import Step, Queue
from lineup.framework import Pipeline, Node
from lineup.backends.redis import JSONRedisBackend
from .base import redis_test


class DummyStep(Step):
    def consume(self, instructions):
        self.produce({'yay': instructions})


@redis_test
def test_queue_adopt_producer_step(context):
    ("Queue#adopt_producer should take a step and know about its id and")

    # Given a dummy manager
    manager = Node(JSONRedisBackend)

    # Given a Queue with maximum size of 10
    queue = Queue('test-queue', backend_class=JSONRedisBackend, maxsize=10)
    q1 = Queue('name1', backend_class=JSONRedisBackend)
    q2 = Queue('name2', backend_class=JSONRedisBackend)

    # And a running step (so it gains a thread id)
    step = DummyStep(q1, q2, manager)

    # When that queue adopts the step as a producer
    consumers, producers = queue.adopt_producer(step)

    # And the redis key should contain the step id
    hostname = socket.gethostname()
    pid = os.getpid()
    tid = step.ident
    value = 'lineup.framework.Node|{hostname}|{pid}|{tid}|tests.functional.test_queue.DummyStep|lineup.framework.Node'.format(**locals())

    members = context.redis.smembers('lineup:test-queue:producers')
    members.should.contain(value)
    repr(q1).should.equal(
        '<lineup.Queue(lineup:name1, backend=<JSONRedisBackend>)>')


@redis_test
def test_pipeline(context):
    ("Pipeline should process steps")

    class OkStep(Step):
        def consume(self, instructions):
            self.produce({'ok': instructions})

    class SaveStep(Step):

        def consume(self, instructions):
            context.redis.set("WORKED", "YES")
            self.produce({"SAVED": instructions})

    class OkFooBar(Pipeline):
        name = 'okfoobar'
        steps = [OkStep, SaveStep]

    manager = OkFooBar(JSONRedisBackend)
    manager.run_daemon()
    manager.feed({'foo': 'Bar'})

    result = manager.get_result()
    manager.stop()

    result.should.equal({"SAVED": {'ok': {'foo': 'Bar'}}})
    context.redis.get("WORKED").should.equal("YES")


@redis_test
def test_pipeline_exception(context):
    ("Pipeline should handle exceptions")

    class CoolStep(Step):
        def consume(self, instructions):
            self.produce({'cool': instructions})

    class BoomStep(Step):
        def rollback(self, instructions):
            self.produce(instructions)

        def consume(self, instructions):
            raise ValueError("BOOM")

    class CoolFooBar(Pipeline):
        name = 'coolfoobar'
        steps = [CoolStep, BoomStep]

    manager = CoolFooBar(JSONRedisBackend)
    manager.run_daemon()
    manager.run_daemon()
    manager.feed({'foo': 'Bar'})

    result = manager.get_result()
    manager.stop()

    result.should.be.a(dict)
    result.should.have.key('cool').being.equal({'foo': 'Bar'})
    result.should.have.key('__lineup__error__').being.have.key("traceback")
    result['__lineup__error__']['traceback'].should.contain("ValueError: BOOM")
    result['__lineup__error__']['consume_queue_size'].should.equal(0)
    result['__lineup__error__']['produce_queue_size'].should.equal(0)


@redis_test
def test_pipeline_exception_bad_rollback(context):
    ("Pipeline should handle a bad rollback")

    class AwesomeStep(Step):
        def consume(self, instructions):
            self.produce({'awesome': instructions})

    class BoomStep(Step):
        def consume(self, instructions):
            raise ValueError("BOOM")

        def rollback(self, instructions):
            raise ValueError("ROLLBACK HELL")

    class AwesomeFooBar(Pipeline):
        name = 'awesomefoobar3'
        steps = [AwesomeStep, BoomStep]

    manager = AwesomeFooBar(JSONRedisBackend)
    manager.run_daemon()
    manager.feed({'foo': 'Bar'})

    result = manager.get_result()
    manager.stop()

    result.should.be.a(dict)
    result.should.have.key('awesome').being.equal({'foo': 'Bar'})
    result.should.have.key('__lineup__error__').being.have.key("traceback")
    result['__lineup__error__']['traceback'].should.contain("ValueError: BOOM")
    result['__lineup__error__']['consume_queue_size'].should.equal(0)
    result['__lineup__error__']['produce_queue_size'].should.equal(0)


@redis_test
def test_pipeline_exception_key_error(context):
    ("Pipeline should handle key error")

    class AwesomeStep(Step):
        def consume(self, instructions):
            instructions['awesome'] = True
            self.produce(instructions)

    class StatusBoom(Step):
        def consume(self, instructions):
            self.produce(instructions['nah'])

        def rollback(self, instructions):
            self.produce({"status": "it's all good", "backup": instructions})

    class AwesomeFooBar(Pipeline):
        name = 'awesomefoobar4'
        steps = [AwesomeStep, StatusBoom]

    manager = AwesomeFooBar(JSONRedisBackend)
    manager.run_daemon()
    manager.feed({'foo': 'Bar'})

    result = manager.get_result()
    manager.stop()

    result.should.equal({
        u'backup': {
            u'awesome': True, u'foo': u'Bar'}, u'status': u"it's all good"})


@redis_test
def test_pipeline_default_rollback(context):
    ("Step has a default rollback")

    class BoomStep(Step):
        def consume(self, instructions):
            if instructions['failpurposedly']:
                raise ValueError("BOOM")

            self.produce({'ok': True})

    class CoolFooBar(Pipeline):
        name = 'coolfoobar2'
        steps = [BoomStep]

    manager = CoolFooBar(JSONRedisBackend)
    manager.run_daemon()
    manager.run_daemon()
    manager.feed({'failpurposedly': True})
    manager.feed({'failpurposedly': False})

    result = manager.get_result()
    manager.stop()

    result.should.equal({
        'ok': True,
    })
