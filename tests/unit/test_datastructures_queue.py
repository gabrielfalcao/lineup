#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from mock import Mock, call
from lineup.datastructures import Queue


def test_backend():
    ("Queue should have a backend")

    # Given a fake backend
    Backend = Mock(name='Backend')

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # Then it should have a backend
    queue.should.have.property("backend").being.equal(
        Backend.return_value)


def test_repr():
    ("repr(Queue) should show name and backend")

    # Given a classy stringifiable fake backend :)
    class StringBackend(object):
        def __repr__(self):
            return "StringBackend, yay!"

    # When I create a queue
    queue = Queue("ChuckNorris", StringBackend)

    # And get its representation
    result = repr(queue)

    # Then it should have the name and backend
    result.should.equal('<lineup.Queue(lineup:ChuckNorris, backend=StringBackend, yay!)>')


def test_adopt_producer():
    ("Queue#adopt_producer should add to producers set and report")

    # Given a queue that mocks the report method
    class IsolatedQueue(Queue):
        report = Mock(name='IsolatedQueue.report')
        def __init__(self):
            self.producers = set()


    # And a fake producer that has an id
    producer = Mock(name='test_adopt_producer.producer')
    producer.id = 'producer.id'

    # When I create a queue
    q = IsolatedQueue()

    # And call `adopt_producer`
    result = q.adopt_producer(producer)

    # Then it should have returned the result from report
    result.should.equal(IsolatedQueue.report.return_value)

    # And the id should have been added to the set of producers
    q.producers.should.equal(set(['producer.id']))

    # And report should have been called without args
    IsolatedQueue.report.assert_called_once_with()


def test_adopt_consumer():
    ("Queue#adopt_consumer should add to consumers set and report")

    # Given a queue that mocks the report method
    class IsolatedQueue(Queue):
        report = Mock(name='IsolatedQueue.report')
        def __init__(self):
            self.consumers = set()


    # And a fake consumer that has an id
    consumer = Mock(name='test_adopt_consumer.consumer')
    consumer.id = 'consumer.id'

    # When I create a queue
    q = IsolatedQueue()

    # And call `adopt_consumer`
    result = q.adopt_consumer(consumer)

    # Then it should have returned the result from report
    result.should.equal(IsolatedQueue.report.return_value)

    # And the id should have been added to the set of consumers
    q.consumers.should.equal(set(['consumer.id']))

    # And report should have been called without args
    IsolatedQueue.report.assert_called_once_with()


def test_report():
    ("Queue#report should report consumers and producers to the backend")

    # Given a fake backend
    Backend = Mock(name='Backend')

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # And add some consumers
    queue.consumers.add("c1")
    queue.consumers.add("c2")

    # And add some producers
    queue.producers.add("p1")
    queue.producers.add("p2")

    # And call `report()`
    result = queue.report()

    # Then ut should be the result from Backend.report_steps
    result.should.equal(
        Backend.return_value.report_steps.return_value)

    # And report_steps should have been called appropriately
    Backend.return_value.report_steps.assert_called_once_with(
        'lineup:some-name', set(["c1", "c2"]), set(["p1", "p2"]))


def test_put_waits_for_queue_to_change():
    ("Queue#put pushes an item")

    # Given a fake backend
    Backend = Mock(name='Backend')
    backend = Backend.return_value

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # And call put
    queue.put("SOMETHING")

    # Then it should have pushed it to redis
    backend.rpush.assert_called_once_with(
        "lineup:some-name", "SOMETHING")


def test_get_wait():
    ("Queue#get when waiting pops an item")

    # Given a fake backend
    Backend = Mock(name='Backend')
    backend = Backend.return_value
    backend.lpop.side_effect = [None,  "COOL"]

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # And call get
    result = queue.get(True)

    # Then it should have popped from the redis list
    backend.lpop.assert_has_calls([
        call("lineup:some-name"),
        call("lineup:some-name"),
    ])


    # And the result should have come from the backend
    result.should.equal("COOL")


def test_get_nowait():
    ("Queue#get pops an item")

    # Given a fake backend
    Backend = Mock(name='Backend')
    backend = Backend.return_value

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # And call get
    result = queue.get(False)

    # Then it should have popped from the redis list
    backend.lpop.assert_called_once_with(
        "lineup:some-name")

    # And the result should have come from the backend
    result.should.equal(backend.lpop.return_value)


def test_get_size():
    ("Queue#get_size returns the length")

    # Given a fake backend
    Backend = Mock(name='Backend')
    backend = Backend.return_value

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # And call get_size
    queue.get_size().should.equal(
        backend.llen.return_value)

    # Then it should have pushed it to redis
    backend.llen.assert_called_once_with(
        "lineup:some-name")
