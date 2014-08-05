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
    backend.put.assert_called_once_with(
        "lineup:some-name", "SOMETHING")


def test_get_wait():
    ("Queue#get when waiting pops an item")

    # Given a fake backend
    Backend = Mock(name='Backend')
    backend = Backend.return_value
    backend.pop.side_effect = [None, "COOL"]

    # When I create a Queue
    queue = Queue("some-name", Backend)

    # And call get
    result = queue.get(True, "some-owner")

    # Then it should have popped from the redis list
    backend.pop.assert_has_calls([
        call("lineup:some-name", "some-owner", 10),
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
    result = queue.get(False, "some-owner", 20)

    # Then it should have popped from the redis list
    backend.pop.assert_called_once_with(
        "lineup:some-name", "some-owner", 20)

    # And the result should have come from the backend
    result.should.equal(backend.pop.return_value)
