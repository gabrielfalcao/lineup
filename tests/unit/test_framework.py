#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from mock import Mock, patch, call
from lineup.framework import Node


@patch('lineup.framework.sys')
def test_node_handle_control_c(sys):
    ("Node#handle_control_c should stop each "
     "worker and raise SystemExit")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node with some fake workers
    node = Node(Backend)
    w1 = Mock(name='w1')
    w2 = Mock(name='w2')
    node.workers = [w1, w2]

    # When I call handle_control_c
    node.handle_control_c("SIGINT", 42)

    # Then it should raise SystemExit
    sys.exit.assert_called_once_with(1)

    # And stop() should have been called in both workers
    w1.stop.assert_called_once_with()
    w2.stop.assert_called_once_with()


def test_node_get_backend():
    ("Node#get_backend should return an instance "
     "of the given Backend class")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node
    node = Node(Backend)

    # When I call get_backend
    node.get_backend().should.equal(backend)


@patch('lineup.framework.os')
@patch('lineup.framework.socket')
def test_node_id(socket, os):
    ("Node#id should stop each "
     "worker and raise SystemExit")

    socket.gethostname.return_value = 'localhost'
    os.getpid.return_value = 123
    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node
    node = Node(Backend)

    # When I call id
    node.id.should.equal('lineup.framework.Node|localhost|123')


def test_node_start():
    ('Node#_start should start all workers and be idempotent')

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node with some fake workers
    node = Node(Backend)
    w1 = Mock(name='w1')
    w2 = Mock(name='w2')
    node.workers = [w1, w2]

    # When I start
    node._start()
    node._start()

    # Then it should have started each worker
    w1.start.assert_called_once_with()
    w2.start.assert_called_once_with()


@patch('lineup.framework.time')
def test_node_feed(time):
    ("Node#feed should start, wait until it's running and put the first item")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node
    node = Node(Backend)
    node._start = Mock(name='node._start')
    node.input = Mock(name='node.input')
    node.is_running = Mock(name='node.is_running')
    node.is_running.side_effect = [False, True]
    # When I feed
    node.feed({'an': 'item'})

    # Then it should have started each worker
    time.sleep.assert_called_once_with(0)
    node.input.put.assert_called_once_with({'an': 'item'})

    node._start.assert_called_once_with()
