#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from mock import Mock, patch
from lineup.framework import Node, Pipeline


class TestBackend(object):
    pass


class TestPipeline(Pipeline):
    name = 'test-pipeline'

    def __init__(self):
        self.backend_class = TestBackend

    timeout = 'forever'
    steps = ['step_class_1', 'step_class_2']


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


def test_noderun_daemon():
    ('Node#run_daemon should start all workers and be idempotent')

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node with some fake workers
    node = Node(Backend)
    w1 = Mock(name='w1')
    w2 = Mock(name='w2')
    node.workers = [w1, w2]

    # When I start
    node.run_daemon()
    node.run_daemon()

    # Then it should have started each worker
    w1.start.assert_called_once_with()
    w2.start.assert_called_once_with()

    # And it should be started
    node.started.should.be.true


def test_node_feed():
    ("Node#feed should start, wait until it's running and put the first item")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node
    node = Node(Backend)
    node.run_daemon = Mock(name='node.run_daemon')
    node.input = Mock(name='node.input')
    # When I feed
    node.feed({'an': 'item'})

    # Then it should have started each worker
    node.input.put.assert_called_once_with({'an': 'item'})


def test_node_stop():
    ("Node#stop should stop all workers")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node with some fake workers
    node = Node(Backend)
    w1 = Mock(name='w1')
    w2 = Mock(name='w2')
    node.workers = [w1, w2]

    # When I stop the node
    node.stop()

    # Then it should have stopped each
    w1.stop.assert_called_once_with()
    w2.stop.assert_called_once_with()


def test_node_is_running():
    ("Node#is_running should check if all workers are alive")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a node with some fake workers
    node = Node(Backend)

    w1 = Mock(name='w1')
    w1.is_alive.return_value = True

    w2 = Mock(name='w2')
    w2.is_alive.return_value = False

    node.workers = [w1, w2]

    # When I check if it is running
    node.is_running().should.be.false

    # Then it should have is_runningped each
    w1.is_alive.assert_called_once_with()
    w2.is_alive.assert_called_once_with()


def test_pipeline_initialize():
    ("Pipeline#initialize should create queues and workers")

    # Given a fake backend
    backend = Mock(name='backend')
    Backend = lambda: backend

    # And a pipeline that mocks out get_queues and make_worker
    class MyPipe(Pipeline):
        name = 'mypipe1'
        get_queues = Mock(name='MyPipe.get_queues')
        make_worker = Mock(name='MyPipe.make_worker')
        steps = ['step1', 'step2']

    MyPipe.make_worker.side_effect = lambda Klass, index: {
        'worker': index,
        'class': Klass,
    }

    # When I instantiate the Pipeline
    pipe = MyPipe(Backend)

    # Then it should have queues
    pipe.should.have.property('queues').being.equal(
        MyPipe.get_queues.return_value)

    # And it should have workers
    pipe.should.have.property('workers').being.equal([
        {'worker': 0, 'class': 'step1'},
        {'worker': 1, 'class': 'step2'},
    ])


def test_pipeline_input():
    ("Pipeline#input should return the first queue")

    # Given a pipeline with fake queues
    q1 = Mock(name='Queue(1)')
    q2 = Mock(name='Queue(2)')

    class MyPipe(Pipeline):
        name = 'mypipe2'
        def __init__(self):
            self.queues = [q1, q2]

    # When I instantiate the Pipeline
    pipe = MyPipe()

    # And get the input
    pipe.input.should.equal(q1)


def test_pipeline_output():
    ("Pipeline#output should return the last queue")

    # Given a pipeline with fake queues
    q1 = Mock(name='Queue(1)')
    q2 = Mock(name='Queue(2)')

    class MyPipe(Pipeline):
        name = 'mypipe3'
        def __init__(self):
            self.queues = [q1, q2]

    # When I instantiate the Pipeline
    pipe = MyPipe()

    # And get the output
    pipe.output.should.equal(q2)


def test_pipeline_get_result():
    ("Pipeline#get_result should get the output "
     "and wait for it")
    # Given a pipeline with fake queues
    q1 = Mock(name='Queue(1)')
    q2 = Mock(name='Queue(2)')

    class MyPipe(Pipeline):
        name = 'mypipe4'
        def __init__(self):
            self.queues = [q1, q2]

    # When I instantiate the Pipeline
    pipe = MyPipe()

    # And call get_result from the output queue
    pipe.get_result().should.equal(q2.get.return_value)

    # And the call should have waited
    q2.get.assert_called_once_with(wait=True)


@patch('lineup.framework.Queue')
def test_pipeline_make_queue(Queue):
    ("Pipeline#make_queue takes an index and "
     "creates the queue")

    # Given a pipeline
    pipe = TestPipeline()

    # When I call make_queue
    result = pipe.make_queue(42)

    # Then it should be a queue
    result.should.equal(Queue.return_value)

    # And Queue should have been called appropriately
    Queue.assert_called_once_with(
        'test-pipeline.queue.42',
        backend_class=TestBackend,
        timeout='forever')


def test_pipeline_get_queues():
    ("Pipeline#get_queues should make queues and return them")

    # Given a Pipeline that already has queues
    class MyPipe(TestPipeline):
        name = 'mypipe5'
        steps = ['step1', 'step2']
        make_queue = Mock(name='MyPipe.make_queue')

    MyPipe.make_queue.side_effect = (
        lambda x: 'Queue({0})'.format(x))

    pipe = MyPipe()

    # When I call get_queues
    result = pipe.get_queues()

    # Then it should have returned the existing queues
    result.should.equal([
        'Queue(0)',
        'Queue(1)',
        'Queue(2)',
    ])


def test_pipeline_make_worker():
    ("Pipeline#make_worker should get the output "
     "and wait for it")
    # Given a pipeline with fake queues
    q1 = Mock(name='Queue(1)')
    q2 = Mock(name='Queue(2)')
    q3 = Mock(name='Queue(3)')
    q4 = Mock(name='Queue(4)')
    q5 = Mock(name='Queue(5)')
    q6 = Mock(name='Queue(6)')

    class Worker(object):
        def __init__(self, previous, following, manager):
            self.previous = previous
            self.following = following
            self.manager = manager

    class MyPipe(Pipeline):
        name = 'mypipe6'
        def __init__(self):
            self.queues = [q1, q2, q3, q4, q5, q6]

    # When I instantiate the Pipeline
    pipe = MyPipe()

    # And call make_worker from the output queue
    result = pipe.make_worker(Worker, 4)

    # Then the result should be a Worker
    result.should.be.a(Worker)

    # And its previous queue should be the index one
    result.previous.should.equal(q5)

    # And its following queue should be the index one
    result.following.should.equal(q6)

    # And its manager should be set
    result.manager.should.equal(pipe)
