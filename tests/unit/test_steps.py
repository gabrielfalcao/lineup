#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from mock import Mock, patch, call
from lineup.steps import Step, KeyMaker

class TestStep(Step):
    def __init__(self):
        super(Step, self).__init__()

    def __repr__(self):
        return self.__class__.__name__

def test_key_maker():
    ("KeyMaker should make keys for logging, alive and error")

    # Given a fake step that has a name
    step = Mock()
    step.name='coolbabe'

    # When I create a KeyMaker for that step
    km = KeyMaker(step)

    # Then it should have a key for logging
    km.should.have.property("logging").being.equal(
        'lineup:coolbabe:logging')

    # And it should have a key for alive
    km.should.have.property("alive").being.equal(
        'lineup:coolbabe:alive')

    # And it should have a key for error
    km.should.have.property("error").being.equal(
        'lineup:coolbabe:error')


@patch("lineup.steps.Event")
@patch("lineup.steps.KeyMaker")
@patch("lineup.steps.logging")
def test_step_manages_to_get_adopted(logging, KeyMaker, Event):
    ("Step should manage to be adopted by the given "
     "consumer and producer queues")

    # Given a consumer queue
    consumer = Mock(name='consumer_queue')
    # And a producer queue
    producer = Mock(name='producer_queue')
    # And a manager
    manager = Mock(name='manager')

    step = Step(consumer, producer, manager)

    consumer.adopt_consumer.assert_called_once_with(step)
    producer.adopt_producer.assert_called_once_with(step)

    step.ready.should.equal(Event.return_value)
    step.key.should.equal(KeyMaker.return_value)

    KeyMaker.assert_called_once_with(step)

    step.logger.should.equal(logging.getLogger.return_value)
    logging.getLogger.assert_called_once_with(
        KeyMaker.return_value.logging)


def test_step_get_name():
    ("Step get_name")
    class MyStep(Step):
        pass

    consume = Mock(name='consume')
    produce = Mock(name='produce')
    parent = Mock(name='parent')

    step = MyStep(consume, produce, parent)

    step.get_name().should.equal('tests.unit.test_steps.MyStep')


def test_step_as_repr():
    ("repr(Step) should return its ancestry")
    class MyStep(Step):
        pass

    consume = Mock(name='consume')
    produce = Mock(name='produce')
    parent = Mock(name='parent')
    parent.get_name.return_value = 'PARENTNAME'
    step = MyStep(consume, produce, parent)

    result = repr(step)

    result.should.equal(
        '<tests.unit.test_steps.MyStep|PARENTNAME>')


def test_step_as_id():
    ("id(Step) should return its ancestry")
    class MyStep(Step):
        ancestry = 'this is the ancestry'
        ident = 'identity'
    consume = Mock(name='consume')
    produce = Mock(name='produce')
    parent = Mock(name='parent')
    parent.id = 'parentid'
    step = MyStep(consume, produce, parent)

    result = step.id

    result.should.equal(
        'parentid|identity|this is the ancestry')


@patch("lineup.steps.KeyMaker")
@patch("lineup.steps.time")
@patch("lineup.steps.logging")
def test_step_log(logging, time, KeyMaker):
    ("Step#log should log as INFO and push to a redis list")

    KeyMaker.return_value.logging = 'step_key_for_logging'

    time.time.return_value = 'CLOCK'
    # Given a consumer queue
    consumer = Mock(name='consumer_queue')
    # And a producer queue
    producer = Mock(name='producer_queue')
    # And a manager
    parent = Mock(name='parent')
    backend = parent.get_backend.return_value


    class FooStep(Step):
        def consume(self, instructions):
            self.produce(instructions)

    step = FooStep(consumer, producer, parent)
    step.log("Mensagem %s", "arg1", foo="bar")
    backend.rpush.assert_called_once_with(
        'step_key_for_logging', {
            'message': 'Mensagem arg1',
            'when': 'CLOCK'
        })


def test_step_do_consume():
    ("Step#do_consume should call consume")

    # Given a consumer queue
    consumer_queue = Mock(name='consumer_queue')
    # And a producer queue
    producer_queue = Mock(name='producer_queue')
    # And a manager
    parent = Mock(name='parent')

    # And a step that implements consume
    class CoolStep(Step):
        consume = Mock(name='CoolStep.consume')

    step = CoolStep(consumer_queue, producer_queue, parent)
    step.do_consume({'instructions': True})

    CoolStep.consume.assert_called_once_with(
        {'instructions': True})


def test_step_produce():
    ("Step#produce should put the payload in "
     "the producer_queue")

    # Given a consumer queue
    consumer_queue = Mock(name='consumer_queue')
    # And a producer queue
    producer_queue = Mock(name='producer_queue')
    # And a manager
    parent = Mock(name='parent')

    # And a step that implements consume
    class CoolStep(Step):
        consume = Mock(name='CoolStep.consume')

    step = CoolStep(consumer_queue, producer_queue, parent)

    step.produce({'foo': 'bar'})

    producer_queue.put.assert_called_once_with(
        {'foo': 'bar'})


@patch("lineup.steps.logging")
def test_step_before_consume(logging):
    ("Step#before_consume by default just logs")

    logger = logging.getLogger.return_value

    # Given a consumer queue
    consumer_queue = Mock(name='consumer_queue')

    # And a producer queue
    producer_queue = Mock(name='producer_queue')

    # And a manager
    parent = Mock(name='parent')

    # And a step that implements consume
    class CoolStep(Step):
        log = Mock(name='CoolStep.log')

    step = CoolStep(
        consumer_queue, producer_queue, parent)

    step.before_consume()

    CoolStep.log.assert_has_calls([
        call("%s is about to consume its queue", step),
    ])


@patch("lineup.steps.logging")
def test_step_after_consume(logging):
    ("Step#after_consume by default just logs")

    logger = logging.getLogger.return_value

    # Given a consumer queue
    consumer_queue = Mock(name='consumer_queue')

    # And a producer queue
    producer_queue = Mock(name='producer_queue')

    # And a manager
    parent = Mock(name='parent')

    # And a step that implements consume
    class CoolStep(Step):
        log = Mock(name='CoolStep.log')

    step = CoolStep(
        consumer_queue, producer_queue, parent)

    step.after_consume({
        'instructions': True,
    })

    CoolStep.log.assert_has_calls([
        call("%s is done", step),
    ])


def test_step_do_rollback():
    ("Step#do_rollback should call consume")

    # Given a consumer queue
    consumer_queue = Mock(name='consumer_queue')
    # And a producer queue
    producer_queue = Mock(name='producer_queue')
    # And a manager
    parent = Mock(name='parent')

    # And a step that implements consume
    class CoolStep(Step):
        rollback = Mock(name='CoolStep.rollback')

    step = CoolStep(consumer_queue, producer_queue, parent)
    step.do_rollback({'instructions': True})

    CoolStep.rollback.assert_called_once_with(
        {'instructions': True})


@patch("lineup.steps.Event")
def test_step_stop(Event):
    ("Step stop should set the ready event")
    class MyStep(Step):
        pass

    consume = Mock(name='consume')
    produce = Mock(name='produce')
    parent = Mock(name='parent')

    step = MyStep(consume, produce, parent)

    step.stop()
    step.ready.should.equal(Event.return_value)

    Event.return_value.set.assert_called_once_with()


@patch("lineup.steps.Event")
def test_step_is_active(Event):
    ("Step is_active")

    Event.return_value.is_set.return_value = False

    class MyStep(Step):
        pass

    consume = Mock(name='consume')
    produce = Mock(name='produce')
    parent = Mock(name='parent')

    step = MyStep(consume, produce, parent)
    step.is_active().should.be.true


def test_step_rollback():
    ("Step#rollback should call consume")

    # Given a consumer queue
    consumer_queue = Mock(name='consumer_queue')
    # And a producer queue
    producer_queue = Mock(name='producer_queue')
    # And a manager
    parent = Mock(name='parent')

    # And a step that implements consume
    step = Step(consumer_queue, producer_queue, parent)
    step.rollback({'instructions': True})

    consumer_queue.put.assert_called_once_with(
        {'instructions': True})


@patch('lineup.steps.traceback')
def test_step_handle_exception(traceback):
    ("Step#handle_exception should log thre traceback, "
     "forward the instructions")

    traceback.format_exc.return_value = 'the infamous traceback'

    class MyStep(TestStep):
        consume_queue = Mock(name='MyStep.consume_queue')
        produce_queue = Mock(name='MyStep.produce_queue')

        produce = Mock(name='MyStep.produce')
        log = Mock(name='MyStep.log')
        do_rollback = Mock(name='MyStep.do_rollback')

    MyStep.consume_queue.get_size.return_value = '# consumers'
    MyStep.produce_queue.get_size.return_value = '# producers'

    step = MyStep()

    my_exception = ValueError("WHAAAT")
    step.handle_exception(
        my_exception, {'some': 'instructions'})

    MyStep.log.assert_called_once_with(
        'the infamous traceback')

    MyStep.produce.assert_called_once_with({
        u'some': u'instructions',
        u'__lineup__error__': {
            u'consume_queue_size': u'# consumers',
            u'produce_queue_size': u'# producers',
            u'traceback': u'the infamous traceback',
        }
    })


def test_step_run():
    ("Step#run should loop while active")

    class MyStep(TestStep):
        is_active = Mock(name='MyStep.is_active')
        loop = Mock(name='MyStep.loop')

    MyStep.is_active.side_effect = [True, True, False]

    step = MyStep()
    step.run()

    MyStep.loop.assert_has_calls([
        call(),
        call(),
    ])


def test_step_loop():
    ("Step#loop should trigger events of before/after "
     "consume, then consume queue and ensure its lifecycle")


    stack = []
    register = lambda name: lambda *args, **kw: stack.append(
        (name, args, kw))

    class MyStep(TestStep):
        consume_queue = Mock(name='consume_queue')
        before_consume = register('before_consume')
        after_consume = register('after_consume')
        do_consume = Mock(name='MyStep.do_consume')
        ready = Mock(name='MyStep.ready')

    MyStep.ready.clear = register('ready.clear()')
    step = MyStep()
    MyStep.consume_queue.get.return_value = 'instructions'

    step.loop()


    MyStep.do_consume.assert_called_once_with('instructions')

    stack.should.equal([
        ('before_consume', (step,), {}),
        ('ready.clear()', (), {}),
        ('after_consume', (step, 'instructions'), {})
    ])


def test_step_loop_upon_exception():
    ("Step#loop should call handle_exception if it happens")


    stack = []
    register = lambda name: lambda *args, **kw: stack.append(
        (name, args, kw))

    class MyStep(TestStep):
        before_consume = register('before_consume')
        after_consume = register('after_consume')

        ready = Mock(name='MyStep.ready')
        consume_queue = Mock(name='consume_queue')
        do_consume = Mock(name='MyStep.do_consume')
        handle_exception = Mock(name='MyStep.handle_exception')


    exc = ValueError('BOOM')
    MyStep.do_consume.side_effect = exc

    MyStep.ready.clear = register('ready.clear()')
    step = MyStep()
    MyStep.consume_queue.get.return_value = 'instructions'

    step.loop()

    MyStep.do_consume.assert_called_once_with('instructions')
    MyStep.handle_exception.assert_called_once_with(
        exc, 'instructions')

    stack.should.equal([
        ('before_consume', (step,), {}),
        ('ready.clear()', (), {}),
        ('after_consume', (step, 'instructions'), {})
    ])
