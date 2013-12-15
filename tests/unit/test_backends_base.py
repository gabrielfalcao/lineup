#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from mock import patch, Mock
from lineup.backends.base import BaseBackend, io_operation


def test_get_name():
    ("BaseBackend#get_name should return class name by default")

    class MyBackend(BaseBackend):
        pass

    # Given an instance of a backend
    backend = MyBackend()

    # When I call get_name()
    result = backend.get_name()

    # Then it should be the class name
    result.should.equal("MyBackend")

def test_repr():
    ("BaseBackend#repr should return class name by default")

    class MyBackend(BaseBackend):
        pass

    # Given an instance of a backend
    backend = MyBackend()

    # When I call repr()
    result = repr(backend)

    # Then it should be the class name
    result.should.equal("<MyBackend>")


@patch('lineup.backends.base.time')
def test_io_operation(time):
    ("@io_operation should acquire lock")

    stack = []

    register = lambda name: lambda *args: stack.append(name)
    lock = Mock(name='FakeBackend.lock')

    lock.acquire.side_effect = register('lock.acquire')
    time.sleep.side_effect = register('time.sleep')
    lock.release.side_effect = register('lock.release')
    lock.foo.side_effect = register('lock.foo')


    class FakeBackend(object):

        def __init__(self):
            self.lock = lock

        @io_operation
        def foo(self):
            self.lock.foo('bar')
            return 'YAY'

    backend = FakeBackend()

    result = backend.foo()
    result.should.equal('YAY')

    stack.should.equal([
        'lock.acquire',
        'lock.foo',
        'time.sleep',
        'lock.release'
    ])
