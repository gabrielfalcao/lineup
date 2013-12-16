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
