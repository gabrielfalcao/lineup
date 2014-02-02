#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
from lineup import Pipeline
from lineup.core import (
    Registry, LineUpPayloadDict, LineUpKeyError)


def test_registry_missing_name():
    ("Pipelines must have a name")

    def define_pipeline():
        class SomePipeline(Pipeline):
            pass

    expected_message = (
        "Pipelines must have an attribute called "
        "\"name\": <class 'tests.unit.test_core.SomePipeline'>")

    define_pipeline.when.called.should.throw(
        TypeError, expected_message)


def test_define_pipelines_and_get_by_name():
    ("Registry.pipelines_by_name should return all "
     "the registered ones")

    Registry.clear()

    class MyPipe1(Pipeline):
        name = 'tha-numba-one'

    class MyPipe2(Pipeline):
        name = 'tha-numba-two'

    Registry.pipelines_by_name().should.equal({
        u'tha-numba-one': MyPipe1,
        u'tha-numba-two': MyPipe2,
    })


def test_lineuppayloaddict_error():
    data = LineUpPayloadDict({})

    def fail():
        data['foo']

    fail.when.called.should.throw(
        LineUpKeyError,
        'expected key foo to be present in the payload {}'
    )


def test_lineuppayloaddict_getitem():
    data = LineUpPayloadDict({'foo': "bar"})
    data['foo'].should.equal("bar")
