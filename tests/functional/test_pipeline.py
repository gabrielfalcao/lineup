#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

import signal
from mock import patch
from lineup import Pipeline, Step, JSONRedisBackend
from lineup.steps import KeyMaker


@patch('lineup.framework.sys')
def test_pipeline_should_stop_upon_sigint(sys):
    ("Pipeline should handle SIGINT")

    class SimpleStep(Step):
        def consume(self, instructions):
            self.produce(instructions)

    class Things(Pipeline):
        name = 'things-1'
        steps = [
            SimpleStep,
            SimpleStep,
            SimpleStep,
        ]

    pipeline = Things(JSONRedisBackend)
    pipeline.run_daemon()
    pipeline.is_running().should.be.true
    pipeline.started.should.be.true

    pipeline.feed({'cool': 'yeah'})
    pipeline.stop()
    pipeline.handle_control_c(signal.SIGINT, None)
    sys.exit.assert_called_once_with(1)


def test_key_maker():
    ("KeyMaker#make_key should prefix things with step name")
    class FakeStep(object):
        name = 'fake-thing'

    km = KeyMaker(FakeStep())
    km.make_key("FOO").should.equal('lineup:fake-thing:FOO')
