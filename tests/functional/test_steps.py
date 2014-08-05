#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

from lineup.steps import KeyMaker, Step


class FakeStep(object):
    def __init__(self, name):
        self.name = name


def test_key_maker_make_key():
    ("KeyMaker#make_key ")

    step = FakeStep("SOMENAME")
    maker = KeyMaker(step)

    maker.make_key("FOO").should.equal("lineup:SOMENAME:FOO")
