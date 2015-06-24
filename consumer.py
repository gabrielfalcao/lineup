#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

import zmq


def consumer():
    print 'consumer ready'
    context = zmq.Context()
    # recieve work
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://127.0.0.1.6557")

    while True:
        work = consumer_receiver.recv_json()
        print 'received', work


consumer()
