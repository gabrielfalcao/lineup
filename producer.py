#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
import sys
import time
import zmq


def producer():
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind("tcp://127.0.0.1:5557")
    # Start your result manager and workers before you start your producers
    for num in xrange(1000000):
        work_message = {
            'name': sys.argv[1]
        }
        zmq_socket.send_json(work_message)


producer()
