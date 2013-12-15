#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os
import socket
import time
import signal
import traceback
from lineup.datastructures import Queue


class Node(object):
    timeout = -1
    def __init__(self, *args, **kw):
        self.children = []
        signal.signal(signal.SIGINT, self.handle_control_c)
        self.__started = False
        self.initialize(*args, **kw)

    def handle_control_c(self, signal, frame):
        for child in self.workers:
            child.stop()

        raise SystemExit

    def initialize(self, *args, **kw):
        pass

    @property
    def id(self):
        return '|'.join([self.get_hostname(), str(os.getpid())])

    @property
    def taxonomy(self):
        class_name = self.__class__.__name__
        module_name = self.__class__.__module__
        return '.'.join([module_name, class_name])

    def get_name(self):
        return getattr(self, 'name', None) or self.taxonomy

    def get_hostname(self):
        return socket.gethostname()

    def make_worker(self, Worker, index):
        return Worker(self, self.input, self.output)

    def _start(self):
        if self.__started:
            return

        for worker in self.workers:
            worker.start()

        self.__started = True

    def feed(self, item):
        self._start()
        while not self.running:
            time.sleep(0)

        result = self.input.put(item, wait=False)
        return result

    def enqueue_error(self, source_class, instructions, exception):
        print exception, source_class, instructions

    def wait_and_get_work(self):
        return self.output.get()

    def stop(self):
        for child in self.workers:
            child.stop()

    @property
    def running(self):
        return all([w.is_alive() for w in self.workers])

    def are_running(self):
        if self.running:
            return True

        self.start()
        return self.running


class Pipeline(Node):
    timeout = -1

    def initialize(self, *args, **kwargs):
        self.queues = self.get_queues(*args, **kwargs)
        self.workers = [self.make_worker(Worker, index) for index, Worker in enumerate(self.steps)]

    @property
    def input(self):
        return self.queues[0]

    @property
    def output(self):
        return self.queues[-1]

    def get_result(self):
        return self.output.get(wait=True)

    def make_queue(self, index):
        name = '.'.join([
            self.__class__.__module__,
            self.__class__.__name__,
            b'queue',
            str(index),
        ])
        return Queue(name, timeout=self.timeout)

    def get_queues(self):
        steps = getattr(self, 'steps', None) or []
        indexes = xrange(len(steps) + 1)
        queues = map(self.make_queue, indexes)
        return queues

    def make_worker(self, Worker, index):
        return Worker(self.queues[index], self.queues[index + 1], self)
