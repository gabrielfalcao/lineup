#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os
import socket
import traceback
from lineup.datastructures import Queue


class Node(object):
    def __init__(self, *args, **kw):
        self.initialize(*args, **kw)

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

    def start(self):
        for worker in self.workers:
            worker.start()

    def feed(self, item):
        self.input.put(item)

    def enqueue_error(self, source_class, instructions, exception):
        print exception, source_class, instructions

    def wait_and_get_work(self):
        return self.output.get()

    @property
    def running(self):
        return all([w.alive for w in self.workers])

    def are_running(self):
        if self.running:
            return True

        self.start()
        return self.running


class Pipeline(Node):
    def initialize(self):
        self.queues = self.get_queues(*args, **kw)
        self.workers = [self.make_worker(Worker, index) for index, Worker in enumerate(steps)]

    @property
    def input(self):
        return self.queues[0]

    @property
    def output(self):
        return self.queues[-1]

    def get_queues(self):
        steps = getattr(self, 'steps', None) or []
        return [Queue() for _ in steps] + [Queue()]

    def make_worker(self, Worker, index):
        return Worker(self, self.queues[index], self.queues[index + 1])
