#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import traceback
from Queue import Queue


class Pipeline(object):
    def __init__(self):
        steps = getattr(self.__class__, 'steps', None) or []
        self.running = False
        self.queues = [Queue() for _ in self.steps] + [Queue()]
        self.workers = [self.make_worker(Worker, index) for index, Worker in enumerate(steps)]

    def make_worker(self, Worker, index):
        return Worker(self, self.queues[index], self.queues[index + 1])

    @property
    def input(self):
        return self.queues[0]

    @property
    def output(self):
        return self.queues[-1]

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
