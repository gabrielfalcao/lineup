#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import os
import time
import socket
import signal
from threading import Thread
from lineup.datastructures import Queue
from lineup.core import PipelineRegistry


class Node(object):
    timeout = -1

    def __init__(self, backend_class, *args, **kw):
        self.workers = []
        self.backend_class = backend_class
        signal.signal(signal.SIGINT, self.handle_control_c)
        self.__started = False
        self.initialize(*args, **kw)

    def get_backend(self, *args, **kwargs):
        return self.backend_class(*args, **kwargs)

    def handle_control_c(self, signal, frame):
        self.stop()
        sys.exit(1)

    def initialize(self, *args, **kw):
        pass

    @property
    def id(self):
        parts = [
            self.get_name(),
            self.get_hostname(),
            str(os.getpid()),
        ]
        return '|'.join(parts)

    @property
    def taxonomy(self):
        class_name = self.__class__.__name__
        module_name = self.__class__.__module__
        return '.'.join([module_name, class_name])

    def get_name(self):
        return getattr(self, 'name', None) or self.taxonomy

    def get_hostname(self):
        return socket.gethostname()

    def run_daemon(self):
        if self.__started:
            return

        self.heartbeat.start()

        for worker in self.workers:
            worker.start()

        self.__started = True

    def feed(self, item):
        self.input.put(item)

    def stop(self):
        for child in self.workers:
            child.stop()

        self.heartbeat.stop()
        self.__started = False

    @property
    def started(self):
        return self.__started

    def is_running(self):
        return all([w.is_alive() for w in self.workers])


class Pipeline(Node):
    timeout = -1

    __metaclass__ = PipelineRegistry

    def initialize(self, *args, **kwargs):
        self.queues = self.get_queues(*args, **kwargs)
        isteps = enumerate(self.steps)
        self.workers = [self.make_worker(Type, i) for i, Type in isteps]
        self.heartbeat = HeartBeatMonitor(self.queues)

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
            self.name,
            b'queue',
            str(index),
        ])
        return Queue(name,
                     backend_class=self.backend_class,
                     timeout=self.timeout)

    def get_queues(self):
        steps = getattr(self, 'steps', None) or []
        indexes = xrange(len(steps) + 1)
        queues = map(self.make_queue, indexes)
        return queues

    def make_worker(self, Worker, index):
        previous = self.queues[index]
        following = self.queues[index + 1]
        return Worker(previous, following, self)


class HeartBeatMonitor(Thread):
    def __init__(self, queues, backend_class, interval=1):
        self.queue_names = [q.name for q in queues]
        self.interval = interval
        self.backend = backend_class()

    def stop(self):
        self.in_use.release()

    def run(self):
        self.in_use.acquire()

        while self.in_use.locked():
            for name in self.queue_names:
                self.backend.heartbeat(name)

            time.sleep(1)
