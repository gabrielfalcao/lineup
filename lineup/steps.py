#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals

import sys
import time
import json
import signal
import logging
import traceback
from pprint import pformat
from redis import StrictRedis
from threading import Thread, Event, RLock
from lineup.datastructures import Queue

class KeyMaker(object):
    def __init__(self, step):
        self.step = step

        for name in ['logging', 'alive', 'error']:
            setattr(self, name, self.make_key(name))

    def make_key(self, suffix, prefix=None):
        prefix = prefix or getattr(self, 'prefix', 'lineup')
        return ":".join([prefix, self.step.name, suffix])



class Step(Thread):
    def __init__(self, consume_queue, produce_queue, parent):

        self.parent = parent
        self.consume_queue = consume_queue
        self.produce_queue = produce_queue
        self.lock = RLock()
        self.backend = parent.get_backend()

        self.ready = Event()
        self.ready.clear()

        super(Step, self).__init__()
        self.daemon = True
        self.key = KeyMaker(self)
        self.name = getattr(self.__class__,
                            'label', self.taxonomy)
        self.logger = logging.getLogger(self.key.logging)
        consume_queue.adopt_consumer(self)
        produce_queue.adopt_producer(self)

    def get_name(self):
        return getattr(self, 'name', None) or self.taxonomy

    def __str__(self):
        return '<{0}>'.format(self.ancestry)

    @property
    def taxonomy(self):
        class_name = self.__class__.__name__
        module_name = self.__class__.__module__
        return '.'.join([module_name, class_name])

    @property
    def ancestry(self):
        return '|'.join([self.get_name(), self.parent.get_name()])

    @property
    def id(self):
        return '|'.join([self.parent.id, str(self.ident), self.ancestry])

    def log(self, message, *args, **kw):
        self.logger.info(message, *args, **kw)
        return self.backend.rpush(self.key.logging, {
            'message': message % args % kw,
            'when': time.time()
        })

    def fail(self, exception, instructions):
        error = traceback.format_exc(exception)
        message = 'The worker %s failed while being processed at the pipeline "%s"'
        args = (self.name, self.parent.get_name())
        self.logger.exception(message, *args)
        self.parent.enqueue_error(source_class=self.__class__, instructions=instructions, exception=exception)
        return self.backend.rpush(self.key.logging, {
            'message': message % args,
            'when': time.time()
        })

    def do_consume(self, instructions):
        return self.consume(instructions)

    def produce(self, payload):
        return self.produce_queue.put(payload)

    def before_consume(self):
        self.log("%s is about to consume its queue", self)

    def after_consume(self, instructions):
        self.log("%s is done", self)

    def do_rollback(self, instructions):
        try:
            self.rollback(instructions)
        except Exception as e:
            self.fail(e, instructions)

    def rollback(self, instructions):
        pass

    def stop(self):
        return self.ready.set()

    def is_active(self):
        is_locked = self.ready.is_set()
        timeout = self.parent.timeout

        if timeout < 0:
            timeout = None

        if is_locked:
            self.ready.wait(timeout)

        return not is_locked

    def run(self):
        while self.is_active():
            self.before_consume()

            instructions = self.consume_queue.get(wait=True)

            if not instructions:
                continue

            try:
                self.do_consume(instructions)
            except Exception as e:
                error = traceback.format_exc(e)
                self.log(error)
                instructions.update({
                    '__lineup__error__': {
                        'traceback': error,
                        'consume_queue_size': self.consume_queue.get_size(),
                        'produce_queue_size': self.produce_queue.get_size(),
                    }
                })
                self.produce(instructions)
                self.do_rollback(instructions)
                continue
            finally:
                self.ready.clear()

            self.after_consume(instructions)
