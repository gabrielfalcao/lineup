#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright © 2013 Propellr LLC
#
from __future__ import unicode_literals
import time
from functools import wraps

def io_operation(method):
    """decorator for methods of a queue, allowing the redis
    operations to run in a lock per thread.
    """
    @wraps(method)
    def decorator(queue, *args, **kwargs):
        queue.lock.acquire()
        result = method(queue, *args, **kwargs)
        time.sleep(0)
        queue.lock.release()
        return result

    return decorator
