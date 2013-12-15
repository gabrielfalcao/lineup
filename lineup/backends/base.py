# #!/usr/bin/env python
# -*- coding: utf-8 -*-
# <lineup - python distributed pipeline framework>
# Copyright (C) <2013>  Gabriel Falcão <gabriel@nacaolivre.org>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from __future__ import unicode_literals

import time
from functools import wraps
from threading import RLock


class BaseBackend(object):
    def __init__(self, *args, **kwargs):
        self.lock = RLock()
        self.initialize(*args, **kwargs)

    def get_name(self):
        return self.__class__.__name__

    def __repr__(self):
        return '<{0}>'.format(self.get_name())

    def initialize(self, *args, **kwargs):
        """to be overwriten by subclasses"""


def io_operation(method):
    """decorator for methods of a backend, allowing the redis
    operations to run in a lock per thread.
    """
    @wraps(method)
    def decorator(backend, *args, **kwargs):
        backend.lock.acquire()
        result = method(backend, *args, **kwargs)
        time.sleep(0)
        backend.lock.release()
        return result

    return decorator
