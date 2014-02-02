#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

from lineup import registry


class Registry(type):
    def __init__(cls, name, bases, attrs):
        has_name = hasattr(cls, 'name')
        if name != 'Pipeline' and not has_name:
            msg = ('Pipelines must have an attribute '
                   'called "name": {0}'.format(cls))
            raise TypeError(msg)
        elif has_name:
            registry.BY_KIND[cls.kind][cls.name] = cls
        super(Registry, cls).__init__(name, bases, attrs)

    @classmethod
    def clear(cls):
        registry.BY_KIND.clear()

    @classmethod
    def pipelines_by_name(cls):
        return registry.BY_KIND['pipeline']


class PipelineRegistry(Registry):
    kind = 'pipeline'


class LineUpKeyError(KeyError):
    pass


class LineUpPayloadDict(dict):
    # # TODO
    # def get(self, key, fallback=None):
    #     value = super(LineUpPayloadDict, self).get(key, fallback)
    #     if isinstance(value, dict):
    #         value = LineUpPayloadDict(value)
    #     return value

    def __getitem__(self, key, *args):
        try:
            value = super(LineUpPayloadDict, self).__getitem__(key, *args)
            return value
        except KeyError:
            msg = ("expected key {1} to be present "
                   "in the payload {0}".format(self, key))
            raise LineUpKeyError(msg)
