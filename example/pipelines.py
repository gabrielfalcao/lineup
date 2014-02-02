#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
import re
import sys
import json
import tempfile
import requests

from lineup import Step, Pipeline, JSONRedisBackend


class Download(Step):
    def consume(self, instructions):
        url = instructions['url']
        method = instructions['method'].lower()

        http_request = getattr(requests, method)
        response = http_request(url)
        instructions['download'] = {
            'content': response.content,
            'headers': dict(response.headers),
            'status_code': response.status_code,
        }
        self.produce(instructions)


class Cache(Step):
    def name(self, url):
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'\W+', '-', url)
        return url

    def consume(self, instructions):
        url = instructions['url']
        name = self.name(url)
        with open(name, 'wb') as fd:
            fd.write(instructions['download']['content'])
            fd.close()
            instructions['cached_at'] = {
                'filename': fd.name,
            }
        self.produce(instructions)


class SimpleUrlDownloader(Pipeline):
    name = 'downloader'
    steps = [Download, Cache]
