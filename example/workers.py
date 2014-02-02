#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import unicode_literals
import codecs
import re
import requests

from lineup import Step


class Download(Step):
    def after_consume(self, instructions):
        self.log(
            "Done downloading %s",
            instructions['url'],
        )

    def before_consume(self):
        self.log("The downloader is ready")

    def consume(self, instructions):
        url = instructions['url']
        method = instructions.get('method', 'get').lower()

        http_request = getattr(requests, method)
        response = http_request(url)
        instructions['download'] = {
            'content': response.content,
            'headers': dict(response.headers),
            'status_code': response.status_code,
        }
        self.produce(instructions)


class Cache(Step):
    def after_consume(self, instructions):
        self.log("Done caching %s", instructions.keys())

    def before_consume(self):
        self.log("The cacher is also ready")

    def get_slug(self, url):
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'\W+', '-', url)
        return url

    def consume(self, instructions):
        url = instructions['url']
        name = self.get_slug(url)
        with codecs.open(name, 'wb', 'utf-8') as fd:
            fd.write(instructions['download']['content'])
            fd.close()
            instructions['cached_at'] = {
                'filename': fd.name,
            }
        self.produce(instructions)
