# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import imp
import logging
from plant import Node

from lineup.core import Registry


logging.captureWarnings(True)

pywarnings = logging.getLogger('py.warnings')
pywarnings.level = logging.ERROR

pywarnings.addHandler(logging.NullHandler(level=logging.ERROR))

logger = logging.getLogger('lineup.utils')


class PipelineScanner(object):
    def __init__(self, lookup_path):
        self.node = Node(lookup_path)
        self.found = self.find_python_files()

    def find_python_files(self):
        found = []
        for node in self.node.find_with_regex("pipelines.py$"):
            module_name = "{0}.{1}".format(
                node.dir.basename,
                node.basename.replace('.py', ''),
            )
            try:
                found.append(imp.load_source(module_name, node.path))
            except (ImportError, SystemError):
                logger.exception("Failed to import \033[1;33m%s\033[0m", str(node.path))

        return found

    def get_pipelines(self):
        return Registry.pipelines_by_name()
