# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import imp
import logging
from plant import Node

from lineup.core import Registry

logger = logging.getLogger('lineup.utils')
logging.captureWarnings(True)

pywarnings = logging.getLogger('py.warnings')
pywarnings.level = logging.ERROR

pywarnings.addHandler(logging.NullHandler(level=logging.ERROR))


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
                logger.debug("Failed to import %s", str(node))

        return found

    def get_pipelines(self):
        return Registry.pipelines_by_name()
