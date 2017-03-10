# Copyright 2017 by Teem, and other contributors,
# as noted in the individual source code files.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections


Node = collections.namedtuple


class NodeVisitor(object):
    node_class = Node

    def visit(self, node):
        name = node.__class__.__name__

        pre_func = getattr(self, "pre_visit_{}".format(name), None)
        post_func = (getattr(self, "post_visit_{}".format(name), None) or
                     getattr(self, "visit_{}".format(name), None))

        if isinstance(node, tuple):
            extras = {}
            if pre_func is not None:
                node, extras = pre_func(node)
            else:
                node, extras = self.pre_default_visit(node)

            node = node.__class__(*[self.visit(x) for x in node])

            if post_func is not None:
                node = post_func(node, **extras)
            else:
                node = self.post_default_visit(node)

        return node

    def default_visit(self, node):
        return node

    def pre_default_visit(self, node):
        return node, {}

    def post_default_visit(self, node):
        return self.default_visit(node)
