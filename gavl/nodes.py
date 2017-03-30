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


class BaseNodeVisitor(object):
    pass


class PostNodeVisitor(BaseNodeVisitor):
    node_class = Node

    def visit(self, node):
        name = node.__class__.__name__

        if isinstance(node, tuple):
            func = getattr(self, "visit_{}".format(name), self.default_visit)
            node = node.__class__(*[self.visit(x) for x in node])
            node = func(node)

        return node

    def default_visit(self, node):
        return node


class PreNodeVisitor(BaseNodeVisitor):
    node_class = Node

    def visit(self, node):
        name = node.__class__.__name__

        if isinstance(node, tuple):
            func = getattr(self, "visit_{}".format(name), self.default_visit)
            node = func(node)
            node = node.__class__(*[self.visit(x) for x in node])

        return node

    def default_visit(self, node):
        return node


class InlineNodeVisitor(BaseNodeVisitor):
    node_class = Node

    def visit(self, node):
        name = node.__class__.__name__

        if isinstance(node, tuple):
            func = getattr(self, "visit_{}".format(name), self.default_visit)
            node = node.__class__(*[self.visit(func(x)) for x in node])

        return node

    def default_visit(self, node):
        return node


# Use PostNodeVisitor as default visitor
NodeVisitor = PostNodeVisitor
