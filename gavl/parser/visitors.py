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

import sys
from gavl import constants
from gavl.relalg.nodes import *

class GavlToRelAlg(nodes.NodeVisitor):
    def __init__(self):
        self._counter = 0

    def gensym(self):
        self._counter = self._counter + 1
        return "_gensym_{}".format(self._counter)

    def visit_var(self, node):
        if node.relation:
            return ProjectNode(node.relation, [node.var_name])
        else:
            return RelationNode(node.var_name)

    def visit_int(self, node):
        return ConstantNode(self.gensym(), node.value)

    def visit_relation(self, node):
        return RelationNode(node.name)

    def visit_binary_op(self, node):
        op_code, left, right = node
        left_is_var = len(ActiveFieldResolver().visit(left)) > 0
        right_is_var = len(ActiveFieldResolver().visit(right)) > 0

        if left_is_var and right_is_var:
            left_fields = list(ActiveFieldResolver().visit(left))
            right_fields = list(ActiveFieldResolver().visit(right))
            assert len(left_fields) == 1
            assert len(right_fields) == 1
            if (hasattr(left, 'relation') and hasattr(right, 'relation')
                and left.relation == right.relation):
                return ArithmeticNode(
                    ProjectNode(
                        left.relation,
                        [left_fields[0], right_fields[0]]
                    ),
                    self.gensym(), left_fields[0], right_fields[0], op_code)
            else:
                return ArithmeticNode(
                    JoinNode(left, right, constants.JoinTypes.INNER,
                            constants.JoinSides.FULL),
                    self.gensym(), left_fields[0], right_fields[0], op_code)
        elif left_is_var or right_is_var:
            active_field = None
            if left_is_var:
                active_field = list(ActiveFieldResolver().visit(left))[0]
            if right_is_var:
                active_field = list(ActiveFieldResolver().visit(right))[0]
            assert active_field is not None

            return ProjectNode(
                JoinNode(left, right, constants.JoinTypes.INNER,
                         constants.JoinSides.FULL),
                [active_field]
            )
        else:
            return JoinNode(left, right, constants.JoinTypes.INNER,
                            constants.JoinSides.FULL)


    def visit_apply(self, node):
        func_name, func_arg = node
        assert len(list(ActiveFieldResolver().visit(node.func_arg))) == 1, str(func_arg)
        return AggNode(func_arg,
                       self.gensym(),
                       list(ActiveFieldResolver().visit(node.func_arg))[0],
                       constants.AggFuncs[func_name.upper()],
                       [])
                       #self.groups)

    def visit_assign(self, node):
        return AssignNode(node.var_name, node.expr)


class ActiveFieldResolver(nodes.NodeVisitor):

    def visit_constant(self, node):
        return {node.field}

    def visit_relation(self, node):
        return set([])

    def visit_project(self, node):
        return set(node.fields)

    def visit_rename(self, node):
        return node.relation

    def visit_join(self, node):
        return node.left | node.right

    def visit_arithmetic(self, node):
        return {node.out_field}

    def visit_agg(self, node):
        return {node.out_field}
