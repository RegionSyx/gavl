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
import numbers
import collections
import sqlalchemy as sa
from gavl import constants, parse, parser, nodes

RelAlgNode = nodes.Node

ConstantNode = RelAlgNode("constant", "field value")
RelationNode = RelAlgNode("relation", "name")
ProjectNode = RelAlgNode("project", "relation fields")
RenameNode = RelAlgNode("rename", "relation old_name new_name")
JoinNode = RelAlgNode("join", "left, right, join_type, join_side")
ArithmeticNode = RelAlgNode("arithmetic",
                            "relation out_field left_field right_field op_code")
AggNode = RelAlgNode("agg", "relation out_field field func groups")
AssignNode = RelAlgNode("assign", "var_name relation")


class Planner(nodes.NodeVisitor):
    def __init__(self, engine, groups={}):
        self.engine = engine
        self.groups = groups
        self._counter = 0

    def gensym(self):
        self._counter = self._counter + 1
        return "_gensym_{}".format(self._counter)

    def visit_var(self, node):
        if node.relation is None:
            definition = self.engine.get_definition(node.var_name)
            if not definition:
                relation = self.engine.get_relation(node.var_name)
                if not relation:
                    raise Exception("'{}' not found".format(node.var_name))
                return RelationNode(node.var_name)
            return definition
        else:
            return ProjectNode(node.relation, [node.var_name])

    def visit_int(self, node):
        return ConstantNode(self.gensym(), node.value)

    def visit_relation(self, node):
        return RelationNode(node.name)

    def visit_binary_op(self, node):
        op_code, left, right = node
        left_is_relation = isinstance(left, RelationNode)
        right_is_relation = isinstance(right, RelationNode)
        assert not (left_is_relation and right_is_relation)

        if (not left_is_relation) and (not right_is_relation):
            left_fields = list(ActiveFieldResolver(self.engine).visit(left))
            right_fields = list(ActiveFieldResolver(self.engine).visit(right))
            assert len(left_fields) == 1
            assert len(right_fields) == 1
            return ArithmeticNode(
                JoinNode(left, right, constants.JoinTypes.INNER,
                        constants.JoinSides.FULL),
                self.gensym(),
                left_fields[0],
                right_fields[0],
                op_code)
        elif left_is_relation or right_is_relation:
            active_field = None
            if not left_is_relation:
                active_field = list(ActiveFieldResolver(self.engine).visit(left))[0]
            if not right_is_relation:
                active_field = list(ActiveFieldResolver(self.engine).visit(right))[0]
            assert active_field is not None

            return ProjectNode(
                JoinNode(left, right, constants.JoinTypes.INNER,
                         constants.JoinSides.FULL),
                [active_field]
            )


    def visit_apply(self, node):
        func_name, func_arg = node
        assert len(list(ActiveFieldResolver(self.engine).visit(node.func_arg))) == 1, str(func_arg)
        return AggNode(func_arg,
                       self.gensym(),
                       list(ActiveFieldResolver(self.engine).visit(node.func_arg))[0],
                       constants.AggFuncs[func_name.upper()],
                       self.groups)

    def visit_assign(self, node):
        return AssignNode(node.var_name, node.expr)


class VariableSaver(nodes.NodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def visit_assign(self, node):
        self.engine.add_definition(node.var_name, node.expr)
        return None

    def default_visit(self, node):
        return node


class ActiveFieldResolver(nodes.NodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, node):
        return {node.field}

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        table = sa_relation.table_clause
        return set([c.name for c in table.c])

    def visit_project(self, node):
        return set(node.fields)

    def visit_rename(self, node):
        return node.relation

    def visit_join(self, node):
        return node.left.union(node.right)

    def visit_arithmetic(self, node):
        return {node.out_field}

    def visit_agg(self, node):
        return {node.out_field}


def plan(ast_node, engine, groups={}):
    return Planner(engine, groups).visit(ast_node)
