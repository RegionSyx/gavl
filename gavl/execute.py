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

import functools
import gavl
import pandas as pd
import sqlalchemy as sa
from gavl import relalg, nodes, constants

ExecutionNode = nodes.Node

SQLNode = ExecutionNode('sql', "fields joins groups")

SUPPORTED_FILTER_OPERATORS = {
    "==": constants.OpCodes.EQ,
    "<": constants.OpCodes.LT,
    "<=": constants.OpCodes.LTE,
    ">": constants.OpCodes.GT,
    ">=": constants.OpCodes.GTE,
}

def plan_execution(node, engine, filters=[]):
    query = node
    print(query)
    query = SQLCompiler(engine, filters).visit(query)
    print(query)


    result = pd.read_sql_query(query, engine.db.connect())
    out_field = list(ActiveFieldResolver(engine).visit(node))[0]
    assert out_field in result, out_field
    result.rename(columns={out_field: "result"}, inplace=True)

    return result


class RetrieveRelations(nodes.NodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_relation(self, node):
        table = self.engine.get_relation(node.name)
        if not table or not isinstance(table, gavl.SARelation):
            raise Exception("Could not find sql relation '{}'".format(node.name))
        return set([table])

    def visit_constant(self, node):
        return set()

    def visit_project(self, node):
        return node.relation

    def visit_rename(self, node):
        return node.relation

    def visit_join(self, node):
        return node.left | node.right

    def visit_arithmetic(self, node):
        return node.relation

    def visit_agg(self, node):
        return set()


class SQLCompiler(nodes.NodeVisitor):
    def __init__(self, engine, filters=[]):
        self.engine = engine
        self.filters = filters

    def visit_constant(self, node):
        return sa.select([sa.sql.expression.literal(node.value)])

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        return sa_relation.table_clause

    def visit_project(self, node):
        return sa.select([c for c in node.relation.columns if c.name
                               in node.fields]).select_from(node.relation)

    def visit_select(self, node):
        return node.relation.filter(node.cond)

    def visit_rename(self, node):
        return node.relation

    def visit_join(self, node):
        left_cols = [c for c in node.left.c]
        right_cols = [c for c in node.right.c]
        join_on = [
            l == r
            for l in left_cols
            for r in right_cols
            if l.name == r.name
        ]

        if join_on:
            join_cond = functools.reduce(sa.and_, join_on)
            return node.left.join(node.right, join_cond)
        else:
            raise Exception("Calculating a Cross Product")
            return sa.select([node.left, node.right])


    def visit_arithmetic(self, node):
        fields = [node.left_field, node.right_field]

        def _visit(left, right):
            f = constants.PYTHON_OPERATORS[node.op_code]
            return f(left, right)

        return sa.select([(
            functools.reduce(
                _visit,
                [getattr(node.relation.c, x) for x in fields])
        ).label(node.out_field)]).select_from(node.relation)

    def visit_agg(self, node):
        if node.func.name == "UNIQUE":
            agg_func = lambda x: sa.func.COUNT(sa.distinct(x))
        else:
            agg_func = getattr(sa.func, node.func.name)

        agg_col = [c for c in node.relation.columns if c.name == node.field]
        assert len(agg_col) == 1, str(agg_col)
        agg_col = agg_col[0]

        return (sa.select([agg_func(agg_col).label(node.out_field)])
                .select_from(node.relation))

class ActiveFieldResolver(nodes.NodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, ):
            return {node.field}

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        return set([c.name for c in sa_relation.table_clause.c])

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
