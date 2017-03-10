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
from gavl import planner, nodes, engine, constants

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
    query = SQLCompiler(engine).visit(query)

    froms = [j for j in query.joins if j[1] is None]
    joins = [j for j in query.joins if j[1] is not None]

    date_table = engine.get_relation("date").table_clause

    group_selects = [date_table.c.day_date.label(k) for k in query.groups]

    sa_query = sa.select(group_selects +
                         [x.label("result_{}".format(i)) for i, x in
                          enumerate(query.fields)])
    assert len(froms) == 1, str(sa_query)
    from_clause = froms[0][0]
    for j in joins:
        from_clause = from_clause.join(*j)

    sa_query = sa_query.select_from(from_clause)

    for f in filters:
        table, column = f["attr"].split(".")
        rel = engine.get_relation(table)
        assert rel is not None

        def oper(x, y):
            return constants.PYTHON_OPERATORS[
                SUPPORTED_FILTER_OPERATORS[f["oper"]]
            ](x, y)

        sa_query = sa_query.where(
            oper(getattr(rel.table_clause.c, column),
                 f["value"]))

    for group in query.groups:
        assert group == "date"
        sa_query = sa_query.group_by(
            engine.get_relation(group).table_clause.c.day_date
        )
        sa_query = sa_query.order_by(
            engine.get_relation(group).table_clause.c.day_date
        )


    result = pd.read_sql_query(sa_query, engine.db.connect())
#    out_field = list(ActiveFieldResolver(engine).visit(node))[0]
    assert "result_0" in result
    result.rename(columns={"result_0": "result"}, inplace=True)

    return result


class RetrieveRelations(nodes.NodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_relation(self, node):
        table = self.engine.get_relation(node.name)
        if not table or not isinstance(table, engine.SARelation):
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
        return node.relation


class SQLCompiler(nodes.NodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, node):
        return SQLNode([sa.sql.expression.literal(node.value)], [], {})

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        return SQLNode([c for c in sa_relation.table_clause.c],
                       [(sa_relation.table_clause, None)], {})

    def visit_project(self, node):
    #    assert len(node.relation.fields) == 1, str(node)
    #    assert hasattr(node.relation.fields[0], 'c'), str(node)
        return SQLNode(fields=[c for c in node.relation.fields if c.name
                               in node.fields], joins=node.relation.joins,
                       groups=node.relation.groups)

    def visit_select(self, node):
        return SQLNode(node.fields, node.joins, node.groups, node.filters +
                       [node.cond])

    def visit_rename(self, node):
        return node

    def pre_visit_join(self, node):
        left_rels = RetrieveRelations(self.engine).visit(node.left)
        right_rels = RetrieveRelations(self.engine).visit(node.right)
        return node, {"left_rels": left_rels, "right_rels": right_rels}

    def visit_join(self, node, left_rels, right_rels):
        joins = node.left.joins
        result = node.left
        diff_rels = right_rels - left_rels

        left_variable_cols = {}
        for rel in left_rels:
            for k, v in rel.variables.items():
                if v not in left_variable_cols:
                    left_variable_cols[v] = getattr(rel.table_clause.c, k)

        for rel in diff_rels:
            join_cols = []
            for k, v in rel.variables.items():
                if v not in left_variable_cols:
                    pass
                    #raise Exception("{} not found in other joins".format(v))
                else:
                    join_cols.append(
                        left_variable_cols[v] == getattr(rel.table_clause.c, k)
                    )

            if len(join_cols) > 0:
                joins.append((rel.table_clause, functools.reduce(sa.and_,
                                                          join_cols)))
                    #result = result.join(table, join_cols[0])

        groups = node.left.groups.copy()
        groups.update(node.right.groups)

        return SQLNode(node.left.fields + node.right.fields, joins,
                       groups)

    def visit_arithmetic(self, node):

        def _visit(left, right):
            f = constants.PYTHON_OPERATORS[node.op_code]
            return f(left, right)

        return SQLNode([(functools.reduce(_visit,
                                          node.relation.fields)).label(node.out_field)],
                       node.relation.joins, node.relation.groups)
#        return result

    def visit_agg(self, node):
        if node.func.name == "UNIQUE":
            agg_func = lambda x: sa.func.COUNT(sa.distinct(x))
        else:
            agg_func = getattr(sa.func, node.func.name)

        if len(node.relation.joins) <= 1:
            return SQLNode([agg_func(node.relation.fields[0])],
                        node.relation.joins, node.groups)

        agg_col = [c for c in node.relation.fields if c.name == node.field]
        assert len(agg_col) == 1, str(agg_col)
        agg_col = agg_col[0]

        froms = [j for j in node.relation.joins if j[1] is None]
        joins = [j for j in node.relation.joins if j[1] is not None]

        from_clause = froms[0][0]
        for j in joins:
            from_clause = from_clause.join(*j)

        sub_query = sa.select([agg_func(agg_col)]).select_from(from_clause).as_scalar()

        return SQLNode(
            [sub_query],
            joins=[],
            groups=node.relation.groups
        )


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
