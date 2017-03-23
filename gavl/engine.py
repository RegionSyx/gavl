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
from gavl import relalg, constants
from gavl.nodes import PreNodeVisitor, PostNodeVisitor, Node
from gavl.parser.visitors import ActiveFieldResolver
import pandas as pd
from pandas.core.common import is_timedelta64_dtype
import numpy as np
import sqlalchemy as sa

SUPPORTED_FILTER_OPERATORS = {
    "==": constants.OpCodes.EQ,
    "<": constants.OpCodes.LT,
    "<=": constants.OpCodes.LTE,
    ">": constants.OpCodes.GT,
    ">=": constants.OpCodes.GTE,
}


def create_sa_db(conn_string):
    db = sa.create_engine(conn_string, connect_args={'sslmode': 'prefer'},
                          echo=True)
    return db


class Relation(object):
    pass


class SARelation(Relation):
    def __init__(self, db, table, variables, schema='public'):
        self.db = db
        self.variables = variables
        self.schema = schema

        self.table_clause = table.__table__

    def execute(self, filters=[]):
        df = pd.read_sql_table(
            self.table, self.db.connect(), schema=self.schema)

        df.rename(columns=self.variables, inplace=True)

        for f in filters:
            if f["attr"] in df:
                df = df[constants.PYTHON_OPERATORS[SUPPORTED_FILTER_OPERATORS[f[
                    "oper"]]](df[f["attr"]], f["value"])]

        df['global'] = 0
        df.set_index(['global'] + list(self.variables.values()), inplace=True)
        return df


class CSVRelation(Relation):
    def __init__(self, csv_file, variables):
        self.csv_file = csv_file
        self.variables = variables

    def execute(self):
        df = pd.read_csv(self.csv_file)

        df.rename(columns=self.variables, inplace=True)
        df['global'] = 0
        df.set_index(['global'] + list(self.variables.values()), inplace=True)
        return df


class Engine(object):
    def __init__(self, db):
        self.relations = {}
        self.symbol_table = {}
        self.db = db

    def get_relation(self, name, default=None):
        return self.relations.get(name, default)

    def add_relation(self, name, relation):
        self.relations[name] = relation

    def get_symbol(self, name, default=None):
        return self.symbol_table.get(name, default)

    def add_symbol(self, name, symbol):
        self.symbol_table[name] = symbol

    def query(self, query, groupby={}, filters=[]):
        root_ast = gavl.parse(query)
        root_ast = VariableSaver(self).visit(root_ast)

        root_relalg = gavl.plan(root_ast)
        root_relalg = VariableReplacer(self).visit(root_relalg)

        root_plan = QueryPlanner(self).visit(root_relalg)
        result = QueryExecutor().visit(root_plan)

        print(root_ast)
        print(root_relalg)
        print(root_plan)
        active_field = list(ActiveFieldResolver().visit(root_relalg))

        result.rename(columns={active_field[0]: "result"}, inplace=True)
        print(active_field, result)

        return result


class VariableSaver(PreNodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_assign(self, node):
        var_name, relation = node
        relalg = gavl.plan(relation)
        self.engine.add_symbol(var_name, relalg)
        return node


class VariableReplacer(PostNodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_relation(self, node):
        symbol = self.engine.get_symbol(node.name)
        if symbol:
            compiled_relalg = VariableReplacer(self.engine).visit(relation)
            return compiled_relalg

        relation = self.engine.get_relation(node.name)
        if relation:
            return relalg.RelationNode(node.name)

        raise Exception("Cannot find relation or symbol "
                            "'{}'".format(node.name))


PlanNode = Node

SAQuery = PlanNode('sa_query', 'query conn')
Pandas = PlanNode('pandas', 'selects filters groups sorts')


class DataSource(object):
    pass

class SADataSource(DataSource):
    pass

class SAQueryBuilder(PostNodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, node):
        return sa.select([sa.sql.expression.literal(node.value)])

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        if sa_relation is not None:
            return sa_relation.table_clause
        else:
            raise Exception("Relation not found: {}".format(node.name))

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


class PandasBuilder(PostNodeVisitor):
    pass

class DataSourceFinder(PostNodeVisitor):
    pass

class QueryPlanner(PreNodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def default_visit(self, node):
        # Shortcut for now
        return SAQuery(SAQueryBuilder(self.engine).visit(node),
                       self.engine.db.connect())

        sources = DataSourceFinder().visit(node)

        if len(sources) > 1:
            return PandasBuilder().visit(node)
        elif len(sources) == 1:
            source = source[0]
            if isinstance(source, SADataStore):
                return SAQuery(SAQueryBuilder(self.engine).visit(node))
            else:
                return PandasBuilder().visit(node)
        else:
            return node

class QueryExecutor(PostNodeVisitor):

    def visit_sa_query(self, node):
        query = sa.select([node.query])

        result = pd.read_sql_query(query, node.conn)
        return result


    def visit_pandas(self, node):
        pass
