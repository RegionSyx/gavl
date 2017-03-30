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


def create_sa_db(conn_string, echo=False):
    db = sa.create_engine(conn_string, connect_args={'sslmode': 'prefer'},
                          echo=echo)
    return db


class Relation(object):
    pass

class Attribute(object):
    pass


class SAAttribute(Attribute):
    def __init__(self, parent, sa_column):
        self.parent = parent
        self.sa_column = sa_column

    def get_sa_column(self):
        return self.sa_column


class SARelation(Relation):
    def __init__(self, db, table, schema='public'):
        self.db = db
        self.schema = schema
        self.attributes = {}

        self.table_clause = table.__table__.alias()

        for attr in self.table_clause.c:
            self.add_attribute(
                attr.key,
                SAAttribute(self, attr)
            )

    def __getattr__(self, name):
        return self.attributes[name]

    def add_attribute(self, name, attr):
        self.attributes[name] = attr




class Engine(object):
    def __init__(self, db):
        self.relations = {}
        self.links = []
        self.symbol_table = {}
        self.db = db

    def get_relation(self, name, default=None):
        return self.relations.get(name, default)

    def add_relation(self, name, relation):
        self.relations[name] = relation
        return relation

    def get_symbol(self, name, default=None):
        return self.symbol_table.get(name, default)

    def add_symbol(self, name, symbol):
        self.symbol_table[name] = symbol

    def link(self, a, b):
        self.links.append((a, b))

    def find_links_between(self, from_relation, to_relation):
        result = []
        for a, b in self.links:
            if a.parent == from_relation and b.parent == to_relation:
                result.append((a, b))
        return result

    def query(self, query, groupby=[]):
        groupby = [tuple(x.split('.')) for x in groupby]
        root_ast = gavl.parse(query)
        root_ast = VariableSaver(self).visit(root_ast)

        root_relalg = gavl.plan(root_ast)
        root_relalg = VariableReplacer(self).visit(root_relalg)

        root_plan = QueryPlanner(self).visit(root_relalg)
        result = QueryExecutor(self, groupby).visit(root_plan)

        active_field = list(ActiveFieldResolver().visit(root_relalg))

        result.rename(columns={active_field[0]: "result"}, inplace=True)

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

SAQuery = PlanNode('sa_query', 'query db')
PandasArith = PlanNode('pandas_arith', 'df out_field left_col right_col op_code')
PandasMerge = PlanNode('pandas_merge', 'left right')


class DataSource(object):
    pass

class SADataSource(DataSource):
    pass

class SASelectBuilder(PostNodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, node):
        return [sa.sql.expression.literal_column(str(node.value))
                .label(node.field)]

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        return sa_relation.table_clause.columns

    def visit_project(self, node):
        projected = [c for c in node.relation if c.name in node.fields]
        assert len(projected) == len(node.fields), node
        assert len(projected) > 0
        return projected

    def visit_join(self, node):
        return list(node.left) + list(node.right)
        selects = [c for c in node.left]
        for c in node.right:
            if c.name not in [x.name for x in selects]:
                selects.append(c)

        return selects

    def visit_arithmetic(self, node):
        left_field = [c for c in node.relation if c.name == node.left_field]
        right_field = [c for c in node.relation if c.name == node.right_field]
        assert len(left_field) == 1, left_field
        assert len(right_field) == 1, right_field

        f = constants.PYTHON_OPERATORS[node.op_code]

        return [f(left_field[0], right_field[0]).label(node.out_field)]

    def visit_agg(self, node):
        if node.func.name == "UNIQUE":
            agg_func = lambda x: sa.func.COUNT(sa.distinct(x))
        else:
            agg_func = getattr(sa.func, node.func.name)

        agg_col = [c for c in node.relation if c.name == node.field]
        assert len(agg_col) == 1, str(agg_col)
        agg_col = agg_col[0]

        return [agg_func(agg_col).label(node.out_field)]

    def visit_select(self, node):
        return node.relation

    def visit_bool_op(self, node):
        assert len(node.left) == 1, node
        assert len(node.right) == 1, node
        if node.op_code == constants.OpCodes.AND:
            f = sa.and_
        elif node.op_code == constants.OpCodes.OR:
            f = sa.or_
        else:
            f = constants.PYTHON_OPERATORS[node.op_code]
        return [f(node.left[0], node.right[0])]

    def visit_bool_constant(self, node):
        return [sa.sql.expression.literal(node.value)]


class SAFromBuilder(PostNodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, node):
        return []

    def visit_relation(self, node):
        sa_relation = self.engine.get_relation(node.name)
        return [sa_relation]

    def visit_join(self, node):
        return list(set(node.left).union(set(node.right)))

    def visit_select(self, node):
        return node.relation

    def visit_project(self, node):
        return node.relation

    def visit_rename(self, node):
        return node.relation

    def visit_arithmetic(self, node):
        return node.relation

    def visit_agg(self, node):
        return node.relation


class SAWhereBuilder(PostNodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_constant(self, node):
        return []

    def visit_relation(self, node):
        return []

    def visit_project(self, node):
        return node.relation

    def visit_join(self, node):
        return node.left + node.right

    def visit_select(self, node):
        return node.relation + [node.bool_expr]

    def visit_rename(self, node):
        return node.relation

    def visit_arithmetic(self, node):
        return node.relation

    def visit_agg(self, node):
        return node.relation

class SABoolBuilder(PreNodeVisitor):

    def __init__(self, engine):
        self.engine = engine

    def visit_select(self, node):
        bool_expr = SASelectBuilder(self.engine).visit(node.bool_expr)[0]
        return relalg.SelectNode(
            node.relation,
            bool_expr
        )

class PandasBuilder(PostNodeVisitor):
    pass

class DataSourceFinder(PostNodeVisitor):
    pass

class QueryPlanner(PreNodeVisitor):
    def __init__(self, engine):
        self.engine = engine

    def visit_arithmetic(self, node):
        if isinstance(node.relation, relalg.JoinNode):
            if (isinstance(node.relation.left, relalg.AggNode) and
                isinstance(node.relation.right, relalg.AggNode)):
                return PandasArith(
                    PandasMerge(
                        node.relation.left,
                        node.relation.right),
                    node.out_field, node.left_field, node.right_field, node.op_code
                )

        return self.default_visit(node)

    def visit_pandas_arith(self, node):
        return node

    def visit_pandas_merge(self, node):
        return node

    def default_visit(self, node):
        # Shortcut for now
        selects = SASelectBuilder(self.engine).visit(node)
        froms = SAFromBuilder(self.engine).visit(node)
        wheres = SAWhereBuilder(self.engine).visit(
            SABoolBuilder(self.engine).visit(node)
        )
        query = sa.select(selects)
        first_from = froms[0]

        nodes = {}
        for f in froms:
            for g in froms:
                nodes.setdefault(f, [])
                nodes[f].extend([x[1] for x in
                                 self.engine.find_links_between(f, g)])
        relations = []
        def _visit(node, visited=[]):
            if node in visited:
                raise Exception("Circular Dependency")
            if node not in relations:
                for r in nodes:
                    edges = self.engine.find_links_between(node, r)
                    if edges:
                        _visit(r, visited + [node])
                relations.insert(0, node)

        for n in nodes:
            if n not in relations:
                _visit(n)

        joins = relations[0].table_clause
        for f in relations[1:]:
            links = []
            for x in relations:
                links = self.engine.find_links_between(x, f)
                if links:
                    break
            columns = [(a.get_sa_column(), b.get_sa_column())
                       for a, b in links]
            if columns:
                join_cond = functools.reduce(sa.and_,
                                             [a == b for a, b in columns])
            else:
                join_cond = sa.sql.expression.literal(True)

            joins = joins.join(f.table_clause, join_cond)

        query = sa.select(selects).select_from(joins)
        for where in wheres:
            query = query.where(where)
        return SAQuery(query, self.engine.db)

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
    """
    In: RelAlg node
    Out: Pandas Dataframes
    """

    def __init__(self, engine, group_by={}):
        self.engine = engine
        self.group_by = group_by

    def visit_sa_query(self, node):
        query = node.query
        for k, v in self.group_by:
            rel = self.engine.get_relation(k)
            col = getattr(rel.table_clause.c, v)
            query = query.column(col)
            query = query.group_by(col).order_by(col)

        connection = node.db.connect()
        result = pd.read_sql_query(query, connection)
        connection.close()
        return result

    def visit_pandas_merge(self, node):
        if len(self.group_by) == 0:
            return pd.merge(node.left, node.right, left_index=True,
                            right_index=True)
        else:
            return pd.merge(node.left, node.right)

    def visit_pandas_arith(self, node):
        f = constants.PYTHON_OPERATORS[node.op_code]

        result = node.df.copy()
        result[node.out_field] = f(node.df[node.left_col],node.df[node.right_col])
        group_cols = [v for k, v in self.group_by] + [node.out_field]
        result = result[group_cols]
        return result

    def visit_pandas(self, node):
        pass
