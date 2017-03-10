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

import gavl
from gavl import planner, constants
from gavl.execute import plan_execution
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


class Definition(object):
    pass


class Engine(object):
    def __init__(self, db):
        self._relations = {}
        self._definitions = {}
        self.db = db

    def get_relation(self, name, default=None):
        return self._relations.get(name, default)

    def get_definition(self, name, default=None):
        ret = self._definitions.get(name, default)
        return ret

    def add_relation(self, name, relation):
        self._relations[name] = relation

    def add_definition(self, name, definition):
        self._definitions[name] = definition

    def query(self, query, groupby={}, filters=[]):
        root_ast = gavl.parse(query)
        groups = {g: gavl.parse(g) for g in groupby}
        root_plan = gavl.plan(root_ast, self, groups)

        root_plan = planner.VariableSaver(self).visit(root_plan)
        if root_plan is None:
            return None

        result = plan_execution(root_plan, self, filters)
        if "global" in result:
            del result["global"]
        return result
