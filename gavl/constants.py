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

import enum
import operator


class OpCodes(enum.Enum):
    ADD = 1
    SUB = 2
    MULT = 3
    DIV = 4
    SIGN_POS = 5
    SIGN_NEG = 6
    LT = 7
    LTE = 8
    GT = 9
    GTE = 10
    EQ = 11


class JoinSides(enum.Enum):
    LEFT = 1
    RIGHT = 2
    FULL = 3


class JoinTypes(enum.Enum):
    INNER = 1
    OUTER = 2


class AggFuncs(enum.Enum):
    SUM = 1
    AVG = 2
    MAX = 3
    MIN = 4
    MEDIAN = 5
    COUNT = 6
    UNIQUE = 7


PYTHON_OPERATORS = {
    OpCodes.ADD: operator.add,
    OpCodes.SUB: operator.sub,
    OpCodes.MULT: operator.mul,
    OpCodes.DIV: operator.truediv,
    OpCodes.LT: operator.lt,
    OpCodes.LTE: operator.le,
    OpCodes.GT: operator.gt,
    OpCodes.GTE: operator.ge,
    OpCodes.EQ: operator.eq,
}
