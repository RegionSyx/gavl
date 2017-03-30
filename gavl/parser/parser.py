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

from gavl.constants import OpCodes
from gavl import nodes

from pyparsing import (Word, alphas, nums, oneOf, opAssoc, operatorPrecedence,
                       Suppress, Forward, delimitedList, Optional, Literal,
                       ZeroOrMore)

_graphviz_counter = 0


from gavl.parser.nodes import (ApplyNode, UnaryOpNode, BinaryOpNode, VarNode,
                               RelationNode, IntNode, AssignNode, BarOpNode,
                               BoolExprNode, BoolLiteral)

expr = Forward()

integer = Word(nums).setParseAction(lambda t: IntNode(int(t[0])))


def parseVariable(t):
    if len(t) == 1:
        return VarNode(t[0], relation=None)
    else:
        relation = RelationNode('.'.join(t[:-1]))
        return VarNode(t[-1], relation=relation)


variable = delimitedList(
    Word(alphas + "_"), delim=".").setParseAction(parseVariable)

barop = Forward()
func = (Word(alphas) + Suppress("(") + delimitedList(barop, delim=',') +
        Suppress(")"))
func.setParseAction(lambda t:
                    ApplyNode(t[0], t[1]) if len(t) == 2
                    else BinaryOpNode(OpCodes[t[0].upper()], t[1], t[2])
                    )

operand = func | integer | variable

signop = oneOf('+ -').setParseAction(
    lambda t: OpCodes.SIGN_POS if t[0] == "+" else OpCodes.SIGN_NEG)

multop = oneOf('* /').setParseAction(
    lambda t: OpCodes.MULT if t[0] == "*" else OpCodes.DIV)

plusop = oneOf('+ -').setParseAction(
    lambda t: OpCodes.ADD if t[0] == "+" else OpCodes.SUB)

expr << operatorPrecedence(operand, [
    (signop, 1, opAssoc.RIGHT),
    (multop, 2, opAssoc.LEFT),
    (plusop, 2, opAssoc.LEFT),
])

expr.setParseAction(lambda t: process_expr(t))


def process_stmt(t):
    if t[0] is None:
        return t[1]
    else:
        return AssignNode(t[0].var_name, t[1])

bool_false = Literal("False")
bool_false.setParseAction(lambda t: BoolLiteral(False))
bool_true = Literal("True")
bool_true.setParseAction(lambda t: BoolLiteral(True))
bool_atom = bool_true | bool_false | variable | integer

bool_op = oneOf('== <= >= < >')
bool_and = Literal('and')
bool_or = Literal('or')
bool_expr = operatorPrecedence(bool_atom, [
    (bool_op, 2, opAssoc.LEFT),
    (bool_and,  2, opAssoc.LEFT),
    (bool_or,  2, opAssoc.LEFT)
])
bool_expr.setParseAction(lambda t: process_bool_expr(t))

barop << expr + ZeroOrMore(Suppress("|") - bool_expr)
barop.setParseAction(lambda t: process_barop(t))

stmt = Optional(variable + Suppress("="), None) + barop
stmt.setParseAction(process_stmt)

def process_bool_expr(t):
    if isinstance(t, (VarNode, BoolLiteral, IntNode)):
        return t
    if len(t) == 1:
        return process_bool_expr(t[0])
    elif len(t) == 3:
        op = t[1]
        if op == "<=":
            op_code = OpCodes.LTE
        elif op == ">=":
            op_code = OpCodes.GTE
        elif op == "<":
            op_code = OpCodes.LT
        elif op == ">":
            op_code = OpCodes.GT
        elif op == "==":
            op_code = OpCodes.EQ
        elif op == "and":
            op_code = OpCodes.AND
        elif op == "or":
            op_code = OpCodes.OR
        else:
            op_code = None

        return BoolExprNode(op_code, process_bool_expr(t[0]),
                            process_bool_expr(t[2]))


def process_barop(t):
    if len(t) == 1:
        return t[0]
    elif len(t) == 2:
        return BarOpNode(t[0], t[1])
    elif len(t) > 2:
        bool_exprs = t[1:]
        return BarOpNode(process_barop(t[:-1]), t[-1])


def process_expr(e):
    if isinstance(e, (ApplyNode, IntNode, VarNode, RelationNode, BinaryOpNode)):
        return e
    if len(e) == 0:
        return None
    elif len(e) == 1:
        return process_expr(e[0])
    elif len(e) == 2:
        assert e[0] in OpCodes
        return UnaryOpNode(e[0], process_expr(e[1]))
    elif len(e) >= 3:
        assert e[1] in OpCodes, e[1]
        return BinaryOpNode(e[1], process_expr(e[0]), process_expr(e[2:]))


class IsAggregateResolver(nodes.NodeVisitor):
    def visit_apply(self, node):
        return True

    def visit_binary_op(self, node):
        return node.left or node.right

    def default_visit(self, node):
        return False


def parse(expression):
    ret = stmt.parseString(expression)[0]
    return ret


def is_aggregate(node):
    return IsAggregateResolver().visit(node)
