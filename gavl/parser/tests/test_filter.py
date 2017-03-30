import pytest
from gavl import parse
from gavl.parser import nodes
from gavl.parser.nodes import (VarNode, BinaryOpNode, RelationNode, ApplyNode,
                               AssignNode, IntNode, BarOpNode, BoolLiteral,
                               BoolExprNode)
from gavl.parser.visitors import GavlToRelAlg
from gavl import relalg
from gavl.constants import OpCodes, JoinTypes, JoinSides


def test_simple_filter():
    test = "foo | True"
    expected = BarOpNode(VarNode('foo', None), BoolLiteral(True))
    assert parse(test) == expected

def test_equal_filter():
    test = "foo | True == True"
    expected = BarOpNode(
        VarNode('foo', None),
        BoolExprNode(OpCodes.EQ,
                     BoolLiteral(True),
                     BoolLiteral(True)
                     )
    )
    assert parse(test) == expected

def test_var_filter():
    test = "foo | foo.bar < baz"
    expected = BarOpNode(
        VarNode('foo', None),
        BoolExprNode(OpCodes.LT,
                     VarNode('bar', RelationNode('foo')),
                     VarNode('baz', None)
                     )
    )
    assert parse(test) == expected

def test_multiple_filter():
    test = "foo | foo.bar < baz | False"
    expected = BarOpNode(
        BarOpNode(
            VarNode('foo', None),
            BoolExprNode(OpCodes.LT,
                         VarNode('bar', RelationNode('foo')),
                         VarNode('baz', None)
                         )
        ),
        BoolLiteral(False),
    )
    assert parse(test) == expected

def test_simple_relalg_filter():
    test = BarOpNode(VarNode('foo', None), BoolLiteral(True))
    expected = relalg.SelectNode(relalg.RelationNode('foo'),
                                       relalg.BoolConstantNode(True))
    assert GavlToRelAlg().visit(test) == expected

def test_expr_relalg_filter():
    test = BarOpNode(
        VarNode('foo', None),
        BoolExprNode(OpCodes.LT,
                     VarNode('bar', RelationNode('foo')),
                     VarNode('baz', None)
                     )
    )
    expected = relalg.SelectNode(
        relalg.RelationNode('foo'),
        relalg.BoolOpNode(
            OpCodes.LT,
            relalg.ProjectNode(
                relalg.RelationNode('foo'),
                ['bar']
            ),
            relalg.RelationNode('baz')
        )
    )
    assert GavlToRelAlg().visit(test) == expected


def test_pushdown_relalg_filter():
    test = BarOpNode(
        BinaryOpNode(
            OpCodes.MULT,
            VarNode('foo', None),
            VarNode('buzz', None),
        ),
        BoolExprNode(OpCodes.LT,
                     VarNode('bar', RelationNode('foo')),
                     BoolLiteral(True)
                     )
    )
    expected = relalg.JoinNode(
        relalg.SelectNode(
            relalg.RelationNode('foo'),
            relalg.BoolOpNode(
                OpCodes.LT,
                relalg.ProjectNode(
                    relalg.RelationNode('foo'),
                    ['bar']
                ),
                relalg.BoolConstantNode(True)
            )
        ),
        relalg.RelationNode('buzz'),
        JoinTypes.INNER,
        JoinSides.FULL,
    )
    assert GavlToRelAlg().visit(test) == expected


@pytest.mark.skip(reason="Not Yet Implemented")
def test_and_pushdown_relalg_filter():
    test = BarOpNode(
        BinaryOpNode(
            OpCodes.ADD,
            VarNode('foo', None),
            VarNode('buzz', None),
        ),
        BoolExprNode(
            OpCodes.AND,
            BoolExprNode(
                OpCodes.EQ,
                VarNode('bar', RelationNode('foo')),
                BoolLiteral(True)),
            BoolExprNode(
                OpCodes.EQ,
                VarNode('baz', RelationNode('buzz')),
                BoolLiteral(True))
        )
    )
    expected = relalg.JoinNode(
        relalg.SelectNode(
            relalg.RelationNode('foo'),
            relalg.BoolOpNode(
                OpCodes.EQ,
                relalg.ProjectNode(
                    relalg.RelationNode('foo'),
                    ['bar']
                ),
                relalg.BoolConstantNode(True)
            )
        ),
        relalg.SelectNode(
            relalg.RelationNode('buzz'),
            relalg.BoolOpNode(
                OpCodes.EQ,
                relalg.ProjectNode(
                    relalg.RelationNode('buzz'),
                    ['baz']
                ),
                relalg.BoolConstantNode(True)
            )
        ),
        JoinTypes.INNER,
        JoinSides.FULL,
    )
    assert GavlToRelAlg().visit(test) == expected
