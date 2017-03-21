import pytest
from gavl import parse
from gavl.parser import nodes
from gavl.parser.nodes import (VarNode, BinaryOpNode, RelationNode, ApplyNode,
                               AssignNode, IntNode)
from gavl.constants import OpCodes


def test_simple_var():
    test = "foo.bar"
    expected = VarNode('bar', RelationNode(name='foo'))
    assert parse(test) == expected

def test_simple_arith():
    test = "A + B"
    expected = BinaryOpNode(
        OpCodes.ADD,
        VarNode("A", relation=None),
        VarNode("B", relation=None)
    )
    assert parse(test) == expected

def test_simple_agg():
    test = "SUM(foo)"
    expected = ApplyNode('SUM', VarNode('foo', None))
    assert parse(test) == expected

def test_simple_assign():
    test = "foo = bar"
    expected = AssignNode('foo', VarNode('bar', None))
    assert parse(test) == expected

def test_simple_constant():
    test = "1"
    expected = IntNode(1)
