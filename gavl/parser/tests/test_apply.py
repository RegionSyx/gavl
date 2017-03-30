import pytest
import gavl
from gavl.parser.nodes import *
from gavl.constants import OpCodes


def test_simple_apply():
    test = "foo(1)"
    expected = ApplyNode('foo', IntNode(1))
    assert gavl.parse(test) == expected


def test_two_args_apply():
    test = "LEAST(1, 2)"
    expected = BinaryOpNode(OpCodes.LEAST, IntNode(1), IntNode(2))
    assert gavl.parse(test) == expected
