from gavl.parser.nodes import *
from gavl.parser.parser import parse, is_aggregate
from gavl.parser.visitors import GavlToRelAlg

def plan(ast_node, engine, groups={}):
    return GavlToRelAlg().visit(ast_node)
