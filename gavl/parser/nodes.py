
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

import gavl.nodes

ASTNode = gavl.nodes.Node

ApplyNode = ASTNode("apply", "func_name func_arg")
UnaryOpNode = ASTNode("unary_op", "op_code expr")
BinaryOpNode = ASTNode("binary_op", "op_code left right")
VarNode = ASTNode("var", "var_name relation")
RelationNode = ASTNode("relation", "name")
IntNode = ASTNode("int", "value")
StrNode = ASTNode("string", "value")
AssignNode = ASTNode("assign", "var_name expr")
BarOpNode = ASTNode("barop", "expr bool")
BoolExprNode = ASTNode("bool_expr", "op_code left right")
BoolLiteral = ASTNode("bool_literal", "value")
