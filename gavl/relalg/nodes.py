
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

from gavl import nodes

RelAlgNode = nodes.Node

ConstantNode = RelAlgNode("constant", "field value")
RelationNode = RelAlgNode("relation", "name")
ProjectNode = RelAlgNode("project", "relation fields")
SelectNode = RelAlgNode("select", "relation bool_expr")
RenameNode = RelAlgNode("rename", "relation old_name new_name")
JoinNode = RelAlgNode("join", "left, right, join_type, join_side")
ArithmeticNode = RelAlgNode("arithmetic",
                            "relation out_field left_field right_field op_code")
AggNode = RelAlgNode("agg", "relation out_field field func groups")
AssignNode = RelAlgNode("assign", "var_name relation")
BoolOpNode = RelAlgNode("bool_op", "op_code left right")
BoolConstantNode = RelAlgNode("bool_constant", "value")
