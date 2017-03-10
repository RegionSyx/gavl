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

import pytest
import gavl
import glob


fixtures = glob.glob("./tests/integration/fixtures/fixture.*.txt")


@pytest.mark.parametrize(
        'fixture',
        fixtures
        )
def test_fixture(fixture):
    with open(fixture, 'r') as f:
        lines = f.readlines()
        expressions = lines[0::3]
        expecteds = lines[1::3]
        for expression, expected in zip(expressions, expecteds):
            assert str(gavl.parse(expression.strip())) == expected.strip()
