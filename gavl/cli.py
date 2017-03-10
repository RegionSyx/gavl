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

import pprint
import importlib
import click
import gavl
import cmd


class REPL(cmd.Cmd):
    def __init__(self, engine, source):
        self.engine = engine
        self.prompt = "gavl> "
        cmd.Cmd.__init__(self, stdin=source)

    def do_exit(self, line):
        return True

    def default(self, line):
        result = self.engine.query(line)
        if result is not None:
            click.echo(result)


@click.command()
@click.argument('gavl_file', type=click.File(), default=None, required=False)
@click.option('--interactive', default=False, is_flag=True)
@click.option('--settings', envvar="GAVL_SETTINGS", type=str, required=True)
def main(gavl_file, interactive, settings):

    module = importlib.import_module(settings)
    engine = getattr(module, 'ENGINE', None)
    if engine is None:
        click.echo("Must define ENGINE in settings file")
        return 2

    if gavl_file is not None:
        result = engine.query(gavl_file.read())
        if result is not None:
            click.echo(result)

    if gavl_file is None or interactive:
        repl = REPL(engine, gavl_file)
        repl.cmdloop()
