
from __future__ import print_function

import os
from dbt.templates import BaseCreateTemplate
from dbt.runner import Runner
from dbt.compilation import Compiler

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self):
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        created_models, created_analyses = compiler.compile()
        print("Compiled {} models and {} analyses".format(created_models, created_analyses))

    def run(self):
        self.compile()

        runner = Runner(self.project, self.project['target-path'], BaseCreateTemplate.label)
        results = runner.run(self.args.models)

        successes = [r for r in results if not r.errored]
        errors = [r for r in results if r.errored]

        print()
        print("Done. Executed {} models executed with {} failures".format(len(successes) + len(errors), len(errors)))
