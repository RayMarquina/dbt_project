
from __future__ import print_function

import os
from dbt.templates import BaseCreateTemplate
from dbt.runners.runner import Runner
from dbt.compilation import Compiler

THREAD_LIMIT = 9

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self):
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        created_models, created_tests, created_analyses = compiler.compile()
        print("Compiled {} models, {} tests, and {} analyses".format(created_models, created_tests, created_analyses))

    def run(self):
        self.compile()

        runner = Runner(self.project, self.project['target-path'], BaseCreateTemplate.label)
        results = runner.run(self.args.models)

        total   = len(results)
        passed  = len([r for r in results if not r.errored and not r.skipped])
        errored = len([r for r in results if r.errored])
        skipped = len([r for r in results if r.skipped])


        print()
        print("Done. PASS={passed} ERROR={errored} SKIP={skipped} TOTAL={total}".format(total=total, passed=passed, errored=errored, skipped=skipped))
