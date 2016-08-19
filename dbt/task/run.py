
from __future__ import print_function

import os
from dbt.templates import TestCreateTemplate, BaseCreateTemplate
from dbt.runners.runner import RunManager
from dbt.compilation import Compiler

THREAD_LIMIT = 9

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self, create_template):
        compiler = Compiler(self.project, create_template)
        compiler.initialize()
        created_models, created_tests, created_analyses = compiler.compile(self.args.dry)
        print("Compiled {} models, {} tests, and {} analyses".format(created_models, created_tests, created_analyses))

    def run(self):
        if self.args.dry:
            create_template = TestCreateTemplate
            run_mode = 'dry-run'
        else:
            create_template = BaseCreateTemplate
            run_mode = 'run'

        self.compile(create_template)

        runner = RunManager(self.project, self.project['target-path'], create_template.label)
        results = runner.run(run_mode, self.args.models)

        total   = len(results)
        passed  = len([r for r in results if not r.errored and not r.skipped])
        errored = len([r for r in results if r.errored])
        skipped = len([r for r in results if r.skipped])


        print()
        print("Done. PASS={passed} ERROR={errored} SKIP={skipped} TOTAL={total}".format(total=total, passed=passed, errored=errored, skipped=skipped))
