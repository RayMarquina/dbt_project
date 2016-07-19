
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
        for (model, passed) in runner.run(self.args.models):
            pass
