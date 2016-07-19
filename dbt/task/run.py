
import os
from dbt.templates import BaseCreateTemplate
from dbt.runner import Runner

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        runner = Runner(self.project, self.project['target-path'], BaseCreateTemplate.label)
        for (model, passed) in runner.run(self.args.models):
            pass
