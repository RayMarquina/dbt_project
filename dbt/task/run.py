
import os
from dbt.templates import BaseCreateTemplate
from dbt.runner import Runner

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def get_target(self):
        return os.path.join(self.project['target-path'], BaseCreateTemplate.label)

    def run(self):
        target_path = self.get_target()

        runner = Runner(self.project, target_path, BaseCreateTemplate.label)
        for (model, passed) in runner.run():
            pass
