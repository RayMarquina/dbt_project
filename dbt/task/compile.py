
from dbt.compilation import Compiler
from dbt.templates import BaseCreateTemplate


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        created_models = compiler.compile()

        print("Created {} models".format(len(created_models)))
