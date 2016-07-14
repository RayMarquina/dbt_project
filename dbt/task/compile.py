
from dbt.compilation import Compiler
from dbt.templates import BaseCreateTemplate


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        created_models, created_analyses = compiler.compile()

        print("Created {} models and {} analyses".format(created_models, created_analyses))
