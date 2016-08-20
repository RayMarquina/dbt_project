
from dbt.compilation import Compiler
from dbt.templates import BaseCreateTemplate, DryCreateTemplate


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        if self.args.dry:
            create_template = DryCreateTemplate
        else:
            create_template = BaseCreateTemplate

        compiler = Compiler(self.project, create_template)
        compiler.initialize()
        created_models, created_tests, created_analyses = compiler.compile(dry=self.args.dry)

        print("Compiled {} models, {} tests and {} analyses".format(created_models, created_tests, created_analyses))
