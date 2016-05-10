
from dbt.compilation import Compiler, BaseCreateTemplate


class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        compiler.compile()

