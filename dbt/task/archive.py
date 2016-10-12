
from dbt.runner import RunManager
from dbt.templates import ArchiveInsertTemplate
from dbt.compilation import Compiler

class ArchiveTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project
        self.create_template = ArchiveInsertTemplate

    def compile(self):
        compiler = Compiler(self.project, self.create_template)
        compiler.initialize()
        compiler.compile_archives()

    def run(self):
        self.compile()
        runner = RunManager(self.project, self.project['target-path'], self.create_template.label)

        results = runner.run_archive()

