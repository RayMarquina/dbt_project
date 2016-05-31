
import os
import psycopg2

from dbt.compilation import Compiler
from dbt.templates import TestCreateTemplate
from dbt.runner import Runner

class TestTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def get_target(self):
        return os.path.join(self.project['target-path'], TestCreateTemplate.label)

    def compile(self):
        compiler = Compiler(self.project, TestCreateTemplate)
        compiler.initialize()
        compiler.compile()

    def execute(self):
        target_path = self.get_target()
        runner = Runner(self.project, target_path, TestCreateTemplate.label)
        runner.run()

    def run(self):
        self.compile()

        try:
            self.execute()
        except psycopg2.ProgrammingError as e:
            print("")
            print("Error encountered while trying to execute tests")
            print("Model: {}".format(".".join(e.model)))
            print(e.message)
            print("Exiting.")
