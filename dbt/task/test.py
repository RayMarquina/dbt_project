
import os, sys
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

    def run(self):
        target_path = self.get_target()

        self.compile()
        runner = Runner(self.project, target_path, TestCreateTemplate.label)

        executed_models = []
        errored = False
        try:
            for model in runner.run():
                executed_models.append(model)
        except psycopg2.ProgrammingError as e:
            errored = True
            print("")
            print("Error encountered while trying to execute tests")
            print("Model: {}".format(".".join(e.model)))
            print(e.message)
        finally:
            runner.drop_models(executed_models)

        if errored:
            print("Exiting.")
            sys.exit(1)
        else:
            num_passed = len(executed_models)
            print("{num_passed}/{num_passed} tests passed!".format(num_passed=num_passed))

