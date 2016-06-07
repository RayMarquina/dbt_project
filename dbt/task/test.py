
import os, sys
import psycopg2
import yaml

from dbt.compilation import Compiler
from dbt.templates import TestCreateTemplate
from dbt.runner import Runner
from dbt.schema_tester import SchemaTester

class TestTask:
    """
    Testing:
        1) Create tmp views w/ 0 rows to ensure all tables, schemas, and SQL statements are valid
        2) Read schema files and validate that constraints are satisfied
           a) not null
           b) uniquenss
           c) referential integrity
    """
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def get_target(self):
        return os.path.join(self.project['target-path'], TestCreateTemplate.label)

    def compile(self):
        compiler = Compiler(self.project, TestCreateTemplate)
        compiler.initialize()
        compiler.compile()

        return compiler

    def execute(self):
        target_path = self.get_target()
        runner = Runner(self.project, target_path, TestCreateTemplate.label)
        executed_models = runner.run()

        # clean up
        print("Cleaning up.")
        runner.drop_models(executed_models)

        return executed_models

    def run_test_creates(self):

        try:
            results = self.execute()
        except psycopg2.ProgrammingError as e:
            print("")
            print("Error encountered while trying to execute tests")
            print("Model: {}".format(".".join(e.model)))
            print(e.message)
            print("Exiting.")
            sys.exit(1)
        print("")

        num_passed = len(results)
        print("{num_passed}/{num_passed} tests passed!".format(num_passed=num_passed))
        print("")

    def run_validations(self, compiler):
        print("Validating schemas")
        schema_tester = SchemaTester(self.project)
        schema_tester.test(compiler)


    def run(self):
        compiler = self.compile()

        if self.args.skip_test_creates:
            print("Skipping test creates (--skip-test-creates provided)")
        else:
            self.run_test_creates()

        if self.args.validate:
            self.run_validations(compiler)
        else:
            print("Skipping validations (--validate not provided)")

        print("Done!")
