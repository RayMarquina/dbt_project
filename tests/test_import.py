import unittest

class ImportTest(unittest.TestCase):

    def test_import_dbt_project(self):
        import dbt.project

    def test_import_dbt_task_run(self):
        import dbt.task.run

    def test_import_dbt_task_compile(self):
        import dbt.task.compile

    def test_import_dbt_task_debug(self):
        import dbt.task.debug

    def test_import_dbt_task_clean(self):
        import dbt.task.clean

    def test_import_dbt_task_deps(self):
        import dbt.task.deps

    def test_import_dbt_task_init(self):
        import dbt.task.init

    def test_import_dbt_task_seed(self):
        import dbt.task.seed

    def test_import_dbt_task_test(self):
        import dbt.task.test

    def test_import_dbt_main(self):
        import dbt.main

    def test_import_dbt_project(self):
        import dbt.project

    def test_import_dbt_schema_tester(self):
        import dbt.schema_tester

    def test_import_dbt_source(self):
        import dbt.source

    def test_import_dbt_templates(self):
        import dbt.templates

    def test_import_dbt_compilation(self):
        import dbt.compilation

    def test_import_dbt_model(self):
        import dbt.model

    def test_import_dbt_runner(self):
        import dbt.runner

    def test_import_dbt_seeder(self):
        import dbt.seeder

    def test_import_dbt_targets(self):
        import dbt.targets

    def test_import_dbt_utils(self):
        import dbt.utils
