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
