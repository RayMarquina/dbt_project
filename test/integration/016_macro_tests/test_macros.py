from test.integration.base import DBTIntegrationTest

class TestMacros(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/016_macro_tests/seed.sql")

    @property
    def schema(self):
        return "test_macros_016"

    @property
    def models(self):
        return "test/integration/016_macro_tests/models"

    @property
    def project_config(self):
        return {
            "macro-paths": ["test/integration/016_macro_tests/macros"],
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("expected_dep_macro","dep_macro")
        self.assertTablesEqual("expected_local_macro","local_macro")
