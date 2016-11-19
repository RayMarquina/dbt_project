from test.integration.base import DBTIntegrationTest

class TestSimpleDependency(DBTIntegrationTest):

    dependency_master = 'https://github.com/fishtown-analytics/dbt-integration-project'
    dependency_branch = 'https://github.com/fishtown-analytics/dbt-integration-project@fun-branch'

    dependency = dependency_master

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/006_simple_dependency/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "test/integration/006_simple_dependency/models"

    @property
    def project_config(self):
        return {
            "repositories": [
                self.dependency
            ]
        }

    def test_simple_dependency(self):
        self.dependency = self.dependency_master

        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

        self.run_sql_file("test/integration/006_simple_dependency/update.sql")

        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

    def test_simple_dependency_branch(self):
        self.dependency = self.dependency_branch

        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")
