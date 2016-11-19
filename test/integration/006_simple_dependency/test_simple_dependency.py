from test.integration.base import DBTIntegrationTest

class TestSimpleDependency(DBTIntegrationTest):

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
                'https://github.com/fishtown-analytics/dbt-integration-project'
            ]
        }

    def deps_run_assert_equality(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

    def test_simple_dependency(self):
        self.deps_run_assert_equality()

        self.run_sql_file("test/integration/006_simple_dependency/update.sql")

        self.deps_run_assert_equality()


class TestSimpleDependencyBranch(DBTIntegrationTest):

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
                'https://github.com/fishtown-analytics/dbt-integration-project@master'
            ]
        }

    def deps_run_assert_equality(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")

    def test_simple_dependency(self):
        self.deps_run_assert_equality()

        self.run_sql_file("test/integration/006_simple_dependency/update.sql")

        self.deps_run_assert_equality()
