from test.integration.base import DBTIntegrationTest

class TestSimpleSeed(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)

        self.run_sql_file("test/integration/005_simple_seed/seed.sql")

    @property
    def schema(self):
        return "simple_seed_005"

    @property
    def models(self):
        return "test/integration/005_simple_seed/models"

    @property
    def project_config(self):
        return {
            "data-paths": ['test/integration/005_simple_seed/data']
        }

    def test_simple_seed(self):
        self.run_dbt(["seed"])
        self.assertTablesEqual("seed_actual","seed_expected")

        # this should truncate the seed_actual table, the re-insert
        self.run_dbt(["seed"])
        self.assertTablesEqual("seed_actual","seed_expected")


    def test_simple_seed_with_drop(self):
        self.run_dbt(["seed"])
        self.assertTablesEqual("seed_actual","seed_expected")

        # this should drop the seed table, then re-create
        self.run_dbt(["seed", "--drop-existing"])
        self.assertTablesEqual("seed_actual","seed_expected")
