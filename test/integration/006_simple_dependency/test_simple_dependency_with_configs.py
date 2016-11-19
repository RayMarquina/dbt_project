from test.integration.base import DBTIntegrationTest

class BaseTestSimpleDependencyWithConfigs(DBTIntegrationTest):

    def setUp(self):
        DBTIntegrationTest.setUp(self)
        self.run_sql_file("test/integration/006_simple_dependency/seed.sql")

    @property
    def schema(self):
        return "simple_dependency_006"

    @property
    def models(self):
        return "test/integration/006_simple_dependency/models"

class TestSimpleDependencyWithConfigs(BaseTestSimpleDependencyWithConfigs):

    @property
    def project_config(self):
        return {
            "models": {
                "DBT Integration Project": {
                    # no configs
                }

            },
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project@configs'
            ]
        }


    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual('seed_config_expected_1',"config")
        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")


class TestSimpleDependencyWithOverriddenConfigs(BaseTestSimpleDependencyWithConfigs):

    @property
    def project_config(self):
        return {
            "models": {
                # project-level configs
                "DBT Integration Project": {
                    "vars": {
                        "config_1": "abc",
                        "config_2": "def"

                    }
                }

            },
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project@configs'
            ]
        }


    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual('seed_config_expected_2',"config")
        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")



class TestSimpleDependencyWithModelSpecificOverriddenConfigs(BaseTestSimpleDependencyWithConfigs):

    @property
    def project_config(self):
        return {
            "models": {
                "DBT Integration Project": {
                    "config": {
                        # model-level configs
                        "vars": {
                            "config_1": "ghi",
                            "config_2": "jkl"

                        }
                    }
                }

            },
            "repositories": [
                'https://github.com/fishtown-analytics/dbt-integration-project@configs'
            ]
        }


    def test_simple_dependency(self):
        self.run_dbt(["deps"])
        self.run_dbt(["run"])

        self.assertTablesEqual('seed_config_expected_3',"config")
        self.assertTablesEqual("seed","table")
        self.assertTablesEqual("seed","view")
        self.assertTablesEqual("seed","incremental")


