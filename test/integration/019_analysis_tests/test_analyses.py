from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest
import os


class TestAnalyses(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_analyses_019"

    @property
    def models(self):
        return "test/integration/019_analysis_tests/models"

    def analysis_path(self):
        return "test/integration/019_analysis_tests/analysis"

    @property
    def project_config(self):
        return {
            "analysis-paths": [self.analysis_path()]
        }

    @attr(type='postgres')
    def test_working_macros(self):

        self.run_dbt(["compile"])

        self.assertEqual(['analysis.sql'], os.listdir('target/build-analysis/test'))

        models = self.get_models_in_schema()
        self.assertTrue('my_model' in models.keys())
