from mock import MagicMock, patch, PropertyMock
import unittest

import dbt.model
import dbt.project
import dbt.templates
import dbt.utils

import networkx as nx

import dbt.compilation

from dbt.logger import GLOBAL_LOGGER as logger

class GraphTest(unittest.TestCase):
    def setUp(self):
        def mock_write_yaml(graph, outfile):
            self.graph_result = graph

        nx.write_yaml = mock_write_yaml
        self.graph_result = None

        self.profiles = {
            'test': {
                'outputs': {
                    'test': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': 'dbt_test'
                    }
                },
                'target': 'test'
            }
        }

        self.project = dbt.project.Project(
            cfg={
                'name': 'test_models_compile',
                'version': '0.1',
                'profile': 'test',
                'project-root': '/fake',
            },
            profiles=self.profiles,
            profiles_dir=None)

        self.project.validate()

        self.compiler = dbt.compilation.Compiler(
            self.project,
            dbt.templates.BaseCreateTemplate,
            {})

        self.compiler.get_macros = MagicMock(return_value=[])

        dbt.utils.dependency_projects = MagicMock(return_value=[])


    def use_models(self, models):
        dbt.clients.system.find_matching = MagicMock(
            return_value=[{'searched_path': 'models',
                           'absolute_path': '/fake/models/{}.sql'.format(k),
                           'relative_path': '{}.sql'.format(k)}
                          for k, v in models.items()])

        def mock_load_file_contents(path):
            k = path.split('/')[-1].split('.')[0]
            return models[k]

        dbt.clients.system.load_file_contents = MagicMock(
            side_effect=mock_load_file_contents)


    def test_single_model(self):
        self.use_models({
            'model_one': 'select * from events',
        })

        self.compiler.compile(limit_to=['models'])

        self.assertEquals(
            self.graph_result.nodes(),
            [('test_models_compile', 'model_one')])

        self.assertEquals(
            self.graph_result.edges(), [])

    def test_two_models_simple_ref(self):
        self.use_models({
            'model_one': 'select * from events',
            'model_two': "select * from {{ref('model_one')}}",
        })

        self.compiler.compile(limit_to=['models'])

        self.assertEquals(
            self.graph_result.nodes(),
            [('test_models_compile', 'model_one'),
             ('test_models_compile', 'model_two'),])

        self.assertEquals(
            self.graph_result.edges(),
            [(('test_models_compile', 'model_one'),
              ('test_models_compile', 'model_two')),])
