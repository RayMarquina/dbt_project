import pprint
import os
import fnmatch
import jinja2
import yaml
import dbt.project
from collections import defaultdict

from functools import partial
import networkx as nx

CREATE_STATEMENT_TEMPLATE = """
create {table_or_view} {schema}.{identifier} {dist_qualifier} {sort_qualifier} as (
    {query}
);"""

class Linker(object):
    def __init__(self):
        self.graph = nx.DiGraph()

    def as_dependency_list(self):
        return nx.topological_sort(self.graph, reverse=True)

    def dependency(self, node1, node2):
        "indicate that node1 depends on node2"
        self.graph.add_node(node1)
        self.graph.add_node(node2)
        self.graph.add_edge(node1, node2)

    def add_node(self, node):
        self.graph.add_node(node)

    def write_graph(self, outfile):
        nx.write_yaml(self.graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_yaml(infile)

class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

        self.models = []
        self.linker = Linker()

    def __project_sources(self, project):
        """returns: {'model': ['pardot/model.sql', 'segment/model.sql']}
        """
        indexed_files = defaultdict(list)

        for source_path in project['source-paths']:
            full_source_path = os.path.join(project['project-root'], source_path)
            for root, dirs, files in os.walk(full_source_path):
                for filename in files:
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, full_source_path)

                    if fnmatch.fnmatch(filename, "*.sql"):
                        indexed_files[full_source_path].append(rel_path)

                        # TODO : include project name here!
                        model_group = os.path.dirname(rel_path)
                        model_name  = os.path.splitext(filename)[0]
                        model = (model_group, model_name)
                        if model not in self.models:
                            self.models.append(model)
                        else:
                            print("WARNING: Conflicting model found {}".format(abs_path))

        return indexed_files

    def __write(self, path, payload):
        target_path = os.path.join(self.project['target-path'], path)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))
        elif os.path.exists(target_path):
            print("Compiler overwrite of {}".format(target_path))

        with open(target_path, 'w') as f:
            f.write(payload)

    def __sort_qualifier(self, model_config):
        sort_keys = model_config['sort']
        if type(sort_keys) == str:
            sort_keys = [sort_keys]

        # remove existing quotes in field name, then wrap in quotes
        formatted_sort_keys = ['"{}"'.format(sort_key.replace('"', '')) for sort_key in sort_keys]
        return "sortkey ({})".format(', '.join(formatted_sort_keys))

    def __dist_qualifier(self, model_config):
        dist_key = model_config['dist']

        if type(dist_key) != str:
            raise RuntimeError("The provided distkey '{}' is not valid!".format(dist_key))

        return 'distkey ("{}")'.format(dist_key)

    def __wrap_in_create(self, model_name, query, model_config):

        # default to view if not provided in config!
        table_or_view = 'table' if model_config['materialized'] else 'view'

        ctx = self.project.context()
        schema = ctx['env'].get('schema', 'public')

        dist_qualifier = ""
        sort_qualifier = ""

        if table_or_view == 'table':
            if 'dist' in model_config:
                dist_qualifier = self.__dist_qualifier(model_config)
            if 'sort' in model_config:
                sort_qualifier = self.__sort_qualifier(model_config)

        opts = {
            "table_or_view": table_or_view,
            "schema": schema,
            "identifier": model_name,
            "query": query,
            "dist_qualifier": dist_qualifier,
            "sort_qualifier": sort_qualifier
        }

        return CREATE_STATEMENT_TEMPLATE.format(**opts)

    def __get_model_identifiers(self, model_filepath):
        model_group = os.path.dirname(model_filepath)
        model_name, _ = os.path.splitext(os.path.basename(model_filepath))
        return model_group, model_name

    def __get_model_config(self, model_group, model_name):
        """merges model, model group, and base configs together. Model config
        takes precedence, then model_group, then base config"""

        config = self.project['model-defaults'].copy()

        model_configs = self.project['models']
        model_group_config = model_configs.get(model_group, {})
        model_config = model_group_config.get(model_name, {})

        config.update(model_group_config)
        config.update(model_config)

        return config

    def __find_model_by_name(self, name):
        for model in self.models:
            model_group, model_name = model
            if model_name == name:
                return model
        raise RuntimeError("Can't find a model named '{}' -- does it exist?".format(name))

    def __ref(self, ctx, source_model):
        schema = ctx['env']['schema']

        # if this node doesn't have any deps, still make sure it's a part of the graph
        self.linker.add_node(source_model)

        def do_ref(other_model_name):
            other_model = self.__find_model_by_name(other_model_name)
            self.linker.dependency(source_model, other_model)
            return '"{}"."{}"'.format(schema, other_model_name)

        return do_ref

    def __compile(self, src_index):
        for src_path, files in src_index.items():
            jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=src_path))
            for f in files:

                model_group, model_name = self.__get_model_identifiers(f)
                model_config = self.__get_model_config(model_group, model_name)

                if not model_config.get('enabled'):
                    continue

                template = jinja.get_template(f)

                context = self.project.context()
                source_model = (model_group, model_name)
                context['ref'] = self.__ref(context, source_model)

                rendered = template.render(context)

                create_stmt = self.__wrap_in_create(model_name, rendered, model_config)

                if create_stmt:
                    self.__write(f, create_stmt)

    def run(self):
        sources = self.__project_sources(self.project)

        for obj in os.listdir(self.project['modules-path']):
            full_obj = os.path.join(self.project['modules-path'], obj)
            if os.path.isdir(full_obj):
                project = dbt.project.read_project(os.path.join(full_obj, 'dbt_project.yml'))
                sources.update(self.__project_sources(project))

        self.__compile(sources)

        graph_path = os.path.join(self.project['target-path'], 'graph.yml')
        self.linker.write_graph(graph_path)
