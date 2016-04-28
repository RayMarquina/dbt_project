import pprint
import os
import fnmatch
import jinja2
import yaml
import dbt.project
from collections import defaultdict
from functools import partial

NAMESPACE_DELIMITER = "."

CREATE_STATEMENT_TEMPLATE = """
create {table_or_view} {schema}.{identifier} {dist_qualifier} {sort_qualifier} as (
    {query}
);"""

class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

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

    def __wrap_in_create(self, path, query, model_config):
        filename = os.path.basename(path)
        identifier, ext = os.path.splitext(filename)

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
            "identifier": identifier,
            "query": query,
            "dist_qualifier": dist_qualifier,
            "sort_qualifier": sort_qualifier
        }

        return CREATE_STATEMENT_TEMPLATE.format(**opts)

    def __get_model_identifiers(self, model_filepath):
        namespace = os.path.dirname(model_filepath).split("/")
        model_name, _ = os.path.splitext(os.path.basename(model_filepath))
        return namespace, model_name

    def __get_model_config(self, namespace, model_name):
        """merges model, model group, and base configs together. Model config
        takes precedence, then namespace, then base config"""

        config = self.project['model-defaults'].copy()

        namespace_config = self.project['models']

        for item in namespace:
            namespace_config = namespace_config.get(item, {})
            config.update(namespace_config)

        model_config = namespace_config.get(model_name, {})
        config.update(model_config)

        return config

    def __include(self, calling_model, *args):
        print 'called from', calling_model, 'args:', args

    def __context(self, calling_model):
        ctx = self.project.context()
        ctx['resolve'] = partial(self.__include, calling_model)
        return ctx

    def __compile(self, src_index):
        for src_path, files in src_index.items():
            jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=src_path))
            for f in files:

                namespace, model_name = self.__get_model_identifiers(f)
                model_config = self.__get_model_config(namespace, model_name)

                if not model_config.get('enabled'):
                    continue

                qualified_name = namespace + [model_name]

                template = jinja.get_template(f)
                rendered = template.render(self.__context(qualified_name))

                create_stmt = self.__wrap_in_create(namespace, model_name, rendered, model_config)

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
