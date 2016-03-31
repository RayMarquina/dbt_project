import pprint
import os
import fnmatch
import jinja2
import yaml
from collections import defaultdict

default_model_config = {
    "materialized": False,
    "enabled": True,
}

class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

        self.model_configs = {}

    def __load_model_config(self, config_path):
        # directory containing the config file
        model_path = os.path.dirname(config_path)

        if model_path not in self.model_configs and os.path.exists(config_path):
            with open(config_path, 'r') as config_fh:
                model_config = yaml.safe_load(config_fh)

                config = default_model_config.copy()
                config.update(model_config)

                self.model_configs[model_path] = config

    def __src_index(self):
        """returns: {'model': ['pardot/model.sql', 'segment/model.sql']}
        """
        indexed_files = defaultdict(list)

        for source_path in self.project['source-paths']:
            for root, dirs, files in os.walk(source_path):
                for filename in files:
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, source_path)

                    if fnmatch.fnmatch(filename, "*.sql"):
                        indexed_files[source_path].append(rel_path)

                    elif filename == 'config.yml':
                        self.__load_model_config(abs_path)

        return indexed_files

    def __write(self, path, payload):
        target_path = os.path.join(self.project['target-path'], path)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))
        elif os.path.exists(target_path):
            print "Compiler overwrite of {}".format(target_path)

        with open(target_path, 'w') as f:
            f.write(payload)

    def __wrap_in_create(self, path, query, model_config):
        filename = os.path.basename(path)
        identifier, ext = os.path.splitext(filename)

        # default to view if not provided in config!
        table_or_view = 'table' if model_config['materialized'] else 'view'

        ctx = self.project.context()
        schema = ctx['env']['schema']

        create_template = "create {table_or_view} {schema}.{identifier} as ( {query} );"

        opts = {
            "table_or_view": table_or_view,
            "schema": schema,
            "identifier": identifier,
            "query": query
        }

        return create_template.format(**opts)


    def __get_sql_file_config(self, src_path, f):
        model_path = os.path.join(src_path, os.path.dirname(f))
        config = self.model_configs.get(model_path, default_model_config)
        identifier, ext = os.path.splitext(os.path.basename(f))
        model_config = config.copy()

        if identifier in model_config:
            model_config.update(config[identifier])

        return model_config

    def __compile(self, src_index):
        for src_path, files in src_index.iteritems():
            jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=src_path))
            for f in files:
                template = jinja.get_template(f)
                rendered = template.render(self.project.context())

                model_config = self.__get_sql_file_config(src_path, f)

                if not model_config['enabled']:
                    continue

                create_stmt = self.__wrap_in_create(f, rendered, model_config)

                if create_stmt:
                    self.__write(f, create_stmt)

    def run(self):
        src_index = self.__src_index()
        self.__compile(src_index)
