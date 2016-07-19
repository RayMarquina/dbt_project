
from __future__ import print_function

import psycopg2
import os, sys

from dbt.compilation import Linker, Compiler
from dbt.templates import BaseCreateTemplate
from dbt.targets import RedshiftTarget
from dbt.source import Source
from dbt.utils import find_model_by_name

class Runner:
    def __init__(self, project, target_path, run_mode):
        self.project = project
        self.target_path = target_path
        self.run_mode = run_mode

    def get_compiled_models(self):
        return Source(self.project).get_compiled(self.target_path, self.run_mode)

    def get_target(self):
        target_cfg = self.project.run_environment()
        return RedshiftTarget(target_cfg)

    def deserialize_graph(self):
        linker = Linker()
        base_target_path = self.project['target-path']
        filename = 'graph-{}.yml'.format(self.run_mode)
        graph_file = os.path.join(base_target_path, filename)
        linker.read_graph(graph_file)

        return linker

    def create_schema(self):
        target_cfg = self.project.run_environment()
        target = self.get_target()
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(target_cfg['schema']))

    def query_for_existing(self, cursor, schema):
        sql = """
            select tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)

        cursor.execute(sql)
        existing = [(name, relation_type) for (name, relation_type) in cursor.fetchall()]

        return dict(existing)

    def __drop(self, cursor, schema, relation, relation_type):
        sql = 'drop {relation_type} if exists "{schema}"."{relation}" cascade'.format(schema=schema, relation_type=relation_type, relation=relation)
        cursor.execute(sql)

    def __do_execute(self, cursor, sql, model):
        try:
            cursor.execute(sql)
        except Exception as e:
            e.model = model
            raise e

    def drop_models(self, models):
        target = self.get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:

                existing = self.query_for_existing(cursor, target.schema);

                for model in models:
                    model_name = model.fqn[-1]
                    self.__drop(cursor, target.schema, model_name, existing[model_name])

    def get_model_by_fqn(self, models, fqn):
        for model in models:
            if tuple(model.fqn) == tuple(fqn):
                return model
        raise RuntimeError("Couldn't find a compiled model with fqn: '{}'".format(fqn))

    def execute_models(self, linker, models, limit_to=None):
        target = self.get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                dependency_list = [self.get_model_by_fqn(models, fqn) for fqn in linker.as_dependency_list(limit_to)]
                existing = self.query_for_existing(cursor, target.schema);

                num_models = len(dependency_list)
                if num_models == 0:
                    print("WARNING: No models to run in '{}'. Try checking your model configs and running `dbt compile`".format(self.target_path))
                    return

                for index, model in enumerate(dependency_list):
                    if model.name in existing:
                        self.__drop(cursor, target.schema, model.name, existing[model.name])
                        handle.commit()

                    print("Running {} of {} -- Creating relation {}.{}".format(index + 1, num_models, target.schema, model.name))
                    self.__do_execute(cursor, model.contents, model)
                    handle.commit()
                    yield model

    def run(self, specified_models=None):
        linker = self.deserialize_graph()
        compiled_models = self.get_compiled_models()

        limit_to = None
        if specified_models is not None:
            limit_to = []
            for model_name in specified_models:
                try:
                    model = find_model_by_name(compiled_models, model_name)
                    limit_to.append(tuple(model.fqn))
                except RuntimeError as e:
                    print("ERROR: {}".format(str(e)))
                    print("Exiting")
                    return
        try:
            self.create_schema()
            for model in self.execute_models(linker, compiled_models, limit_to):
                yield model, True
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)
