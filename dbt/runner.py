
from __future__ import print_function

import psycopg2
import os, sys
import functools

from dbt.compilation import Linker, Compiler
from dbt.templates import BaseCreateTemplate
from dbt.targets import RedshiftTarget
from dbt.source import Source
from dbt.utils import find_model_by_name

from multiprocessing.dummy import Pool as ThreadPool

SCHEMA_PERMISSION_DENIED_MESSAGE = """The user '{user}' does not have sufficient permissions to create the schema '{schema}'.
Either create the schema  manually, or adjust the permissions of the '{user}' user."""

RELATION_PERMISSION_DENIED_MESSAGE = """The user '{user}' does not have sufficient permissions to create the model '{model}'  in the schema '{schema}'.
Please adjust the permissions of the '{user}' user on the '{schema}' schema.
With a superuser account, execute the following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """The user '{user}' does not have sufficient permissions to drop the model '{model}' in the schema '{schema}'.
This is likely because the relation was created by a different user. Either delete the model "{schema}"."{model}" manually,
or adjust the permissions of the '{user}' user in the '{schema}' schema."""

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

    def create_schema(self, schema_name):
        target = self.get_target()
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(schema_name))

    def get_schemas(self):
        target = self.get_target()
        existing = []
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('select nspname from pg_catalog.pg_namespace')

                existing = [name for (name,) in cursor.fetchall()]
        return existing

    def create_schema_or_exit(self, schema_name):

        target_cfg = self.project.run_environment()
        user = target_cfg['user']

        try:
            self.create_schema(schema_name)
        except psycopg2.ProgrammingError as e:
            if "permission denied for" in e.diag.message_primary:
                raise RuntimeError(SCHEMA_PERMISSION_DENIED_MESSAGE.format(schema=schema_name, user=user))
            else:
                raise e

    def query_for_existing(self, target, schema):
        sql = """
            select tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)


        with target.get_handle() as handle:
            with handle.cursor() as cursor:
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

    def execute_model(self, data):
        target = data['target']
        model = data['model']
        drop_type = data['drop_type']

        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                if drop_type is not None:
                    try:
                        self.__drop(cursor, target.schema, model.name, drop_type)
                    except psycopg2.ProgrammingError as e:
                        if "must be owner of relation" in e.diag.message_primary:
                            raise RuntimeError(RELATION_NOT_OWNER_MESSAGE.format(model=model.name, schema=target.schema, user=target.user))
                        else:
                            raise e
                    handle.commit()

                try:
                    self.__do_execute(cursor, model.contents, model)
                except psycopg2.ProgrammingError as e:
                    if "permission denied for" in e.diag.message_primary:
                        raise RuntimeError(RELATION_PERMISSION_DENIED_MESSAGE.format(model=model.name, schema=target.schema, user=target.user))
                    else:
                        raise e
                handle.commit()

        return model

    def execute_models(self, linker, models, limit_to=None):
        target = self.get_target()

        sequential_node_lists = linker.as_sequential_dependency_lists(limit_to)

        if len(sequential_node_lists) == 0:
            print("WARNING: No models to run in '{}'. Try checking your model configs and running `dbt compile`".format(self.target_path))
            return

        existing = self.query_for_existing(target, target.schema);

        # TODO : better names and clean this up!
        sequential_model_list = []
        for node_list in sequential_node_lists:
            model_list = []
            for node in node_list:
                model = self.get_model_by_fqn(models, node)
                drop_type = existing.get(model.name, None) # False, 'view', or 'table'
                data = {
                    "model" : model,
                    "target": target,
                    "drop_type": drop_type
                }
                model_list.append(data)
            sequential_model_list.append(model_list)

        # TODO : make this an arg
        completed = 0
        num_models = sum([len(model_list) for model_list in sequential_model_list])

        pool = ThreadPool(4)
        for model_list in sequential_model_list:
            results = pool.map(self.execute_model, model_list)
            for model in results:
                completed += 1
                print("{} of {} -- Created relation {}.{}".format(completed, num_models, target.schema, model.name))
        pool.close()
        pool.join()

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

        target_cfg = self.project.run_environment()
        schema_name = target_cfg['schema']

        try:
            schemas = self.get_schemas()

            if schema_name not in schemas:
                self.create_schema_or_exit(schema_name)

            self.execute_models(linker, compiled_models, limit_to)
            return []
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)
