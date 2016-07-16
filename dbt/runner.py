
from __future__ import print_function

import pprint
import psycopg2
import os, sys
import fnmatch

from dbt.compilation import Linker, Compiler
from dbt.templates import BaseCreateTemplate

class RedshiftTarget:
    def __init__(self, cfg):
        assert cfg['type'] == 'redshift'
        self.host = cfg['host']
        self.user = cfg['user']
        self.password = cfg['pass']
        self.port = cfg['port']
        self.dbname = cfg['dbname']
        self.schema = cfg['schema']

    def __get_spec(self):
        return "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(
            self.dbname,
            self.user,
            self.host,
            self.password,
            self.port
        )

    def get_handle(self):
        return psycopg2.connect(self.__get_spec())

class Runner:
    def __init__(self, project, target_path, run_mode):
        self.project = project
        self.target_path = target_path
        self.run_mode = run_mode
        self.model_sql_map = {}

    def __compiled_files(self):
        compiled_files = []

        for root, dirs, files in os.walk(self.target_path):
            for filename in files:
                if fnmatch.fnmatch(filename, "*.sql"):
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, self.target_path)
                    compiled_files.append(rel_path)

        return compiled_files

    def __get_target(self):
        target_cfg = self.project.run_environment()
        if target_cfg['type'] == 'redshift':
            return RedshiftTarget(target_cfg)
        else:
            raise NotImplementedError("Unknown target type '{}'".format(target_cfg['type']))

    def __deserialize_graph(self, linker):
        base_target_path = self.project['target-path']
        filename = 'graph-{}.yml'.format(self.run_mode)
        graph_file = os.path.join(base_target_path, filename)
        linker.read_graph(graph_file)

    def __create_schema(self):
        target_cfg = self.project.run_environment()
        target = self.__get_target()
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(target_cfg['schema']))

    def __load_models(self):
        target = self.__get_target()
        for f in self.__compiled_files():
            with open(os.path.join(self.target_path, f), 'r') as fh:
                namespace = os.path.dirname(f)
                model_name, _ = os.path.splitext(os.path.basename(f))

                model = (self.project['name'], namespace, model_name)
                self.model_sql_map[model] = fh.read()

    def __query_for_existing(self, cursor, schema):
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
        target = self.__get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:

                existing = self.__query_for_existing(cursor, target.schema);

                for model in models:
                    model_name = model[-1]
                    self.__drop(cursor, target.schema, model_name, existing[model_name])

    def __execute_models(self, linker):
        target = self.__get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                existing = self.__query_for_existing(cursor, target.schema);
                dependency_list = list(linker.as_dependency_list())

                num_models = len(dependency_list)
                if num_models == 0:
                    print("WARNING: Target directory is empty: '{}'. Try running `dbt compile`.".format(self.target_path))
                    return

                for index, model in enumerate(dependency_list):
                    package_name, namespace, model_name = model
                    if model_name in existing:
                        self.__drop(cursor, target.schema, model_name, existing[model_name])
                        handle.commit()

                    print("{} of {} -- Creating relation {}.{}".format(index + 1, num_models, target.schema, model_name))
                    sql = self.model_sql_map[model]
                    self.__do_execute(cursor, sql, model)
                    handle.commit()
                    yield model

    def run(self):
        compiler = Compiler(self.project, BaseCreateTemplate)
        compiler.initialize()
        created_models, created_analyses = compiler.compile()
        print("Created {} models and {} analyses".format(created_models, created_analyses))

        linker = Linker()
        self.__deserialize_graph(linker)
        self.__load_models()

        try:
            self.__create_schema()
            for model in self.__execute_models(linker):
                yield model, True
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)
