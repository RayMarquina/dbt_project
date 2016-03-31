import pprint
import psycopg2
import os
import fnmatch
import re

from build.linker import Linker

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


class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

        self.linker = Linker()

    def __compiled_files(self):
        compiled_files = []
        sql_path = self.project['target-path']

        for root, dirs, files in os.walk(sql_path):
            for filename in files:
                if fnmatch.fnmatch(filename, "*.sql"):
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, sql_path)
                    compiled_files.append(rel_path)

        return compiled_files

    def __get_target(self):
        target_cfg = self.project.run_environment()
        if target_cfg['type'] == 'redshift':
            return RedshiftTarget(target_cfg)
        else:
            raise NotImplementedError("Unknown target type '{}'".format(target_cfg['type']))

    def __create_schema(self):
        target_cfg = self.project.run_environment()
        target = self.__get_target()
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(target_cfg['schema']))

    def __load_models(self):
        target = self.__get_target()
        for f in self.__compiled_files():
            with open(os.path.join(self.project['target-path'], f), 'r') as fh:
                self.linker.link(fh.read())

    def __query_for_existing(self, cursor, schema):
        sql = """
            select '{schema}.' || tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select '{schema}.' || viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)

        cursor.execute(sql)
        existing = [(name, relation_type) for (name, relation_type) in cursor.fetchall()]

        return dict(existing)

    def __drop(self, cursor, relation, relation_type):
        cascade = "cascade" if relation_type == 'view' else ""

        sql = "drop {relation_type} if exists {relation} {cascade}".format(relation_type=relation_type, relation=relation, cascade=cascade)

        cursor.execute(sql)

    def __execute_models(self):
        target = self.__get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:

                existing =  self.__query_for_existing(cursor, target.schema);

                for (relation, sql) in self.linker.as_dependency_list():

                    if relation in existing:
                        self.__drop(cursor, relation, existing[relation])
                        handle.commit()

                    print "creating {}".format(relation)
                    #print "         {}...".format(re.sub( '\s+', ' ', sql[0:100] ).strip())
                    cursor.execute(sql)
                    print "         {}".format(cursor.statusmessage)
                    handle.commit()

    def run(self):
        self.__create_schema()
        self.__load_models()
        self.__execute_models()

