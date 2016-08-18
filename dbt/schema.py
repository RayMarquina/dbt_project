
import psycopg2
import logging
import time

SCHEMA_PERMISSION_DENIED_MESSAGE = """The user '{user}' does not have sufficient permissions to create the schema '{schema}'.
Either create the schema  manually, or adjust the permissions of the '{user}' user."""

RELATION_PERMISSION_DENIED_MESSAGE = """The user '{user}' does not have sufficient permissions to create the model '{model}'  in the schema '{schema}'.
Please adjust the permissions of the '{user}' user on the '{schema}' schema.
With a superuser account, execute the following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select, insert, delete on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """The user '{user}' does not have sufficient permissions to drop the model '{model}' in the schema '{schema}'.
This is likely because the relation was created by a different user. Either delete the model "{schema}"."{model}" manually,
or adjust the permissions of the '{user}' user in the '{schema}' schema."""

class Schema(object):
    def __init__(self, project, target):
        self.project = project
        self.target = target
        self.logger = logging.getLogger(__name__)

    def create_schema(self, schema_name):
        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(schema_name))

    def get_schemas(self):
        existing = []
        with self.target.get_handle() as handle:
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

    def query_for_existing(self, schema):
        sql = """
            select tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)


        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute(sql)
                existing = [(name, relation_type) for (name, relation_type) in cursor.fetchall()]

        return dict(existing)

    def execute(self, sql):
        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    self.logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    self.logger.debug("SQL status: %s in %d seconds", cursor.statusmessage, post-pre)
                    return cursor.statusmessage
                except Exception as e:
                    self.logger.exception("Error running SQL: %s", sql)
                    raise e

    def execute_and_handle_permissions(self, query, model_name):
        try:
            return self.execute(query)
        except psycopg2.ProgrammingError as e:
            error_data = {"model": model_name, "schema": self.target.schema, "user": self.target.user}
            if 'must be owner of relation' in e.diag.message_primary:
                raise RuntimeError(RELATION_NOT_OWNER_MESSAGE.format(**error_data))
            elif "permission denied for" in e.diag.message_primary:
                raise RuntimeError(RELATION_PERMISSION_DENIED_MESSAGE.format(**error_data))
            else:
                raise e

    def drop(self, schema, relation_type, relation):
        sql = 'drop {relation_type} if exists "{schema}"."{relation}" cascade'.format(schema=schema, relation_type=relation_type, relation=relation)
        self.logger.info("dropping %s %s.%s", relation_type, schema, relation)
        self.execute_and_handle_permissions(sql, relation)
        self.logger.info("dropped %s %s.%s", relation_type, schema, relation)


    def rename(self, schema, from_name, to_name):
        rename_query =  'alter table "{schema}"."{from_name}" rename to "{to_name}"'.format(schema=schema, from_name=from_name, to_name=to_name)
        self.logger.info("renaming model %s.%s --> %s.%s", schema, from_name, schema, to_name)
        self.execute_and_handle_permissions(rename_query, from_name)
        self.logger.info("renamed model %s.%s --> %s.%s", schema, from_name, schema, to_name)

