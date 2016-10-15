
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

        self.schema_cache = {}

    def cache_table_columns(self, schema, table, columns):
        tid = (schema, table)

        if tid not in self.schema_cache:
            self.schema_cache[tid] = columns

        return tid

    def get_table_columns_if_cached(self, schema, table):
        tid = (schema, table)
        return self.schema_cache.get(tid, None)

    def get_schemas(self):
        existing = []
        results = self.execute_and_fetch('select nspname from pg_catalog.pg_namespace')
        return [name for (name,) in results]

    def create_schema(self, schema_name):
        target_cfg = self.project.run_environment()
        user = target_cfg['user']

        try:
            self.execute('create schema if not exists "{}"'.format(schema_name))
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


        results = self.execute_and_fetch(sql)
        existing = [(name, relation_type) for (name, relation_type) in results]

        return dict(existing)

    def execute(self, sql):
        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    self.logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    self.logger.debug("SQL status: %s in %0.2f seconds", cursor.statusmessage, post-pre)
                    return cursor.statusmessage
                except Exception as e:
                    self.target.rollback()
                    self.logger.exception("Error running SQL: %s", sql)
                    self.logger.debug("rolling back connection")
                    raise e

    def execute_and_fetch(self, sql):
        with self.target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    self.logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    self.logger.debug("SQL status: %s in %0.2f seconds", cursor.statusmessage, post-pre)
                    data = cursor.fetchall()
                    self.logger.debug("SQL response: %s", data)
                    return data
                except Exception as e:
                    self.target.rollback()
                    self.logger.exception("Error running SQL: %s", sql)
                    self.logger.debug("rolling back connection")
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

    def execute_without_auto_commit(self, sql, handle=None):
        if handle is None:
            handle = self.target.get_handle()

        cursor = handle.cursor()

        try:
            self.logger.debug("SQL: %s", sql)
            pre = time.time()
            cursor.execute(sql)
            post = time.time()
            self.logger.debug("SQL status: %s in %0.2f seconds", cursor.statusmessage, post-pre)
            return handle, cursor.statusmessage
        except Exception as e:
            self.target.rollback()
            self.logger.exception("Error running SQL: %s", sql)
            self.logger.debug("rolling back connection")
            raise e
        finally:
            cursor.close()

    def drop(self, schema, relation_type, relation):
        sql = 'drop {relation_type} if exists "{schema}"."{relation}" cascade'.format(schema=schema, relation_type=relation_type, relation=relation)
        self.logger.info("dropping %s %s.%s", relation_type, schema, relation)
        self.execute_and_handle_permissions(sql, relation)
        self.logger.info("dropped %s %s.%s", relation_type, schema, relation)

    def get_columns_in_table(self, schema_name, table_name):
        self.logger.debug("getting columns in table %s.%s", schema_name, table_name)

        columns = self.get_table_columns_if_cached(schema_name, table_name)
        if columns is not None:
            self.logger.debug("Found columns (in cache): %s", columns)
            return columns

        sql = self.target.sql_columns_in_table(schema_name, table_name)
        results = self.execute_and_fetch(sql)
        columns = [(column, data_type) for (column, data_type) in results]

        self.cache_table_columns(schema_name, table_name, columns)

        self.logger.debug("Found columns: %s", columns)
        return columns

    def rename(self, schema, from_name, to_name):
        rename_query =  'alter table "{schema}"."{from_name}" rename to "{to_name}"'.format(schema=schema, from_name=from_name, to_name=to_name)
        self.logger.info("renaming model %s.%s --> %s.%s", schema, from_name, schema, to_name)
        self.execute_and_handle_permissions(rename_query, from_name)
        self.logger.info("renamed model %s.%s --> %s.%s", schema, from_name, schema, to_name)

    def get_missing_columns(self, from_schema, from_table, to_schema, to_table):
        "Returns dict of {column:type} for columns in from_table that are missing from to_table"
        from_columns = {col:dtype for (col,dtype) in self.get_columns_in_table(from_schema, from_table)}
        to_columns = {col:dtype for (col,dtype) in self.get_columns_in_table(to_schema, to_table)}

        missing_columns = set(from_columns.keys()) - set(to_columns.keys())

        return [(col, dtype) for (col, dtype) in from_columns.items() if col in missing_columns]

    def create_table(self, schema, table, columns, sort, dist):
        fields = ['"{field}" {data_type}'.format(field=field, data_type=data_type) for (field, data_type) in columns]
        fields_csv = ",\n  ".join(fields)
        # TODO : Sort and Dist keys??
        sql = 'create table if not exists "{schema}"."{table}" (\n  {fields}\n);'.format(schema=schema, table=table, fields=fields_csv)
        self.logger.info('creating table "%s"."%s"'.format(schema, table))
        self.execute_and_handle_permissions(sql, table)

    def create_schema_if_not_exists(self, schema_name):
        schemas = self.get_schemas()

        if schema_name not in schemas:
            self.create_schema(schema_name)

    def expand_column_types_if_needed(self, from_table, to_schema, to_table):
        "The hard part!"
        pass

