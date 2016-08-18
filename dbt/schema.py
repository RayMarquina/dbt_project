
import psycopg2

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

