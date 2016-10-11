

from __future__ import print_function
import dbt.targets
import dbt.schema
import dbt.templates
import jinja2

class ArchivableTable(object):
    def __init__(self, source_table, dest_table, unique_key, updated_at):
        self.source_table = source_table
        self.dest_table = dest_table
        self.unique_key = unique_key
        self.updated_at = updated_at

    def __repr__(self):
        return "<ArchiveTable {} --> {} unique:{} updated_at:{}>".format(self.source_table, self.dest_table, self.unique_key, self.updated_at)

class SourceSchema(object):
    def __init__(self, source_schema, target_schema, tables):
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.tables = [self.parse_table(t) for t in tables]

    def parse_table(self, table_definition):
        return ArchivableTable(**table_definition)

class ArchiveTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

        self.target = dbt.targets.get_target(self.project.run_environment())
        self.schema = dbt.schema.Schema(self.project, self.target)

    def run(self):
        if 'archive' not in self.project:
            raise RuntimeError("dbt_project.yml file is missing an 'archive' config!")

        # TODO : obviously handle input / validate better here
        raw_source_schemas = self.project['archive']
        source_schemas = [SourceSchema(**item) for item in raw_source_schemas]

        for source_schema in source_schemas:

            # create archive schema if not exists!
            self.schema.create_schema(source_schema.target_schema)

            for table in source_schema.tables:
                columns = self.schema.get_columns_in_table(source_schema.source_schema, table.source_table)

                if len(columns) == 0:
                    raise RuntimeError('Source table "{}"."{}" does not exist'.format(source_schema.source_schema, table.source_table))

                # create archive table if not exists! TODO: Sort & Dist keys! Hmmmm

                extra_cols = [
                    ("valid_from", "timestamp"),
                    ("valid_to", "timestamp"),
                    ("scd_id","text"),
                    ("dbt_updated_at","timestamp")
                ]

                dest_columns = columns + extra_cols
                self.schema.create_table(source_schema.target_schema, table.dest_table, dest_columns, sort=table.updated_at, dist=table.unique_key)

                env = jinja2.Environment()

                ctx = {
                    "columns": columns,
                    "table"  : table,
                    "archive": source_schema
                }

                base_query = dbt.templates.SCDArchiveTemplate 
                template = env.from_string(base_query, globals=ctx)
                rendered = template.render(ctx)

                template = dbt.templates.ArchiveInsertTemplate()
                transaction = template.wrap(source_schema.target_schema, table.dest_table, rendered, table.unique_key)

                self.schema.execute_and_handle_permissions(transaction, table.dest_table)
