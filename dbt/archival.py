
from __future__ import print_function
import dbt.targets
import dbt.schema
import dbt.templates
import jinja2


class Archival(object):

    def __init__(self, project, archive_model):
        self.archive_model = archive_model
        self.project = project

        self.target = dbt.targets.get_target(self.project.run_environment())
        self.schema = dbt.schema.Schema(self.project, self.target)

    def compile(self):
        source_schema = self.archive_model.source_schema
        target_schema = self.archive_model.target_schema
        source_table  = self.archive_model.source_table
        dest_table    = self.archive_model.dest_table
        unique_key    = self.archive_model.unique_key
        updated_at    = self.archive_model.updated_at

        self.schema.create_schema(target_schema)

        source_columns = self.schema.get_columns_in_table(source_schema, source_table)

        if len(source_columns) == 0:
            raise RuntimeError('Source table "{}"."{}" does not exist'.format(source_schema, source_table))

        # create archive table if not exists! TODO: Sort & Dist keys! Hmmmm

        extra_cols = [
            ("valid_from", "timestamp"),
            ("valid_to", "timestamp"),
            ("scd_id","text"),
            ("dbt_updated_at","timestamp")
        ]

        dest_columns = source_columns + extra_cols
        self.schema.create_table(target_schema, dest_table, dest_columns, sort=updated_at, dist=unique_key)

        env = jinja2.Environment()

        ctx = {
            "columns"       : source_columns,
            "updated_at"    : updated_at,
            "unique_key"    : unique_key,
            "source_schema" : source_schema,
            "source_table"  : source_table,
            "target_schema" : target_schema,
            "dest_table"    : dest_table
        }

        base_query = dbt.templates.SCDArchiveTemplate 
        template = env.from_string(base_query, globals=ctx)
        rendered = template.render(ctx)

        return rendered

    def runtime_compile(self, compiled_model):
        context = self.context.copy()
        context.update(model.context())
        model.compile(context)

