import os

from dbt.targets import RedshiftTarget
from dbt.source import Source
from dbt.runner import RunModelResult

import psycopg2
import logging
import time

QUERY_VALIDATE_NOT_NULL = """
with validation as (
  select "{field}" as f
  from "{schema}"."{table}"
)
select count(*) from validation where f is null
"""

QUERY_VALIDATE_UNIQUE = """
with validation as (
  select "{field}" as f
  from "{schema}"."{table}"
),
validation_errors as (
    select f from validation group by f having count(*) > 1
)
select count(*) from validation_errors
"""

QUERY_VALIDATE_ACCEPTED_VALUES = """
with all_values as (
  select distinct "{field}" as f
  from "{schema}"."{table}"
),
validation_errors as (
    select f from all_values where f not in ({values_csv})
)
select count(*) from validation_errors
"""

QUERY_VALIDATE_REFERENTIAL_INTEGRITY = """
with parent as (
  select "{parent_field}" as id
  from "{schema}"."{parent_table}"
), child as (
  select "{child_field}" as id
  from "{schema}"."{child_table}"
)
select count(*) from child
where id not in (select id from parent) and id is not null
"""

class SchemaTester(object):
    def __init__(self, project):
        self.logger = logging.getLogger(__name__)
        self.project = project

    def project_schemas(self):
        source_paths = self.project['source-paths']
        return Source(self.project).get_schemas(source_paths)

    def get_query_params(self, table, field):
        target_cfg = self.project.run_environment()
        schema = target_cfg['schema']
        return {
            "schema": schema,
            "table": table,
            "field": field
        }

    def make_query(self, query, params):
        return query.format(**params)

    def get_target(self):
        target_cfg = self.project.run_environment()
        return RedshiftTarget(target_cfg)

    def execute_query(self, model, sql):
        target = self.get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    self.logger.debug("SQL: %s", sql)
                    pre = time.time()
                    cursor.execute(sql)
                    post = time.time()
                    self.logger.debug("SQL status: %s in %d seconds", cursor.statusmessage, post-pre)
                except psycopg2.ProgrammingError as e:
                    self.logger.exception('programming error: %s', sql)
                    return e.diag.message_primary
                except Exception as e:
                    self.logger.exception('encountered exception while running: %s', sql)
                    e.model = model
                    raise e

                result = cursor.fetchone()
                if len(result) != 1:
                    self.logger.error("SQL: %s", sql)
                    self.logger.error("RESULT: %s", result)
                    raise RuntimeError("Unexpected validation result. Expected 1 record, got {}".format(len(result)))
                else:
                    return result[0]

    def validate_not_null(self, model, constraint_data):
        for field in constraint_data:
            params = self.get_query_params(model.name, field)
            sql = self.make_query(QUERY_VALIDATE_NOT_NULL, params)
            print('VALIDATE NOT NULL "{}"."{}"'.format(model.name, field))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def validate_unique(self, model, constraint_data):
        for field in constraint_data:
            params = self.get_query_params(model.name, field)
            sql = self.make_query(QUERY_VALIDATE_UNIQUE, params)
            print('VALIDATE UNIQUE "{}"."{}"'.format(model.name, field))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def validate_relationships(self, model, constraint_data):
        for reference in constraint_data:
            target_cfg = self.project.run_environment()
            params = {
                "schema": target_cfg['schema'],
                "child_table": model.name,
                "child_field": reference['from'],
                "parent_table": reference['to'],
                "parent_field": reference['field']
            }
            sql = self.make_query(QUERY_VALIDATE_REFERENTIAL_INTEGRITY, params)
            print('VALIDATE REFERENTIAL INTEGRITY "{}"."{}" to "{}"."{}"'.format(model.name, reference['from'], reference['to'], reference['field']))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def validate_accepted_values(self, model, constraint_data):
        for constraint in constraint_data:
            quoted_values = ["'{}'".format(v) for v in constraint['values']]
            quoted_values_csv = ",".join(quoted_values)
            target_cfg = self.project.run_environment()
            params = {
                "schema": target_cfg['schema'],
                "table" : model.name,
                "field" : constraint['field'],
                "values_csv": quoted_values_csv
            }
            sql = self.make_query(QUERY_VALIDATE_ACCEPTED_VALUES, params)
            print('VALIDATE ACCEPTED VALUES "{}"."{}" VALUES ({})'.format(model.name, constraint['field'], quoted_values_csv))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                #print(sql)
                yield False

    def validate_schema_constraint(self, model, constraint_type, constraint_data):
        constraint_map = {
            'not_null': self.validate_not_null,
            'unique': self.validate_unique,
            'relationships': self.validate_relationships,
            'accepted-values': self.validate_accepted_values
        }

        if constraint_type in constraint_map:
            validator = constraint_map[constraint_type]
            for test_passed in validator(model, constraint_data):
                yield test_passed
        else:
            raise RuntimeError("Invalid constraint '{}' specified for '{}' in schema.yml".format(constraint_type, model))

    def validate_schema(self, schemas):
        "generate queries for each schema constraints"
        for schema in schemas:
            for model_name in schema.schema.keys():
                # skip this model if it's not enabled
                model = schema.get_model(self.project, model_name)
                config = model.get_config(self.project)
                if not config['enabled']:
                    continue

                constraints = schema.schema[model_name]['constraints']
                for constraint_type, constraint_data in constraints.items():
                    for test_passed in self.validate_schema_constraint(model, constraint_type, constraint_data):
                        yield model, test_passed

    def test(self):
        schemas = self.project_schemas()
        for (model, test_passed) in self.validate_schema(schemas):
            error = None if test_passed else "ERROR"
            yield RunModelResult(model, error)
