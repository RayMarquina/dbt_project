import os
import yaml

from dbt.runner import RedshiftTarget

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
        self.project = project

    def project_schemas(self):
        schemas = {}

        for source_path in self.project['source-paths']:
            full_source_path = os.path.join(self.project['project-root'], source_path)
            for root, dirs, files in os.walk(full_source_path):
                for filename in files:
                    if filename == "schema.yml":
                        filepath = os.path.join(root, filename)
                        abs_path = os.path.join(root, filename)
                        rel_path = os.path.relpath(abs_path, full_source_path)
                        parent_path = os.path.dirname(rel_path)
                        with open(filepath) as fh:
                            project_cfg = yaml.safe_load(fh)
                            schemas[parent_path] = project_cfg

        return schemas

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
        if target_cfg['type'] == 'redshift':
            return RedshiftTarget(target_cfg)
        else:
            raise NotImplementedError("Unknown target type '{}'".format(target_cfg['type']))

    def execute_query(self, model, sql):
        target = self.get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    e.model = model
                    raise e

                result = cursor.fetchone()
                if len(result) != 1:
                    print("SQL: {}".format(sql))
                    print("RESULT:".format(result))
                    raise RuntimeError("Unexpected validation result. Expected 1 record, got {}".format(len(result)))
                else:
                    return result[0]

    def validate_not_null(self, model, constraint_data):
        table = model[-1]
        for field in constraint_data:
            params = self.get_query_params(table, field)
            sql = self.make_query(QUERY_VALIDATE_NOT_NULL, params)
            print ('VALIDATE NOT NULL "{}"."{}"'.format(table, field))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def validate_unique(self, model, constraint_data):
        table = model[-1]
        for field in constraint_data:
            params = self.get_query_params(table, field)
            sql = self.make_query(QUERY_VALIDATE_UNIQUE, params)
            print ('VALIDATE UNIQUE "{}"."{}"'.format(table, field))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def validate_relationships(self, model, constraint_data):
        table = model[-1]
        for reference in constraint_data:
            target_cfg = self.project.run_environment()
            params = {
                "schema": target_cfg['schema'],
                "child_table": table,
                "child_field": reference['from'],
                "parent_table": reference['to'],
                "parent_field": reference['field']
            }
            sql = self.make_query(QUERY_VALIDATE_REFERENTIAL_INTEGRITY, params)
            print ('VALIDATE REFERENTIAL INTEGRITY "{}"."{}" to "{}"."{}"'.format(table, reference['from'], reference['to'], reference['field']))
            num_rows = self.execute_query(model, sql)
            if num_rows == 0:
                print("  OK")
                yield True
            else:
                print("  FAILED ({})".format(num_rows))
                yield False

    def validate_accepted_values(self, model, constraint_data):
        table = model[-1]
        for constraint in constraint_data:
            quoted_values = ["'{}'".format(v) for v in constraint['values']]
            quoted_values_csv = ",".join(quoted_values)
            target_cfg = self.project.run_environment()
            params = {
                "schema": target_cfg['schema'],
                "table" : table,
                "field" : constraint['field'],
                "values_csv": quoted_values_csv
            }
            sql = self.make_query(QUERY_VALIDATE_ACCEPTED_VALUES, params)
            print ('VALIDATE ACCEPTED VALUES "{}"."{}" VALUES ({})'.format(table, constraint['field'], quoted_values_csv))
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

    def validate_schema(self, schemas, compiler):
        "generate queries for each schema constraints"
        for model_group, model_schemas in schemas.items():
            for model_name, schema_info in model_schemas.items():

                model = (model_group, model_name)
                # skip this model if it's not enabled
                model_config = compiler.get_model_config(model_group, model_name)
                if not model_config['enabled']:
                    continue

                constraints = schema_info['constraints']
                for constraint_type, constraint_data in constraints.items():
                    try:
                        for test_passed in self.validate_schema_constraint(model, constraint_type, constraint_data):
                            yield model, test_passed
                    except RuntimeError as e:
                        print("ERRROR: {}".format(str(e)))

    def test(self, compiler):
        schemas = self.project_schemas()
        for (model, test_passed) in self.validate_schema(schemas, compiler):
            yield model, test_passed
