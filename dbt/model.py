
import os.path
import yaml
import jinja2
import re
from dbt.templates import BaseCreateTemplate, DryCreateTemplate
import dbt.schema_tester
import dbt.project

class SourceConfig(object):
    Materializations = ['view', 'table', 'incremental', 'ephemeral']
    ConfigKeys = ['enabled', 'materialized', 'dist', 'sort', 'sql_where', 'unique_key']

    def __init__(self, active_project, own_project, fqn):
        self.active_project = active_project
        self.own_project = own_project
        self.fqn = fqn

        self.in_model_config   = {} # the config options defined within the model

    def _merge(self, *configs):
        merged_config = {}
        for config in configs:
            merged_config.update(config)
        return merged_config

    # this is re-evaluated every time `config` is called.
    # we can cache it, but that complicates things. TODO : see how this fares performance-wise
    @property
    def config(self):
        """
        Config resolution order:

         if this is a dependency model:
           - own project config
           - in-model config
           - active project config
         if this is a top-level model:
           - active project config
           - in-model config
        """
        defaults = {"enabled": True, "materialized": "view"}
        active_config = self.load_config_from_active_project()

        if self.active_project['name'] == self.own_project['name']:
            return self._merge(defaults, active_config, self.in_model_config)
        else:
            own_config = self.load_config_from_own_project()
            return self._merge(defaults, own_config, self.in_model_config, active_config)

    def update_in_model_config(self, config):
        self.in_model_config.update(config)

    def get_project_config(self, project):
        config = {} 
        model_configs = project['models']

        fqn = self.fqn[:]
        for level in fqn:
            level_config = model_configs.get(level, None)
            if level_config is None:
                break
            relevant_configs = {key: level_config[key] for key in level_config if key in self.ConfigKeys}
            config.update(relevant_configs)
            model_configs = model_configs[level]

        return config

    def load_config_from_own_project(self):
        return self.get_project_config(self.own_project)

    def load_config_from_active_project(self):
        return self.get_project_config(self.active_project)

class DBTSource(object):
    dbt_run_type = 'base'

    def __init__(self, project, top_dir, rel_filepath, own_project):
        self.project = project
        self.own_project = own_project

        self.top_dir = top_dir
        self.rel_filepath = rel_filepath
        self.filepath = os.path.join(top_dir, rel_filepath)
        self.filedir = os.path.dirname(self.filepath)
        self.name = self.fqn[-1]
        self.own_project_name = self.fqn[0]

        self.source_config = SourceConfig(project, own_project, self.fqn)

    @property
    def root_dir(self):
        return os.path.join(self.own_project['project-root'], self.top_dir)

    def compile(self):
        raise RuntimeError("Not implemented!")
    
    def serialize(self):
        serialized = {
            "build_path": os.path.join(self.project['target-path'], self.build_path()),
            "source_path": self.filepath,
            "name": self.name,
            "tmp_name": self.tmp_name(),
            "project_name": self.own_project['name'],
            "dbt_run_type": self.dbt_run_type
        }

        serialized.update(self.config)
        return serialized

    @property
    def contents(self):
        with open(self.filepath) as fh:
            return fh.read().strip()

    @property
    def config(self):
        return self.source_config.config

    def update_in_model_config(self, config):
        self.source_config.update_in_model_config(config)

    @property
    def materialization(self):
        return self.config['materialized']

    @property
    def is_incremental(self):
        return self.materialization == 'incremental'

    @property
    def is_ephemeral(self):
        return self.materialization == 'ephemeral'

    @property
    def is_table(self):
        return self.materialization == 'table'

    @property
    def is_view(self):
        return self.materialization == 'view'

    @property
    def is_enabled(self):
        return self.config['enabled']


    @property
    def fqn(self):
        "fully-qualified name for model. Includes all subdirs below 'models' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.own_project['name']] + parts[1:-1] + [name]

    @property
    def original_fqn(self):
        return self.fqn

    def tmp_name(self):
        return "{}__dbt_tmp".format(self.name)

    def rename_query(self, schema):
        opts = {
            "schema": schema,
            "tmp_name": self.tmp_name(),
            "final_name": self.name
        }

        return 'alter table "{schema}"."{tmp_name}" rename to "{final_name}"'.format(**opts)


class Model(DBTSource):
    dbt_run_type = 'run'

    def __init__(self, project, model_dir, rel_filepath, own_project, create_template):
        self.prologue = []
        self.create_template = create_template
        super(Model, self).__init__(project, model_dir, rel_filepath, own_project)

    def add_to_prologue(self, s):
        self.prologue.append(s)

    def get_prologue_string(self):
        blob = "\n".join("-- {}".format(s) for s in self.prologue)
        return "-- Compiled by DBT\n{}".format(blob)

    def sort_qualifier(self, model_config):
        if 'sort' not in model_config or self.is_view or self.is_ephemeral:
            return ''
        sort_keys = model_config['sort']
        if type(sort_keys) == str:
            sort_keys = [sort_keys]

        # remove existing quotes in field name, then wrap in quotes
        formatted_sort_keys = ['"{}"'.format(sort_key.replace('"', '')) for sort_key in sort_keys]
        return "sortkey ({})".format(', '.join(formatted_sort_keys))

    def dist_qualifier(self, model_config):
        if 'dist' not in model_config or self.is_view or self.is_ephemeral:
            return ''

        dist_key = model_config['dist']

        if type(dist_key) != str:
            raise RuntimeError("The provided distkey '{}' is not valid!".format(dist_key))

        return 'distkey ("{}")'.format(dist_key)

    def build_path(self):
        build_dir = self.create_template.label
        filename = "{}.sql".format(self.create_template.model_name(self.name))
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def compile(self, rendered_query, project, create_template):
        model_config = self.config

        if self.materialization not in SourceConfig.Materializations:
            raise RuntimeError("Invalid materialize option given: '{}'. Must be one of {}".format(self.materialization, SourceConfig.Materializations))

        ctx = project.context().copy()
        schema = ctx['env'].get('schema', 'public')

        # these are empty strings if configs aren't provided
        dist_qualifier = self.dist_qualifier(model_config)
        sort_qualifier = self.sort_qualifier(model_config)

        if self.materialization == 'incremental':
            identifier = self.name
            ctx['this'] =  '"{}"."{}"'.format(schema, identifier)
            if 'sql_where' not in model_config:
                raise RuntimeError("sql_where not specified in model materialized as incremental: {}".format(self))
            raw_sql_where = model_config['sql_where']
            env = jinja2.Environment()
            sql_where = env.from_string(raw_sql_where).render(ctx)
            unique_key = model_config.get('unique_key', None)
        else:
            identifier = self.tmp_name()
            ctx['this'] =  '"{}"."{}"'.format(schema, identifier)
            sql_where = None
            unique_key = None

        opts = {
            "materialization": self.materialization,
            "schema": schema,
            "identifier": identifier,
            "query": rendered_query,
            "dist_qualifier": dist_qualifier,
            "sort_qualifier": sort_qualifier,
            "sql_where": sql_where,
            "prologue": self.get_prologue_string(),
            "unique_key" : unique_key
        }

        return create_template.wrap(opts)
    
    @property
    def cte_name(self):
        return "__dbt__CTE__{}".format(self.name)

    def __repr__(self):
        return "<Model {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Analysis(Model):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        return super(Analysis, self).__init__(project, target_dir, rel_filepath, own_project, BaseCreateTemplate())

    def build_path(self):
        build_dir = 'build-analysis'
        filename = "{}.sql".format(self.name)
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def __repr__(self):
        return "<Analysis {}: {}>".format(self.name, self.filepath)

class TestModel(Model):
    dbt_run_type = 'dry-run'

    def __init__(self, project, target_dir, rel_filepath, own_project, create_template):
        return super(TestModel, self).__init__(project, target_dir, rel_filepath, own_project, create_template)

    def build_path(self):
        build_dir = self.create_template.label
        filename = "{}.sql".format(self.name)
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    @property
    def fqn(self):
        "fully-qualified name for model. Includes all subdirs below 'models' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        test_name = DryCreateTemplate.model_name(name)
        return [self.own_project['name']] + parts[1:-1] + [test_name]

    @property
    def original_fqn(self):
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.project['name']] + parts[1:-1] + [name]

    def __repr__(self):
        return "<TestModel {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class SchemaTest(DBTSource):
    test_type = "base"
    dbt_run_type = 'test'

    def __init__(self, project, target_dir, rel_filepath, model_name, options):
        self.schema = project.context()['env']['schema']
        self.model_name = model_name
        self.options = options
        self.params = self.get_params(options)

        super(SchemaTest, self).__init__(project, target_dir, rel_filepath, project)

    @property
    def fqn(self):
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.project['name']] + parts[1:-1] + ['schema',  self.get_filename()]

    def get_params(self, options):
        return {
            "schema": self.schema,
            "table": self.model_name,
            "field": options
        }

    def unique_option_key(self):
        return self.params

    def get_filename(self):
        key = re.sub('[^0-9a-zA-Z]+', '_', self.unique_option_key())
        filename = "{test_type}_{model_name}_{key}".format(test_type=self.test_type, model_name=self.model_name, key=key)
        return filename

    def build_path(self):
        build_dir = "test"
        filename = "{}.sql".format(self.get_filename())
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    @property
    def template(self):
        raise NotImplementedError("not implemented")

    def render(self):
        return self.template.format(**self.params)

    def __repr__(self):
        class_name = self.__class__.__name__
        return "<{} {}.{}: {}>".format(class_name, self.project['name'], self.name, self.filepath)

class NotNullSchemaTest(SchemaTest):
    template = dbt.schema_tester.QUERY_VALIDATE_NOT_NULL
    test_type = "not_null"

    def unique_option_key(self):
        return self.params['field']

    def describe(self):
        return 'VALIDATE NOT NULL {schema}.{table}.{field}'.format(**self.params)

class UniqueSchemaTest(SchemaTest):
    template = dbt.schema_tester.QUERY_VALIDATE_UNIQUE
    test_type = "unique"

    def unique_option_key(self):
        return self.params['field']

    def describe(self):
        return 'VALIDATE UNIQUE {schema}.{table}.{field}'.format(**self.params)

class ReferentialIntegritySchemaTest(SchemaTest):
    template = dbt.schema_tester.QUERY_VALIDATE_REFERENTIAL_INTEGRITY
    test_type = "relationships"

    def get_params(self, options):
        return {
            "schema": self.schema,
            "child_table": self.model_name,
            "child_field": options['from'],
            "parent_table": options['to'],
            "parent_field": options['field']
        }

    def unique_option_key(self):
        return "{child_field}_to_{parent_table}_{parent_field}".format(**self.params)

    def describe(self):
        return 'VALIDATE REFERENTIAL INTEGRITY {schema}.{child_table}.{child_field} to {schema}.{parent_table}.{parent_field}'.format(**self.params)

class AcceptedValuesSchemaTest(SchemaTest):
    template = dbt.schema_tester.QUERY_VALIDATE_ACCEPTED_VALUES
    test_type = "accepted_values"

    def get_params(self, options):
        quoted_values = ["'{}'".format(v) for v in options['values']]
        quoted_values_csv = ",".join(quoted_values)
        return {
            "schema": self.schema,
            "table" : self.model_name,
            "field" : options['field'],
            "values_csv": quoted_values_csv
        }

    def unique_option_key(self):
        return "{field}".format(**self.params)

    def describe(self):
        return 'VALIDATE ACCEPTED VALUES {schema}.{table}.{field} VALUES ({values_csv})'.format(**self.params)

class SchemaFile(DBTSource):
    SchemaTestMap = {
        'not_null': NotNullSchemaTest,
        'unique': UniqueSchemaTest,
        'relationships': ReferentialIntegritySchemaTest,
        'accepted_values': AcceptedValuesSchemaTest
    }

    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(SchemaFile, self).__init__(project, target_dir, rel_filepath, own_project)
        self.og_target_dir = target_dir
        self.schema = yaml.safe_load(self.contents)

    def get_test(self, test_type):
        if test_type in SchemaFile.SchemaTestMap:
            return SchemaFile.SchemaTestMap[test_type]
        else:
            possible_types = ", ".join(SchemaFile.SchemaTestMap.keys())
            raise RuntimeError("Invalid validation type given in {}: '{}'. Possible: {}".format(self.filepath, test_type, possible_types))

    def compile(self):
        schema_tests = []
        for model_name, constraint_blob in self.schema.items():
            constraints = constraint_blob.get('constraints', {})
            for constraint_type, constraint_data in constraints.items():
                for params in constraint_data:
                    schema_test_klass = self.get_test(constraint_type)
                    schema_test = schema_test_klass(self.project, self.og_target_dir, self.rel_filepath, model_name, params)
                    schema_tests.append(schema_test)
        return schema_tests

    def __repr__(self):
        return "<SchemaFile {}.{}: {}>".format(self.project['name'], self.model_name, self.filepath)

class Csv(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(Csv, self).__init__(project, target_dir, rel_filepath, own_project)

    def __repr__(self):
        return "<Csv {}.{}: {}>".format(self.project['name'], self.model_name, self.filepath)
