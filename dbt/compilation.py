
import os
import fnmatch
import jinja2
from collections import defaultdict
import dbt.project
from dbt.source import Source
from dbt.utils import find_model_by_fqn, find_model_by_name, dependency_projects
from dbt.linker import Linker
import sqlparse

class Compiler(object):
    def __init__(self, project, create_template_class):
        self.project = project
        self.create_template = create_template_class()

    def initialize(self):
        if not os.path.exists(self.project['target-path']):
            os.makedirs(self.project['target-path'])

        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

    def get_target(self):
        target_cfg = self.project.run_environment()
        return RedshiftTarget(target_cfg)

    def model_sources(self, this_project, own_project=None):
        if own_project is None:
            own_project = this_project

        paths = own_project.get('source-paths', [])
        if self.create_template.label == 'build':
            return Source(this_project, own_project=own_project).get_models(paths, self.create_template)
        elif self.create_template.label == 'test':
            return Source(this_project, own_project=own_project).get_test_models(paths, self.create_template)
        else:
            raise RuntimeError("unexpected create template type: '{}'".format(self.create_template.label))

    def project_schemas(self):
        source_paths = self.project.get('source-paths', [])
        return Source(self.project).get_schemas(source_paths)

    def analysis_sources(self, project):
        paths = project.get('analysis-paths', [])
        return Source(project).get_analyses(paths)

    def validate_models_unique(self, models):
        found_models = defaultdict(list)
        for model in models:
            found_models[model.name].append(model)
        for model_name, model_list in found_models.items():
            if len(model_list) > 1:
                models_str = "\n  - ".join([str(model) for model in model_list])
                raise RuntimeError("Found {} models with the same name! Can't create tables. Name='{}'\n  - {}".format(len(model_list), model_name, models_str))

    def __write(self, build_filepath, payload):
        target_path = os.path.join(self.project['target-path'], build_filepath)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))

        with open(target_path, 'w') as f:
            f.write(payload)


    def __model_config(self, model, linker):
        def do_config(*args, **kwargs):
            if len(args) == 1 and len(kwargs) == 0:
                opts = args[0]
            elif len(args) == 0 and len(kwargs) > 0:
                opts = kwargs
            else:
                raise RuntimeError("Invalid model config given inline in {}".format(model))

            if type(opts) != dict:
                raise RuntimeError("Invalid model config given inline in {}".format(model))

            model.update_in_model_config(opts)
            model.add_to_prologue("Config specified in model: {}".format(opts))
            return ""
        return do_config

    def __ref(self, linker, ctx, model, all_models):
        schema = ctx['env']['schema']

        source_model = tuple(model.fqn)
        linker.add_node(source_model)

        def do_ref(*args):
            if len(args) == 1:
                other_model_name = self.create_template.model_name(args[0])
                other_model = find_model_by_name(all_models, other_model_name)
            elif len(args) == 2:
                other_model_package, other_model_name = args
                other_model_name = self.create_template.model_name(other_model_name)
                other_model = find_model_by_name(all_models, other_model_name, package_namespace=other_model_package)

            other_model_fqn = tuple(other_model.fqn[:-1] + [other_model_name])
            if not other_model.is_enabled:
                src_fqn = ".".join(source_model)
                ref_fqn = ".".join(other_model_fqn)
                raise RuntimeError("Model '{}' depends on model '{}' which is disabled in the project config".format(src_fqn, ref_fqn))

            linker.dependency(source_model, other_model_fqn)

            if other_model.is_ephemeral:
                linker.inject_cte(model, other_model)
                return other_model.cte_name
            else:
                return '"{}"."{}"'.format(schema, other_model_name)

        def wrapped_do_ref(*args):
            try:
                return do_ref(*args)
            except RuntimeError as e:
                print("Compiler error in {}".format(model.filepath))
                print("Enabled models:")
                for m in all_models:
                    print(" - {}".format(".".join(m.fqn)))
                raise e

        return wrapped_do_ref

    def compile_model(self, linker, model, models):
        jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=model.root_dir))
        template = jinja.get_template(model.rel_filepath)

        context = self.project.context()
        context['ref'] = self.__ref(linker, context, model, models)
        context['config'] = self.__model_config(model, linker)

        rendered = template.render(context)
        return rendered

    def write_graph_file(self, linker):
        filename = 'graph-{}.yml'.format(self.create_template.label)
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def combine_query_with_ctes(self, model, query, ctes, compiled_models):
        parsed_stmts = sqlparse.parse(query)
        if len(parsed_stmts) != 1:
            raise RuntimeError("unexpectedly parsed {} queries from model {}".format(len(parsed_stmts), model))
        parsed = parsed_stmts[0]

        with_stmt = None
        for token in parsed.tokens:
            if token.is_keyword and token.normalized == 'WITH':
                with_stmt = token
                break

        if with_stmt is None:
            # no with stmt, add one!
            first_token = parsed.token_first()
            with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, 'with')
            parsed.insert_before(first_token, with_stmt)
        else:
            # stmt exists, add a comma (which will come after our injected CTE(s) )
            trailing_comma = sqlparse.sql.Token(sqlparse.tokens.Punctuation, ',')
            parsed.insert_after(with_stmt, trailing_comma)

        cte_mapping = [(model.cte_name, compiled_models[model]) for model in ctes]

        # these newlines are important -- comments could otherwise interfere w/ query
        cte_stmts = [" {} as (\n{}\n)".format(name, contents) for (name, contents) in cte_mapping]

        cte_text = ", ".join(cte_stmts)
        parsed.insert_after(with_stmt, cte_text)

        return sqlparse.format(str(parsed), keyword_case='lower', reindent=True)

    def add_cte_to_rendered_query(self, linker, primary_model, compiled_models):
        fqn_to_model = {tuple(model.fqn): model for model in compiled_models}
        sorted_nodes = linker.as_topological_ordering()
        required_ctes = []
        for node in sorted_nodes:

            if node not in fqn_to_model:
                continue

            model = fqn_to_model[node]
            if model.is_ephemeral and model in linker.cte_map[primary_model]:
                required_ctes.append(model)

        query = compiled_models[primary_model]
        if len(required_ctes) == 0:
            return query
        else:
            compiled_query = self.combine_query_with_ctes(primary_model, query, required_ctes, compiled_models)
            return compiled_query

    def compile_models(self, linker, models):
        compiled_models = {model: self.compile_model(linker, model, models) for model in models}
        sorted_models = [find_model_by_fqn(models, fqn) for fqn in linker.as_topological_ordering()]

        written_models = []
        for model in sorted_models:
            injected_stmt = self.add_cte_to_rendered_query(linker, model, compiled_models)
            wrapped_stmt = model.compile(injected_stmt, self.project, self.create_template)

            serialized = model.serialize()
            linker.update_node_data(tuple(model.fqn), serialized)

            if model.is_ephemeral:
                continue

            self.__write(model.build_path(), wrapped_stmt)
            written_models.append(model)

        return compiled_models, written_models

    def compile_analyses(self, linker, compiled_models):
        analyses = self.analysis_sources(self.project)
        compiled_analyses = {analysis: self.compile_model(linker, analysis, compiled_models) for analysis in analyses}

        written_analyses = []
        referenceable_models = {}
        referenceable_models.update(compiled_models)
        referenceable_models.update(compiled_analyses)
        for analysis in analyses:
            injected_stmt = self.add_cte_to_rendered_query(linker, analysis, referenceable_models)
            build_path = analysis.build_path()
            self.__write(build_path, injected_stmt)
            written_analyses.append(analysis)

        return written_analyses

    def compile_schema_tests(self, linker):
        target_cfg = self.project.run_environment()

        schemas = self.project_schemas()

        schema_tests = []
        for schema in schemas:
            schema_tests.extend(schema.compile()) # compiling a SchemaFile returns >= 0 SchemaTest models

        written_tests = []
        for schema_test in schema_tests:
            serialized = schema_test.serialize()
            linker.update_node_data(tuple(schema_test.fqn), serialized)

            query = schema_test.render()
            self.__write(schema_test.build_path(), query)
            written_tests.append(schema_test)

        return written_tests

    def compile(self, dry=False):
        linker = Linker()

        all_models = self.model_sources(this_project=self.project)

        for project in dependency_projects(self.project):
            all_models.extend(self.model_sources(this_project=self.project, own_project=project))

        enabled_models = [model for model in all_models if model.is_enabled]

        compiled_models, written_models = self.compile_models(linker, enabled_models)

        # TODO : only compile schema tests for enabled models
        written_schema_tests = self.compile_schema_tests(linker)

        self.validate_models_unique(compiled_models)
        self.validate_models_unique(written_schema_tests)
        self.write_graph_file(linker)

        if self.create_template.label != 'test':
            written_analyses = self.compile_analyses(linker, compiled_models)
        else:
            written_analyses = []

        return len(written_models), len(written_schema_tests), len(written_analyses)
