
import os
import fnmatch
import jinja2
from collections import defaultdict
import dbt.project
from dbt.source import Source
from dbt.utils import find_model_by_name

import networkx as nx

class Linker(object):
    def __init__(self):
        self.graph = nx.DiGraph()

    def nodes(self):
        return self.graph.nodes()

    def as_dependency_list(self, limit_to=None):
        try:
            return nx.topological_sort(self.graph, nbunch=limit_to)
        except KeyError as e:
            model_name = ".".join(e.message)
            raise RuntimeError("Couldn't find model '{}' -- does it exist or is it diabled?".format(model_name))

    def dependency(self, node1, node2):
        "indicate that node1 depends on node2"
        self.graph.add_node(node1)
        self.graph.add_node(node2)
        self.graph.add_edge(node2, node1)

    def add_node(self, node):
        self.graph.add_node(node)

    def write_graph(self, outfile):
        nx.write_yaml(self.graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_yaml(infile)

class Compiler(object):
    def __init__(self, project, create_template_class):
        self.project = project
        self.create_template = create_template_class()

    def initialize(self):
        if not os.path.exists(self.project['target-path']):
            os.makedirs(self.project['target-path'])

        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

    def dependency_projects(self):
        for obj in os.listdir(self.project['modules-path']):
            full_obj = os.path.join(self.project['modules-path'], obj)
            if os.path.isdir(full_obj):
                project = dbt.project.read_project(os.path.join(full_obj, 'dbt_project.yml'))
                yield project

    def model_sources(self, project):
        "source_key is a dbt config key like source-paths or analysis-paths"
        paths = project.get('source-paths', [])
        if self.create_template.label == 'build':
            return Source(project).get_models(paths)
        elif self.create_template.label == 'test':
            return Source(project).get_test_models(paths)
        else:
            raise RuntimeError("unexpected create template type: '{}'".format(self.create_template.label))

    def analysis_sources(self, project):
        "source_key is a dbt config key like source-paths or analysis-paths"
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


    def __ref(self, linker, ctx, model, all_models):
        schema = ctx['env']['schema']

        # if this node doesn't have any deps, still make sure it's a part of the graph
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
            other_model_config = other_model.get_config(self.project)
            if not other_model_config['enabled']:
                src_fqn = ".".join(source_model)
                ref_fqn = ".".join(other_model_fqn)
                raise RuntimeError("Model '{}' depends on model '{}' which is disabled in the project config".format(src_fqn, ref_fqn))

            linker.dependency(source_model, other_model_fqn)
            return '"{}"."{}"'.format(schema, other_model_name)

        return do_ref

    def compile_model(self, linker, model, models):
        jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=model.root_dir))

        model_config = model.get_config(self.project)

        if not model_config.get('enabled'):
            return None

        template = jinja.get_template(model.rel_filepath)

        context = self.project.context()
        context['ref'] = self.__ref(linker, context, model, models)

        rendered = template.render(context)

        stmt = model.compile(rendered, self.project, self.create_template)
        if stmt:
            build_path = model.build_path(self.create_template)
            self.__write(build_path, stmt)
            return True
        return False

    def __write_graph_file(self, linker):
        filename = 'graph-{}.yml'.format(self.create_template.label)
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def is_enabled(self, model):
        config = model.get_config(self.project)
        enabled = config['enabled']
        return enabled

    def compile(self):
        all_models = self.model_sources(self.project)

        for project in self.dependency_projects():
            all_models.extend(self.model_sources(project))

        models = [model for model in all_models if self.is_enabled(model)]

        self.validate_models_unique(models)

        model_linker = Linker()
        compiled_models = []
        for model in models:
            compiled = self.compile_model(model_linker, model, models)
            if compiled:
                compiled_models.append(compiled)

        self.__write_graph_file(model_linker)

        # don't compile analyses in test mode!
        compiled_analyses = []
        if self.create_template.label != 'test':
            analysis_linker = Linker()
            analyses = self.analysis_sources(self.project)
            for analysis in analyses:
                compiled = self.compile_model(analysis_linker, analysis, models)
                if compiled:
                    compiled_analyses.append(compiled)

        return len(compiled_models), len(compiled_analyses)
