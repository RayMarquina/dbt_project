
import dbt.project
import os

from dbt.compilation import Compiler

class CompileTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def __ensure_paths_exist(self):
        if not os.path.exists(self.project['target-path']):
            os.makedirs(self.project['target-path'])

        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

    def __dependency_projects(self):
        for obj in os.listdir(self.project['modules-path']):
            full_obj = os.path.join(self.project['modules-path'], obj)
            if os.path.isdir(full_obj):
                project = dbt.project.read_project(os.path.join(full_obj, 'dbt_project.yml'))
                yield project


    def run(self):
        compiler = Compiler(self.project)

        self.__ensure_paths_exist()
        sources = compiler.project_sources(self.project)

        for project in self.__dependency_projects():
            sources.update(compiler.project_sources(project))

        project_models = compiler.project_models(sources)
        linker = compiler.compile(sources, project_models)

        graph_path = os.path.join(self.project['target-path'], 'graph.yml')
        linker.write_graph(graph_path)
