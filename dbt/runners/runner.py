
from __future__ import print_function

import psycopg2
import os, sys
import logging
import time
import itertools

from dbt.compilation import Compiler
from dbt.linker import Linker
from dbt.templates import BaseCreateTemplate
from dbt.targets import RedshiftTarget
from dbt.source import Source
from dbt.utils import find_model_by_fqn, find_model_by_name, dependency_projects
from dbt.compiled_model import make_compiled_model
import dbt.schema

from multiprocessing.dummy import Pool as ThreadPool

class RunModelResult(object):
    def __init__(self, model, error=None, skip=False, status=None):
        self.model = model
        self.error = error
        self.skip  = skip
        self.status = status

    @property
    def errored(self):
        return self.error is not None

    @property
    def skipped(self):
        return self.skip

class BaseRunner(object):
    def pre_run_msg(self, model):
        raise NotImplementedError("not implemented")

    def skip_msg(self, model):

    def post_run_msg(self, model):
        raise NotImplementedError("not implemented")

    @classmethod
    def pre_run_all_msg(cls, models):
        raise NotImplementedError("not implemented")

    @classmethod
    def post_run_all_msg(cls, models):
        raise NotImplementedError("not implemented")

    @classmethod
    def post_run_all(cls, models):
        pass

    @classmethod
    def pre_run_all(cls, models):
        pass

class ModelRunner(BaseRunner):
    def pre_run_msg(self, model):
        print("pre", model)

    def post_run_msg(self, model):
        print("post", model)

    @classmethod
    def pre_run_all_msg(cls, models):
        print("pre all", models)

    @classmethod
    def post_run_all_msg(cls, models):
        print("post all", models)

class DryRunner(object):
    pass

class TestRunner(object):
    pass

class RunManager(object):
    def __init__(self, project, target_path, run_mode):
        self.logger = logging.getLogger(__name__)
        self.project = project
        self.target_path = target_path
        self.run_mode = run_mode

        self.target = RedshiftTarget(self.project.run_environment())
        self.schema = dbt.schema.Schema(self.project, self.target)

    def deserialize_graph(self):
        linker = Linker()
        base_target_path = self.project['target-path']
        filename = 'graph-{}.yml'.format(self.run_mode)
        graph_file = os.path.join(base_target_path, filename)
        linker.read_graph(graph_file)

        return linker

    def execute_model(self, model):
        self.logger.info("executing model %s", model)

        if model.tmp_drop_type is not None:
            self.schema.drop(self.target.schema, model.tmp_drop_type, model.tmp_name)

        status = self.schema.execute_and_handle_permissions(model.contents, model.name)

        if model.final_drop_type is not None:
            self.schema.drop(self.target.schema, model.final_drop_type, model.name)

        if model.should_rename():
            self.schema.rename(model.target.schema, model.tmp_name, model.name)

        return status

    def safe_execute_model(self, model):
        error = None
        try:
            status = self.execute_model(model)
        except (RuntimeError, psycopg2.ProgrammingError) as e:
            error = "Error executing {filepath}\n{error}".format(filepath=model['build_path'], error=str(e).strip())
            status = "ERROR"
        except Exception as e:
            error = "Unhandled error while executing {filepath}\n{error}".format(filepath=model['build_path'], error=str(e).strip())
            self.logger.exception(error)
            raise e

        return RunModelResult(model, error=error, status=status)


    def as_concurrent_dep_list(self, linker, models, existing, target):
        model_dependency_list = []
        dependency_list = linker.as_dependency_list()
        for node_list in dependency_list:
            level = []
            for fqn in node_list:
                model = find_model_by_fqn(models, fqn)
                if model.should_execute():
                    model.prepare(existing, target)
                    level.append(model)
            model_dependency_list.append(level)
        return model_dependency_list

    def on_model_failure(self, linker, model, models):
        dependent_nodes = linker.get_dependent_nodes(model.fqn)
        for node in dependent_nodes:
            model_to_skip = find_model_by_fqn(models, node)
            print("DEBUG: skipping model {} because {} failed".format(model_to_skip, model))
            model_to_skip.skip()

    def execute_models(self, linker, runner, model_dependency_list):
        flat_models = list(itertools.chain.from_iterable(model_dependency_list))
        num_models = len(flat_models)

        if num_models == 0:
            print("WARNING: No models to run in '{}'. Try checking your model configs and running `dbt compile`".format(self.target_path))
            return []

        num_threads = self.target.threads
        print("Concurrency: {} threads (target='{}')".format(num_threads, self.project['run-target']))
        print("Running!")

        pool = ThreadPool(num_threads)

        model_results = []
        for model_list in model_dependency_list:
            # TODO : do this differently? mabye it should be a method on the Runner
            for i, model in enumerate([model for model in model_list if model.should_skip()]):
                model_result = RunModelResult(model, skip=True)
                model_results.append(model_result)
                print("{} of {} -- SKIP relation {}.{} because parent failed".format(len(model_results), num_models, self.target.schema, model_result.model.name))

            models_to_execute = [model for model in model_list if not model.should_skip()]
            for i, model in enumerate(models_to_execute):
                print_vars = {
                    "progress": 1 + i + len(model_results),
                    "total" : num_models,
                    "schema": self.target.schema,
                    "model_name": model.name,
                    "model_type": model.materialization,
                    "info": "START"
                }

                output = "{progress} of {total} -- {info} {model_type} model {schema}.{model_name} ".format(**print_vars)
                print("{} [Running]".format(output.ljust(80, ".")))

            #run_model_results = pool.map(self.safe_execute_model, models_to_execute)
            run_model_results = [self.safe_execute_model(model) for model in models_to_execute]

            for run_model_result in run_model_results:
                model_results.append(run_model_result)

                print_vars = {
                    "progress": len(model_results),
                    "total" : num_models,
                    "schema": self.target.schema,
                    "model_name": run_model_result.model.name,
                    "model_type": run_model_result.model.materialization,
                    "info": "ERROR creating" if run_model_result.errored else "OK created"
                }

                output = "{progress} of {total} -- {info} {model_type} model {schema}.{model_name} ".format(**print_vars)
                print("{} [{}]".format(output.ljust(80, "."), run_model_result.status))

                if run_model_result.errored:
                    print(run_model_result.error)

        pool.close()
        pool.join()

        return model_results

    def run(self, specified_models=None):
        linker = self.deserialize_graph()

        if specified_models is not None:
            raise NotImplementedError("TODO")

        compiled_models = [make_compiled_model(fqn, linker.get_node(fqn)) for fqn in linker.nodes()]

        schema_name = self.target.schema

        try:
            self.schema.create_schema_if_not_exists(schema_name)
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)

        existing = self.schema.query_for_existing(schema_name);
        model_dependency_list = self.as_concurrent_dep_list(linker, compiled_models, existing, self.target)

        runner = ModelRunner()

        return self.execute_models(linker, runner, model_dependency_list)
