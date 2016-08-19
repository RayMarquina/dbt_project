
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
        return "SKIP relation {}.{} because parent failed".format(model.target.schema, model.name)

    def post_run_msg(self, result):
        raise NotImplementedError("not implemented")

    def pre_run_all_msg(self, models):
        raise NotImplementedError("not implemented")

    def post_run_all_msg(self, results):
        raise NotImplementedError("not implemented")

    def post_run_all(self, schema, results):
        pass

    def pre_run_all(self, models):
        pass

    def status(self, result):
        raise NotImplementedError("not implemented")

class ModelRunner(BaseRunner):
    def pre_run_msg(self, model):
        print_vars = {
            "schema": model.target.schema,
            "model_name": model.name,
            "model_type": model.materialization,
            "info": "START"
        }

        output = "START {model_type} model {schema}.{model_name} ".format(**print_vars)
        return output

    def post_run_msg(self, result):
        model = result.model
        print_vars = {
            "schema": model.target.schema,
            "model_name": model.name,
            "model_type": model.materialization,
            "info": "ERROR creating" if result.errored else "OK created"
        }

        output = "{info} {model_type} model {schema}.{model_name} ".format(**print_vars)
        return output

    def pre_run_all_msg(self, models):
        return "Running {} models".format(len(models))

    def post_run_all_msg(self, results):
        return "Finished running {} models".format(len(results))

    def status(self, result):
        return result.status

    def execute(self, schema, target, model):
        if model.tmp_drop_type is not None:
            schema.drop(target.schema, model.tmp_drop_type, model.tmp_name)

        status = schema.execute_and_handle_permissions(model.contents, model.name)

        if model.final_drop_type is not None:
            schema.drop(target.schema, model.final_drop_type, model.name)

        if model.should_rename():
            schema.rename(target.schema, model.tmp_name, model.name)

        return status

class DryRunner(ModelRunner):
    def pre_run_msg(self, model):
        output = "DRY-RUN model {schema}.{model_name} ".format(schema=model.target.schema, model_name=model.name)
        return output

    def post_run_msg(self, result):
        model = result.model
        output = "DONE model {schema}.{model_name} ".format(schema=model.target.schema, model_name=model.name)
        return output

    def pre_run_all_msg(self, models):
        return "Dry-running {} models".format(len(models))

    def post_run_all_msg(self, results):
        return "Finished dry-running {} models".format(len(results))

    def post_run_all(self, schema, results):
        for result in results:
            if result.errored or result.skipped:
                continue
            model = result.model
            schema_name = model.target.schema

            relation_type = 'table' if model.materialization == 'incremental' else 'view'
            schema.drop(schema_name, relation_type, model.name)

class TestRunner(ModelRunner):
    def pre_run_msg(self, model):
        return "TEST {name} ".format(name=model.name)

    def post_run_msg(self, result):
        model = result.model
        info = self.status(result)

        return "{info} {name} ".format(info=info, name=model.name)

    def pre_run_all_msg(self, models):
        return "Running {} tests".format(len(models))

    def post_run_all_msg(self, results):
        total = len(results)
        passed  = len([result for result in results if not result.errored and not result.skipped])
        errored = len([result for result in results if result.errored])
        skipped = len([result for result in results if result.skipped])
        overview = "PASS={passed} ERROR={errored} SKIP={skipped} TOTAL={total}".format(total=total, passed=passed, errored=errored, skipped=skipped)
        if errored > 0:
            final = "Tests completed with errors"
        else:
            final = "All tests passed"

        return "\n{overview}\n{final}".format(overview=overview, final=final)

    def status(self, result):
        if result.errored:
            info = "ERROR"
        elif result.status > 0:
            info = 'FAIL {}'.format(result.status)
        elif result.status == 0:
            info = 'PASS'
        else:
            raise RuntimeError("unexpected status: {}".format(result.status))

        return info

    def execute(self, schema, target, model):
        rows = schema.execute_and_fetch(model.contents)
        if len(rows) > 1:
            raise RuntimeError("Bad test {name}: Returned {num_rows} rows instead of 1".format(name=model.name, num_rows=len(rows)))

        row = rows[0]
        if len(row) > 1:
            raise RuntimeError("Bad test {name}: Returned {num_cols} cols instead of 1".format(name=model.name, num_cols=len(row)))

        return row[0]

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

    def execute_model(self, runner, model):
        self.logger.info("executing model %s", model)
        return runner.execute(self.schema, self.target, model)

    def safe_execute_model(self, data):
        runner, model = data['runner'], data['model']

        error = None
        try:
            status = self.execute_model(runner, model)
        except (RuntimeError, psycopg2.ProgrammingError) as e:
            error = "Error executing {filepath}\n{error}".format(filepath=model['build_path'], error=str(e).strip())
            status = "ERROR"
        except Exception as e:
            error = "Unhandled error while executing {filepath}\n{error}".format(filepath=model['build_path'], error=str(e).strip())
            self.logger.exception(error)
            raise e

        return RunModelResult(model, error=error, status=status)

    def as_concurrent_dep_list(self, linker, models, existing, target, limit_to):
        model_dependency_list = []
        dependency_list = linker.as_dependency_list(limit_to)
        for node_list in dependency_list:
            level = []
            for fqn in node_list:
                try:
                    model = find_model_by_fqn(models, fqn)
                except RuntimeError as e:
                    continue
                if model.should_execute():
                    model.prepare(existing, target)
                    level.append(model)
            model_dependency_list.append(level)
        return model_dependency_list

    def on_model_failure(self, linker, models):
        def skip_dependent(model):
            dependent_nodes = linker.get_dependent_nodes(model.fqn)
            for node in dependent_nodes:
                model_to_skip = find_model_by_fqn(models, node)
                model_to_skip.do_skip()
        return skip_dependent

    def execute_models(self, runner, model_dependency_list, on_failure):
        flat_models = list(itertools.chain.from_iterable(model_dependency_list))

        num_models = len(flat_models)
        if num_models == 0:
            print("WARNING: No models to run in '{}'. Try checking your model configs and running `dbt compile`".format(self.target_path))
            return []

        num_threads = self.target.threads
        print("Concurrency: {} threads (target='{}')".format(num_threads, self.project['run-target']))
        print("Running!")

        pool = ThreadPool(num_threads)

        print(runner.pre_run_all_msg(flat_models))
        runner.pre_run_all(flat_models)

        model_results = []
        for model_list in model_dependency_list:
            for i, model in enumerate([model for model in model_list if model.should_skip()]):
                output = runner.skip_msg(model).ljust(80, ".")
                print("{} [SKIP]".format(output))
                model_result = RunModelResult(model, skip=True)
                model_results.append(model_result)

            models_to_execute = [model for model in model_list if not model.should_skip()]

            for i, model in enumerate(models_to_execute):
                msg = runner.pre_run_msg(model)
                output = msg.ljust(80, ".")
                print("{} [Running]".format(output))

            wrapped_models_to_execute = [{"runner": runner, "model": model} for model in models_to_execute]
            run_model_results = pool.map(self.safe_execute_model, wrapped_models_to_execute)
            #run_model_results = [self.safe_execute_model(runner, model) for model in models_to_execute]

            for run_model_result in run_model_results:
                model_results.append(run_model_result)

                msg = runner.post_run_msg(run_model_result)
                status = runner.status(run_model_result)
                output = msg.ljust(80, ".")

                print("{} [{}]".format(output, status))

                if run_model_result.errored:
                    on_failure(run_model_result.model)
                    print(run_model_result.error)

        pool.close()
        pool.join()

        print(runner.post_run_all_msg(model_results))
        runner.post_run_all(self.schema, model_results)

        return model_results

    def run(self, run_mode, limit_to=None):
        linker = self.deserialize_graph()

        if run_mode == 'test':
            runner = TestRunner()
        elif run_mode == 'run':
            runner = ModelRunner()
        elif run_mode == 'dry-run':
            runner = DryRunner()
        else:
            raise RuntimeError('invalid run_mode provided: {}'.format(run_mode))

        compiled_models = [make_compiled_model(fqn, linker.get_node(fqn)) for fqn in linker.nodes()]
        relevant_compiled_models = [m for m in compiled_models if m.data['dbt_run_type'] == run_mode]

        schema_name = self.target.schema

        try:
            self.schema.create_schema_if_not_exists(schema_name)
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(str(e))
            sys.exit(1)

        existing = self.schema.query_for_existing(schema_name);

        if limit_to is None:
            specified_models = None
        else:
            specified_models = [find_model_by_name(relevant_compiled_models, name).fqn for name in limit_to]
        model_dependency_list = self.as_concurrent_dep_list(linker, relevant_compiled_models, existing, self.target, specified_models)

        on_failure = self.on_model_failure(linker, relevant_compiled_models)
        return self.execute_models(runner, model_dependency_list, on_failure)
