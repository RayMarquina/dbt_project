
import os.path
import fnmatch
from dbt.model import Model, CompiledModel, Schema

class Source():
    def __init__(self, project_root):
        self.project_root = project_root

    def find(self, source_paths, file_pattern):
        """returns abspath, relpath, filename of files matching file_regex in source_paths"""
        found = []
        for source_path in source_paths:
            root_path = os.path.join(self.project_root, source_path)
            for root, dirs, files in os.walk(root_path):
                for filename in files:
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, root_path)

                    if fnmatch.fnmatch(filename, file_pattern):
                        found.append((source_path, rel_path))
        return found

    def get_models(self, model_dirs):
        pattern = "[!.#~]*.sql"
        models = [Model(*paths) for paths in self.find(model_dirs, pattern)]
        return models

    def get_compiled(self, target_dir, compilation_type):
        "Get compiled SQL files. compilation_type E {build, test}"
        pattern = "[!.#~]*.sql"
        source_dir = os.path.join(target_dir, compilation_type)
        compiled_models = [CompiledModel(*paths) for paths in self.find([source_dir], pattern)]
        return compiled_models

    def get_schemas(self, model_dirs):
        "Get schema.yml files"
        pattern = "[!.#~]*.yml"
        schemas = [Schema(*paths) for paths in self.find(model_dirs, pattern)]
        return schemas


# compilation.py: map of abspath to model root --> (project, model_rel_path)
# args: project, paths
#['/Users/Drew/Desktop/analyst_collective/test-dbt/models'] --> 'idk/people.sql'

# runner.py: list of rel paths to compiled files
# args: none
#['idk/accounts.sql', 'idk/people.sql', 'idk/people_accounts.sql'] 

# schema_tester.py: map of model root --> parsed yaml file
# args: none
#{'idk': {'accounts': {'constraints': {'accepted-values': [{'field': 'type', 'values': ['paid', 'free']}]}}, 'people': {'constraints': {'relationships': [{'to': 'accounts', 'from': 'account_id', 'field': 'id'}], 'not_null': ['id', 'account_id', 'name'], 'unique': ['id'], 'accepted-values': [{'field': 'type', 'values': ['paid', 'free']}]}}}}

# seeder.py: list of abs path to CSV files in specified dir
# args: none
#['/Users/Drew/Desktop/analyst_collective/test-dbt/data/iris3.csv']

# source.py:
