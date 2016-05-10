import pprint
import os

SAMPLE_CONFIG = """
name: 'package_name'
version: '1.0'

source-paths: ["models"]   # paths with source code to compile
target-path: "target"      # path for compiled code
clean-targets: ["target"]  # directories removed by the clean task
test-paths: ["test"]       # where to store test results

# default paramaters that apply to _all_ models (unless overridden below)
model-defaults:
  enabled: true           # enable all models by default
  materialized: false     # If true, create tables. If false, create views

# specify per-model configs
#models:
#  pardot:                 # assuming pardot is listed in the models/ directory
#    enabled: false        # disable all pardot models except where overriden
#    pardot_emails:        # override the configs for the pardot_emails model
#      enabled: true       # enable this specific model
#      materialized: true  # create a table instead of a view

# uncomment below and add real repositories to add dependencies to this project
#repositories:
#  - "git@github.com:analyst-collective/repo-name-1"
#  - "git@github.com:analyst-collective/repo-name-2"
"""

GIT_IGNORE = """
target/
dbt_modules/
"""

class InitTask:
    def __init__(self, args, project=None):
        self.args = args
        self.project = project

    def __write(self, path, filename, contents):
        file_path = os.path.join(path, filename)

        with open(file_path, 'w') as fh:
            fh.write(contents)

    def run(self):
        project_dir = self.args.project_name

        if os.path.exists(project_dir):
            raise RuntimeError("directory {} already exists!".format(project_dir))

        os.mkdir(project_dir)

        project_dir = self.args.project_name
        self.__write(project_dir, 'dbt_project.yml', SAMPLE_CONFIG)
        self.__write(project_dir, '.gitignore', GIT_IGNORE)

        dirs = ['models', 'analysis', 'tests']
        for dir_name in dirs:
            dir_path = os.path.join(project_dir, dir_name)
            os.mkdir(dir_path)
