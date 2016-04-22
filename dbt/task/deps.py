import os
import re
import yaml
import pprint
import subprocess
import dbt.project as project

class DepsTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def __pull_repo(self, repo):
        proc = subprocess.Popen(
            ['git', 'clone', repo],
            cwd=self.project['modules-path'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        out, err = proc.communicate()

        exists = re.match("fatal: destination path '(.+)' already exists", err)
        folder = None
        if exists:
            folder = exists.group(1)
            print "updating existing dependency {}".format(folder)
            full_path = os.path.join(self.project['modules-path'], folder)
            proc = subprocess.Popen(
                ['git', 'fetch', '--all'],
                cwd=full_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            out, err = proc.communicate()
            proc = subprocess.Popen(
                ['git', 'reset', '--hard', 'origin/master'],
                cwd=full_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            out, err = proc.communicate()
        else:
            matches = re.match("Cloning into '(.+)'", err)
            folder = matches.group(1)
            print "pulled new dependency {}".format(folder)

        return folder

    def __pull_deps_recursive(self, repos):
        for repo in repos:
            dep_folder = self.__pull_repo(repo)
            dep_project = project.read_project(
                os.path.join(self.project['modules-path'],
                             dep_folder,
                             'dbt_project.yml')
            )
            self.__pull_deps_recursive(dep_project['repositories'])

    def run(self):
        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

        self.__pull_deps_recursive(self.project['repositories'])
