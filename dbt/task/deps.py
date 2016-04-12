import os
import yaml
import pprint
import subprocess


class DepsTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def __clone_or_update_repo(self, repo):
        p = subprocess.Popen(
            ['git', 'clone', repo],
            cwd=self.project['modules-path'])

        out = p.communicate()

    def run(self):
        pprint.pprint(self.project['modules-path'])
        if not os.path.exists(os.path.dirname(self.project['modules-path'])):
            os.makedirs(self.project['modules-path'])

        for repo in self.project['repositories']:
            self.__clone_or_update_repo(repo)
