import os
import errno
import re
import yaml
import pprint
import subprocess
import dbt.project as project

def folder_from_git_remote(remote_spec):
    start = remote_spec.rfind('/') + 1
    end = len(remote_spec) - (4 if remote_spec.endswith('.git') else 0)
    return remote_spec[start:end]

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

    def __pull_deps_recursive(self, repos, processed_repos = set()):
        for repo in repos:
            repo_folder = folder_from_git_remote(repo)
            try:
                if repo_folder in processed_repos:
                    print "skipping already processed dependency {}".format(repo_folder)
                else:
                    dep_folder = self.__pull_repo(repo)
                    dep_project = project.read_project(
                        os.path.join(self.project['modules-path'],
                                     dep_folder,
                                     'dbt_project.yml')
                    )
                    processed_repos.add(dep_folder)
                    self.__pull_deps_recursive(dep_project['repositories'], processed_repos)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    print "'{}' is not a valid dbt project - dbt_project.yml not found".format(repo)
                    exit(1)
                else:
                    raise e

    def run(self):
        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

        self.__pull_deps_recursive(self.project['repositories'])
