import os.path
import yaml
import pprint
import copy

default_project_cfg = {
    'source-paths': ['models'],
    'test-paths': ['test'],
    'target-path': 'target',
    'clean-targets': ['target'],
    'outputs': {'default': {}},
    'run-target': 'default',
    'models': {},
    'model-defaults': {
        "enabled": True,
        "materialized": False
    }
}

default_profiles = {
    'user': {}
}

default_active_profiles = ['user']

class Project:

    def __init__(self, cfg, profiles, active_profile_names=[]):
        self.cfg = default_project_cfg.copy()
        self.cfg.update(cfg)
        self.profiles = default_profiles.copy()
        self.profiles.update(profiles)
        self.active_profile_names = active_profile_names

        for profile_name in active_profile_names:
            self.cfg.update(self.profiles[profile_name])

    def __str__(self):
        return pprint.pformat({'project': self.cfg, 'profiles': self.profiles})

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        return self.cfg.__getitem__(key)

    def __contains__(self, key):
        return self.cfg.__contains__(key)

    def __setitem__(self, key, value):
        return self.cfg.__setitem__(key, value)

    def get(self, key, default=None):
        return self.cfg.get(key, default)

    def run_environment(self):
        target_name = self.cfg['run-target']
        return self.cfg['outputs'][target_name]

    def context(self):
        target_cfg = self.run_environment()
        filtered_target = copy.deepcopy(target_cfg)
        filtered_target.pop('pass')
        return {'env': target_cfg}

    def with_profiles(self, profiles=[]):
        return Project(
            copy.deepcopy(self.cfg),
            copy.deepcopy(self.profiles),
            profiles)


def read_profiles():
    profiles = {}
    paths = [
        os.path.join(os.path.expanduser('~'), '.dbt/profiles.yml')
    ]
    for path in paths:
        if os.path.isfile(path):
            with open(path, 'r') as f:
                m = yaml.safe_load(f)
                profiles.update(m)

    return profiles


def init_project(project_cfg):
    profiles = read_profiles()
    return Project(project_cfg, profiles, default_active_profiles)


def read_project(filename):
    with open(filename, 'r') as f:
        cfg = yaml.safe_load(f)
        return init_project(cfg)


def default_project():
    return init_project(default_project_cfg)
