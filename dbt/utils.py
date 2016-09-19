
import os
import dbt.project

class This(object):
    def __init__(self, schema, table):
        self.schema = schema
        self.table = table
        self.grant_name = self.schema_table(self.schema, "{}__dbt_tmp".format(self.table))

    def schema_table(self, schema, table):
        return '"{}"."{}"'.format(schema, table)

    def __repr__(self):
        return self.schema_table(self.schema, self.table)

def find_model_by_name(models, name, package_namespace=None):
    found = []
    for model in models:
        if model.name == name:
            if package_namespace is None:
                found.append(model)
            elif package_namespace is not None and package_namespace == model.project['name']:
                found.append(model)

    nice_package_name = 'ANY' if package_namespace is None else package_namespace
    if len(found) == 0:
        raise RuntimeError("Can't find a model named '{}' in package '{}' -- does it exist?".format(name, nice_package_name))
    elif len(found) == 1:
        return found[0]
    else:
        raise RuntimeError("Model specification is ambiguous: model='{}' package='{}' -- {} models match criteria: {}".format(name, nice_package_name, len(found), found))

def find_model_by_fqn(models, fqn):
    for model in models:
        if tuple(model.fqn) == tuple(fqn):
            return model
    raise RuntimeError("Couldn't find a compiled model with fqn: '{}'".format(fqn))

def dependency_projects(project):
    for obj in os.listdir(project['modules-path']):
        full_obj = os.path.join(project['modules-path'], obj)
        if os.path.isdir(full_obj):
            yield dbt.project.read_project(os.path.join(full_obj, 'dbt_project.yml'))

def split_path(path):
    norm = os.path.normpath(path)
    return path.split(os.sep)
