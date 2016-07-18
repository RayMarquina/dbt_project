
import os.path

class Model(object):
    def __init__(self, model_dir, rel_filepath):
        self.filepath = os.path.join(model_dir, rel_filepath)

        self.fqn = self.get_fqn(self.filepath)
        self.name = self.fqn[-1]
        self.contents = self.get_contents(self.filepath)

    def name(self):
        return self._name

    def get_fqn(self, filepath):
        parts = filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return parts[:-1] + [name]

    def get_contents(self, filepath):
        with open(filepath) as fh:
            return fh.read().strip()

    def __repr__(self):
        return "<Model {}: {}>".format(self.name, self.filepath)

class CompiledModel(Model):
    def __init__(self, target_dir, rel_filepath):
        return super(CompiledModel, self).__init__(target_dir, rel_filepath)

    def __repr__(self):
        return "<CompiledModel {}: {}>".format(self.name, self.filepath)

class Schema(Model):
    def __init__(self, target_dir, rel_filepath):
        return super(Schema, self).__init__(target_dir, rel_filepath)

    def __repr__(self):
        return "<Schema {}: {}>".format(self.name, self.filepath)
