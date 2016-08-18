
class CompiledBase(object):
    def __init__(self, fqn, data):
        self.fqn = fqn
        self.data = data

        # these are set just before the models are executed
        self.tmp_drop_type = None
        self.final_drop_type = None
        self.target = None

    def __getitem__(self, key):
        return self.data[key]

    def should_execute(self):
        return True

    def should_rename(self):
        return False

    @property
    def contents(self):
        with open(self.data['build_path']) as fh:
            return fh.read()

    @property
    def materialization(self):
        return self.data['materialized']

    @property
    def name(self):
        return self.data['name']

    @property
    def tmp_name(self):
        return self.data['tmp_name']

    def project(self):
        return {'name': self.data['project_name']}

    @property
    def schema(self):
        if self.target is None:
            raise RuntimeError("`target` not set in compiled model {}".format(self))
        else:
            return self.target.schema

    def rename_query(self):
        raise NotImplementedError("not implemented")

    def __repr__(self):
        return "<CompiledBase {}.{}: {}>".format(self.data['project_name'], self.name, self.data['build_path'])

class CompiledModel(CompiledBase):
    def __init__(self, fqn, data):
        super(CompiledModel, self).__init__(fqn, data)

    def should_execute(self):
        return self.data['enabled'] and self.materialization != 'ephemeral'

    def should_rename(self):
        return not self.data['materialized'] == 'incremental' 

    def rename_query(self):
        return 'alter table "{schema}"."{tmp_name}" rename to "{final_name}"'.format(schema=self.schema, tmp_name=self.tmp_name, final_name=self.name)

    def __repr__(self):
        return "<CompiledModel {}.{}: {}>".format(self.data['project_name'], self.name, self.data['build_path'])


class CompiledTest(CompiledModel):
    def __init__(self, fqn, data):
        super(CompiledTest, self).__init__(fqn, data)

    def should_execute(self):
        return True

    def should_rename(self):
        return False

    def __repr__(self):
        return "<CompiledTest {}.{}: {}>".format(self.data['project_name'], self.name, self.data['build_path'])


