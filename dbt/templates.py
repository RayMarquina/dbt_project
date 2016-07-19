
class BaseCreateTemplate(object):
    template = """
    create {table_or_view} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
        {query}
    );"""

    label = "build"

    @classmethod
    def model_name(cls, base_name):
        return base_name

    def wrap(self, opts):
        return self.template.format(**opts)

class TestCreateTemplate(object):
    template = """
    create view "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
        SELECT * FROM (
            {query}
        ) as tmp LIMIT 0
    );"""

    label = "test"

    @classmethod
    def model_name(cls, base_name):
        return 'test_{}'.format(base_name)

    def wrap(self, opts):
        return self.template.format(**opts)


