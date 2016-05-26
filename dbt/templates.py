
class BaseCreateTemplate(object):
    template = """
    create {table_or_view} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
        {query}
    );"""

    label = "build"

    def wrap(self, opts):
        return self.template.format(**opts)

class TestCreateTemplate(object):
    template = """
    create table "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
        SELECT * FROM (
            {query}
        ) LIMIT 0
    );"""

    label = "test"

    def wrap(self, opts):
        opts['identifier'] = 'test_{}'.format(opts['identifier'])
        return self.template.format(**opts)


