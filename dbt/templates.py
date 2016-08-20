
class BaseCreateTemplate(object):
    template = """
create {materialization} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
    {query}
);"""

    # Distribution style, sort keys,BACKUP, and NULL properties are inherited by LIKE tables,
    # but you cannot explicitly set them in the CREATE TABLE ... LIKE statement.
    # via http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
    incremental_template = """
create temporary table "{identifier}__dbt_incremental_tmp" {dist_qualifier} {sort_qualifier} as (
    select * from (
        {query}
    ) as tmp limit 0
);

create table if not exists "{schema}"."{identifier}" (like "{identifier}__dbt_incremental_tmp");

{incremental_delete_statement}

insert into "{schema}"."{identifier}" (
    with dbt_incr_sbq as (
        {query}
    )
    select * from dbt_incr_sbq
    where ({sql_where}) or ({sql_where}) is null
);
    """

    incremental_delete_template = """
delete from "{schema}"."{identifier}" where  ({unique_key}) in (
    with dbt_delete_sbq as (
        {query}
    )
    select ({unique_key}) from dbt_delete_sbq
    where ({sql_where}) or ({sql_where}) is null
);
"""

    label = "build"

    @classmethod
    def model_name(cls, base_name):
        return base_name

    def wrap(self, opts):
        sql = ""
        if opts['materialization'] in ('table', 'view'):
            sql = self.template.format(**opts)
        elif opts['materialization'] == 'incremental':
            if opts.get('unique_key') is not None:
                delete_sql = self.incremental_delete_template.format(**opts)
            else:
                delete_sql = "-- no unique key provided... skipping delete"

            opts['incremental_delete_statement'] = delete_sql
            sql = self.incremental_template.format(**opts)


        elif opts['materialization'] == 'ephemeral':
            sql = opts['query']
        else:
            raise RuntimeError("Invalid materialization parameter ({})".format(opts['materialization']))

        return "{}\n\n{}".format(opts['prologue'], sql)

class DryCreateTemplate(object):
    base_template = """
create view "{schema}"."{identifier}" as (
    SELECT * FROM (
        {query}
    ) as tmp LIMIT 0
);"""


    incremental_template = """
create temporary table "{identifier}__dbt_incremental_tmp" {dist_qualifier} {sort_qualifier} as (
    select * from (
        {query}
    ) as tmp limit 0
);

create table if not exists "{schema}"."{identifier}" (like "{identifier}__dbt_incremental_tmp");

{incremental_delete_statement}

insert into "{schema}"."{identifier}" (
    with dbt_incr_sbq as (
        {query}
    )
    select * from dbt_incr_sbq
    where ({sql_where}) or ({sql_where}) is null
    limit 0
);
    """

    incremental_delete_template = """
delete from "{schema}"."{identifier}" where  ({unique_key}) in (
    with dbt_delete_sbq as (
        {query}
    )
    select ({unique_key}) from dbt_delete_sbq
    where ({sql_where}) or ({sql_where}) is null
);
"""

    label = "test"

    @classmethod
    def model_name(cls, base_name):
        return 'test_{}'.format(base_name)

    def wrap(self, opts):
        sql = ""
        if opts['materialization'] in ('table', 'view'):
            sql = self.base_template.format(**opts)
        elif opts['materialization'] == 'incremental':
            if 'unique_key' in opts:
                delete_sql = self.incremental_delete_template.format(**opts)
            else:
                delete_sql = "-- no unique key provided... skipping delete"

            opts['incremental_delete_statement'] = delete_sql
            sql = self.incremental_template.format(**opts)

        elif opts['materialization'] == 'ephemeral':
            sql = opts['query']
        else:
            raise RuntimeError("Invalid materialization parameter ({})".format(opts['materialization']))

        return "{}\n\n{}".format(opts['prologue'], sql)
