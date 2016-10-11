
class BaseCreateTemplate(object):
    template = """
create {materialization} "{schema}"."{identifier}" {dist_qualifier} {sort_qualifier} as (
    {query}
);"""

    # Distribution style, sort keys,BACKUP, and NULL properties are inherited by LIKE tables,
    # but you cannot explicitly set them in the CREATE TABLE ... LIKE statement.
    # via http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
    incremental_template = """
create temporary table "{identifier}__dbt_incremental_empty_tmp" {dist_qualifier} {sort_qualifier} as (
    select * from (
        {query}
    ) as tmp limit 0
);

create table if not exists "{schema}"."{identifier}" (like "{identifier}__dbt_incremental_empty_tmp");

create temporary table "{identifier}__dbt_incremental_tmp" as (
    with dbt_incr_sbq as (
        {query}
    )
    select * from dbt_incr_sbq
    where ({sql_where}) or ({sql_where}) is null
);

{incremental_delete_statement}

insert into "{schema}"."{identifier}" (
    select * from "{identifier}__dbt_incremental_tmp"
);
    """

    incremental_delete_template = """
delete from "{schema}"."{identifier}" where  ({unique_key}) in (
    select ({unique_key}) from "{identifier}__dbt_incremental_tmp"
);
"""

    extras_template = """
{prologue}

-- Pre-model hooks
{pre_hooks}

-- Model SQL
{sql}

-- Post-model hooks
{post_hooks}
"""

    label = "build"

    @classmethod
    def model_name(cls, base_name):
        return base_name

    def add_extras(self, opts, sql):
        pre_hooks = ';\n\n'.join(opts['pre-hooks'])
        post_hooks = ';\n\n'.join(opts['post-hooks'])

        if len(pre_hooks) > 0:
            pre_hooks = pre_hooks + ';'

        if len(post_hooks) > 0:
            post_hooks = post_hooks + ';'

        extras = {
            'prologue': opts['prologue'],
            'pre_hooks': pre_hooks,
            'sql': sql,
            'post_hooks': post_hooks,
        }

        return self.extras_template.format(**extras)

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

        return self.add_extras(opts, sql)


class DryCreateTemplate(object):
    base_template = """
create view "{schema}"."{identifier}" as (
    SELECT * FROM (
        {query}
    ) as tmp LIMIT 0
);"""


    incremental_template = """
create temporary table "{identifier}__dbt_incremental_empty_tmp" {dist_qualifier} {sort_qualifier} as (
    select * from (
        {query}
    ) as tmp limit 0
);


create table if not exists "{schema}"."{identifier}" (like "{identifier}__dbt_incremental_empty_tmp");

create temporary table "{identifier}__dbt_incremental_tmp" as (
    with dbt_incr_sbq as (
        {query}
    )
    select * from dbt_incr_sbq
    where ({sql_where}) or ({sql_where}) is null
    limit 0
);


{incremental_delete_statement}

insert into "{schema}"."{identifier}" (
    select * from "{identifier}__dbt_incremental_tmp"
);
    """

    incremental_delete_template = """
delete from "{schema}"."{identifier}" where  ({unique_key}) in (
    select ({unique_key}) from "{identifier}__dbt_incremental_tmp"
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



SCDArchiveTemplate = """
    with current_data as (

        select *,
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk
        from "{{ source_schema }}"."{{ source_table }}"

    ),

    archived_data as (

        select
            {% for (col, type) in columns %}
                "{{ col }}",
            {% endfor %}
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk
        from "{{ target_schema }}"."{{ dest_table }}"

    ),

    combined as (

        select
        {% for (col, type) in columns %}
            "{{ col }}",
        {% endfor %}
        dbt_updated_at,
        dbt_pk
        from current_data

            union all

        select
        {% for (col, type) in columns %}
            "{{ col }}",
        {% endfor %}
        dbt_updated_at,
        dbt_pk
        from archived_data

    ),

    merged as (

        select
        distinct
            combined.*,
            least(combined.dbt_updated_at, current_data.dbt_updated_at) as valid_from,
            case when combined.dbt_updated_at = current_data.dbt_updated_at then null
                else current_data.dbt_updated_at
            end as valid_to
        from current_data
        left outer join combined
             on combined.dbt_pk = current_data.dbt_pk
            and current_data.dbt_updated_at >= combined.dbt_updated_at

    ),
    with_id as (
        select *,
            row_number() over (partition by dbt_pk order by dbt_updated_at asc) as dbt_archive_id,
            count(*) over (partition by dbt_pk) as num_changes
        from merged
    )

    --
    -- TODO : The order of scd_id and dbt_updated_at depends
    --        on the order of col injections in archive.py
    select
        {% for (col, type) in columns %}
           "{{ col }}",
        {% endfor %}
           valid_from,
           valid_to,
           md5(dbt_pk || '|' || dbt_archive_id) as scd_id,
           dbt_updated_at

    from with_id
    where num_changes > 1
"""


class ArchiveInsertTemplate(object):

    label = "archive"

    archival_template = """
create temporary table "{identifier}__dbt_archival_tmp" as (
    with dbt_archive_sbq as (
        {query}
    )
    select * from dbt_archive_sbq
);

delete from "{schema}"."{identifier}" where ({unique_key}) in (
    select ({unique_key}) from "{identifier}__dbt_archival_tmp"
);

insert into "{schema}"."{identifier}" (
    select * from "{identifier}__dbt_archival_tmp"
);
"""

    def wrap(self, schema, table, query, unique_key):
        sql = self.archival_template.format(schema=schema, identifier=table, query=query, unique_key=unique_key)
        return sql

