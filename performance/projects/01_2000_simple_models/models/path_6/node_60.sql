select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_1') }}
union all
select * from {{ ref('node_56') }}
