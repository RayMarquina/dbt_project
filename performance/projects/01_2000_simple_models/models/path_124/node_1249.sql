select 1 as id
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_8') }}
union all
select * from {{ ref('node_17') }}
union all
select * from {{ ref('node_25') }}
union all
select * from {{ ref('node_40') }}
union all
select * from {{ ref('node_776') }}
union all
select * from {{ ref('node_1032') }}
