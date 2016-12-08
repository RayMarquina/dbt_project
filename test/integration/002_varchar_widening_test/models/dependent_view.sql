
{{
    config(materialized='view')
}}

select gender from {{ ref('materialized') }}
