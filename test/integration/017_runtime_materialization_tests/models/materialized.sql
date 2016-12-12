{{
  config(
    materialized = "table"
  )
}}

-- this is a unicode character: å
select * from "runtime_materialization_017"."seed"
