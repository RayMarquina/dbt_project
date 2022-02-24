with acc
as(
select * from 
{{source('sharex','account')}}

)
select * from acc