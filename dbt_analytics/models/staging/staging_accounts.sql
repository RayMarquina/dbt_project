with acc
as(
select * from 
{{source('sharex','accounts')}}

)
select * from acc