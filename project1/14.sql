create view CityInfo as
select C.name as name, count(*) as count
from City C, Trainer T
where C.name = T.hometown
group by C.name;

select name
from CityInfo
where count = (
  select Max(count)
  from CityInfo
);