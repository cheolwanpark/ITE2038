create view EvolvePokemon as
select id, name, before_id
from Pokemon P, Evolution E
where P.id = E.after_id;

select name
from EvolvePokemon
where before_id not in (
  select id
  from EvolvePokemon
)
order by name;