select P.name
from Pokemon P
where P.id not in (
  select distinct pid
  from CatchedPokemon
)
order by P.name;

