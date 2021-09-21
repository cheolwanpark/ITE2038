create view CatchedPokemonWithoutDup as
select owner_id, count(*) as count
from CatchedPokemon
group by owner_id, pid;

select distinct name
from Trainer T, CatchedPokemonWithoutDup P
where T.id = P.owner_id and
      P.count >= 2;