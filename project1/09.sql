create view TrainerInfo as
select T.id, T.name as name, count(*) as pokemon_count, max(P.level) as max_level
from Trainer T, CatchedPokemon P
where T.id = P.owner_id
group by T.id;

select T.name, P.nickname
from TrainerInfo T, CatchedPokemon P
where T.id = P.owner_id and
      T.pokemon_count >= 4 and
      T.max_level = P.level
order by P.nickname;