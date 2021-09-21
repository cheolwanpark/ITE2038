select T.name, count(*)
from Trainer T, CatchedPokemon C
where T.id = C.owner_id
group by T.id
order by T.name;