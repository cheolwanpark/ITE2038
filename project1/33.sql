select distinct T.name
from Trainer T, CatchedPokemon C, Pokemon P
where T.id = C.owner_id and
      C.pid = P.id and
      P.type = 'Psychic'
order by T.name;