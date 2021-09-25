select avg(P.level)
from Trainer T, CatchedPokemon P
where T.id = P.owner_id and
      T.name = 'Red';