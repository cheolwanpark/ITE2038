select sum(level)
from (
  select level
  from CatchedPokemon P, Trainer T
  where P.owner_id = T.id and
        T.name = 'Matis'
) as P;