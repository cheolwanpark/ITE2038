select P.name
from Trainer T, Gym G, CatchedPokemon C, Pokemon P
where G.city = 'Rainbow City' and
      T.id = G.leader_id and
      C.owner_id = T.id and
      C.pid = P.id
order by P.name;
