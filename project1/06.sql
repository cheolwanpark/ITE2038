select name, LevelInfo.avg_level
from Trainer T, Gym G,
(select T.id as trainer_id, avg(P.level) as avg_level
 from Trainer T, CatchedPokemon P
 where T.id = P.owner_id
 group by T.id) as LevelInfo
where T.id = G.leader_id and
      T.id = LevelInfo.trainer_id
order by name