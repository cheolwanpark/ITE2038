select T.name
from Trainer T, City C, Gym G
where T.id = G.leader_id and
      G.city = C.name and
      C.description = 'Amazon';