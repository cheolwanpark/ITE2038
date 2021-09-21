select T.name
from Trainer T, Gym G
where T.id = G.leader_id and
      G.city = 'Brown City';