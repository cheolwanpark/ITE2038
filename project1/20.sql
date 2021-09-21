select name
from Trainer T, Gym G
where T.id = G.leader_id
order by name;