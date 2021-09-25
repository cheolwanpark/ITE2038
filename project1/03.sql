select name
from Trainer T
where T.id not in (
  select G.leader_id
  from Gym G)
order by name