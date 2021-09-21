select name
from Trainer as T, 
(select owner_id, count(*) as count
 from CatchedPokemon
 group by owner_id) as CountInfo
where T.id = CountInfo.owner_id and
      CountInfo.count >= 3
order by CountInfo.count;