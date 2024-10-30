select nickname
from CatchedPokemon
where owner_id >= 5 and
      level >= 40
order by nickname;