select sum(C.level)
from CatchedPokemon C, Pokemon P
where P.id = C.pid and
      P.type = 'Fire';