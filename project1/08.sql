select P.type, count(*)
from Pokemon P, CatchedPokemon Catched
where P.id = Catched.pid
group by P.type;