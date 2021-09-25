create view CatchedWaterPokemon as
select level
from Pokemon P, CatchedPokemon Catched
where P.id = Catched.pid and
      P.type = 'Water';

select avg(level)
from CatchedWaterPokemon;