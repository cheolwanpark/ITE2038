create view CityPokemon as
select nickname, type
from CatchedPokemon Catched, Gym G, Pokemon P
where G.city = 'Sangnok City' and
      G.leader_id = Catched.owner_id and
      Catched.pid = P.id;

select nickname
from CityPokemon
where type = 'Water'
order by nickname;