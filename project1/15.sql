create view CatchedPokemonWithName as
select owner_id, name
from CatchedPokemon Catched, Pokemon P
where Catched.pid = P.id;

select T.name
from Trainer T, CatchedPokemonWithName as P
where hometown = 'Sangnok City' and
      T.id = P.owner_id and
      P.name like 'P%'
order by T.name;
