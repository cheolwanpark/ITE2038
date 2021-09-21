create view SangnokPokemon as
select distinct P.name
from Trainer T, CatchedPokemon C, Pokemon P
where T.hometown = 'Sangnok City' and
      T.id = C.owner_id and
      C.pid = P.id;

create view BluePokemon as
select distinct P.name
from Trainer T, CatchedPokemon C, Pokemon P
where T.hometown = 'Blue City' and
      T.id = C.owner_id and
      C.pid = P.id;

select SP.name
from SangnokPokemon SP, BluePokemon BP
where SP.name = BP.name
order by SP.name;