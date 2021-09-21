create view TypeCount as
select id, count(*) as type_count
from (
  select C.owner_id as id, P.type as type
  from CatchedPokemon C, Pokemon P
  where C.pid = P.id
  group by C.owner_id, P.type
) as A
group by id;

create view OneTypeTrainer as
select T.id, T.name
from Trainer T, TypeCount TC
where T.id = TC.id and
      TC.type_count = 1;

create view PokemonWithoutDup as
select pid, owner_id, count(*) as count
from CatchedPokemon
group by owner_id, pid;

select OTT.name, P.name, PWD.count
from OneTypeTrainer OTT, PokemonWithoutDup PWD, Pokemon P
where PWD.owner_id = OTT.id and
      PWD.pid = P.id
order by OTT.name;