create view TrainerTotalLevel as
select P.owner_id as trainer_id, sum(P.level) as total_level
from CatchedPokemon P
group by P.owner_id;

select T.name, TTL.total_level
from Trainer T, TrainerTotalLevel TTL
where T.id = TTL.trainer_id and
      TTL.total_level = (
        select max(total_level)
        from TrainerTotalLevel
      );