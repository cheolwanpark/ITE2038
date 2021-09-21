create view TrainerPokemon as
select T.id as trainer_id,
       T.hometown as trainer_hometown,
       P.nickname as nickname, 
       P.level as level
from CatchedPokemon P, Trainer T
where P.owner_id = T.id;

create view MaxLevelInfo as
select C.name as city_name, max(TP.level) as level
from TrainerPokemon TP, City C
where TP.trainer_hometown = C.name
group by C.name;

select TP.trainer_hometown, TP.nickname
from TrainerPokemon TP, MaxLevelInfo LI
where TP.trainer_hometown = LI.city_name and
      TP.level = LI.level
order by nickname;
