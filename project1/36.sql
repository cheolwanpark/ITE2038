select name
from Pokemon
where type = 'Water' and
      id not in (
        select P.id
        from Pokemon P, Evolution E
        where P.id = E.before_id
      ) and
      id in (
        select P.id
        from Pokemon P, Evolution E
        where P.id = E.before_id or
              P.id = E.after_id
      );