select P.name
from Pokemon P, Evolution E
where P.id = E.before_id and
      E.before_id > E.after_id;