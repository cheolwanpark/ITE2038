select P2.name
from Pokemon P1, Pokemon P2, Evolution E1, Evolution E2
where P2.id = E2.after_id and
      P1.id = E1.before_id and
      P1.name = 'Charmander' and
      E1.after_id = E2.before_id;