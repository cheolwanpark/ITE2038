select name
from Pokemon
where name like 'a%' or
      name like 'e%' or
      name like 'i%' or
      name like 'o%' or
      name like 'u%'
order by name;