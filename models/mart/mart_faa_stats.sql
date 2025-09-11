select * from flights 
    count(distinct dest)
From prep_flights 
group by origin

select dest
    count(DISTINCT origin)
from prep_flights
group by dest