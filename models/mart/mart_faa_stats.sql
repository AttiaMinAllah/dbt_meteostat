WITH departures AS (
    SELECT 
        origin as airport_code,
        COUNT(DISTINCT origin) AS nunique_from,
        COUNT(sched_dep_time) AS dep_planned,
        SUM(cancelled) AS dep_cancelled,
        SUM(diverted) AS dep_diverted,
        COUNT(dep_time) AS dep_n_flights
    FROM {{ref('prep_flights')}}
),
arrivals AS (
    SELECT 
        dest as airport_code,
        COUNT(DISTINCT origin) AS nunique_to,
        COUNT(sched_arr_time) AS arr_planned,
        SUM(cancelled) AS arr_cancelled,
        SUM(diverted) AS arr_diverted,
        COUNT(arr_time) AS arr_n_flights
    FROM {{ref('prep_flights')}}
    GROUP BY dest
),
total_stats AS (
    SELECT 
        d.airport_code,
        a.nunique_to,
        d.nunique_from,
        (d.dep_planned + a.arr_planned) AS total_planned,
        (d.dep_cancelled + a.arr_cancelled) AS total_cancelled,
        (d.dep_diverted + a.arr_diverted) AS total_diverted,
        (d.dep_n_flights + a.arr_n_flights) AS total_flights
    FROM departures d
    JOIN arrivals a 
      using(airport_code)
)
SELECT a.city, a.country, a.name,
ts.* 
from total_stats ts
join {{ref('prep_flights')}} a
on ts.airport_code = a.faa

