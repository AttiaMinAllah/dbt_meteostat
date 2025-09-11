WITH departures AS (
    SELECT 
        origin AS airport_code,   -- departures are from origin
        COUNT(DISTINCT dest) AS nunique_to,
        COUNT(sched_dep_time) AS dep_planned,
        SUM(cancelled) AS dep_cancelled,
        SUM(diverted) AS dep_diverted,
        COUNT(dep_time) AS dep_n_flights
    FROM prep_flights
    GROUP BY origin
),
arrivals AS (
    SELECT 
        dest AS airport_code,     -- arrivals are into dest
        COUNT(DISTINCT origin) AS nunique_from,
        COUNT(sched_arr_time) AS arr_planned,
        SUM(cancelled) AS arr_cancelled,
        SUM(diverted) AS arr_diverted,
        COUNT(arr_time) AS arr_n_flights
    FROM prep_flights
    GROUP BY dest
),
total_stats AS (
    SELECT 
        d.airport_code,
        d.nunique_to,
        a.nunique_from,
        (d.dep_planned + a.arr_planned) AS total_planned,
        (d.dep_cancelled + a.arr_cancelled) AS total_cancelled,
        (d.dep_diverted + a.arr_diverted) AS total_diverted,
        (d.dep_n_flights + a.arr_n_flights) AS total_flights
    FROM departures d
    JOIN arrivals a 
      ON d.airport_code = a.airport_code
)
SELECT 
    ap.city, 
    ap.country, 
    ap.name,
    ts.*
FROM total_stats ts
JOIN prep_airports ap
  ON ts.airport_code = ap.faa;   -- make sure prep_airports has column `faa`
