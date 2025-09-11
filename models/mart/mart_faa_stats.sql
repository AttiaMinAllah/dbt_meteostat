with departures as(
select
    origin as airport_code,
    count(distinct dest) nunique_to,
    count(sched_dep_time) dep_planned,
    sum(cancelled) dep_cancelled,
    sum(pf.diverted) dep_diverted,
    count(dep_time) dep_n_flights
from
    {{ref('prep_flights')}} as pf
group by
    origin
    ),
arrivals as (
select 
    dest as airport_code,
    count(distinct origin) nunique_from,
    count(sched_dep_time) arr_planned,
    sum(cancelled) arr_cancelled,
    sum(pf.diverted) arr_diverted,
    count(arr_time) arr_n_flights
from
    {{ref('prep_flights')}} as pf
group by
    dest
    ),
    total_stats as (
select
    airport_code,
    nunique_to,
    nunique_from,
    (dep_planned + arr_planned) total_planned,
    (dep_cancelled + arr_cancelled) total_cancelled,
    (dep_diverted + arr_diverted) total_diverted,
    (dep_n_flights + arr_n_flights)total_flights
from
    departures d
join arrivals a
        using (airport_code)
    )
    select

    *
from
    total_stats ts
join {{ref('prep_airports')}} as a on
    ts.airport_code = a.faa