WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
        , timestamp::TIME AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp::DATE,'day') AS weekday        -- weekday name as TEXT
        , DATE_PART('day', timestamp) AS date_day
        , TO_CHAR(timestamp::DATE,'month') AS date_month
        , TO_CHAR(timestamp::DATE,'year') AS  date_year
        , DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        ,(CASE 
            WHEN time BETWEEN '22:00:00' AND '00:00:00' THEN 'night'
            when time between '00:00:00' and '06:00:00' then 'night'
            WHEN time between '06:00:00' and '18:00:00' THEN 'day'
            when time between '18:00:00' and '22:00:00' THEN 'evening'
        END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features