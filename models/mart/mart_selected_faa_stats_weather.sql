-- -- we want to see for each airport daily:
-- WITH flights_cleaned AS (
--     SELECT * 
--     FROM {{ ref('prep_flights') }}
-- ),
-- airports AS (
--     SELECT faa, city, country, name
--     FROM {{ ref('prep_airports') }}
-- ),
-- weather_daily AS (
--     SELECT * 
--     FROM {{ref('prep_weather_daily')}}
-- ),
-- route_stats AS (
--     SELECT
--         origin,
--         dest,
--         flight_date,
--         COUNT(DISTINCT origin) AS num_unique_departures,  --unique number of departures connections
--         COUNT(DISTINCT dest) AS num_unique_arrivals, -- unique number of arrival connections
--         COUNT(*) AS total_flights, -- total number of flights
--         SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled, -- total cancelled flights
--         SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted, -- total diverted flights
--         COUNT(*) AS actual_flights -- how many flights actually occured in total (departures & arrivals)
--     FROM flights_cleaned
--     GROUP BY origin, dest, flight_date
-- ),
-- weather_stats AS (
--     SELECT
--     -- daily min temperature
--     -- daily max temperature
--     -- daily precipitation
--     -- daily snow fall
--     -- daily average wind direction
--     -- daily average wind speed
--     -- daily wnd peakgust
--         date,
--         airport_code,
--         min_temp_c,
--         max_temp_c,
--         precipitation_mm,
--         max_snow_mm,
--         avg_wind_direction,
--         avg_wind_speed_kmh,
--         wind_peakgust_kmh
--     FROM weather_daily
-- ),
-- joined_airports_weather AS (
--     SELECT 
--         ws.date,
--         ws.airport_code,
--         a.city AS airport_city,
--         a.name AS airport_name,
--         a.country AS airport_country,
--         -- Wetter
--         ws.min_temp_c,
--         ws.max_temp_c,
--         ws.precipitation_mm,
--         ws.max_snow_mm,
--         ws.avg_wind_direction,
--         ws.avg_wind_speed_kmh,
--         ws.wind_peakgust_kmh
--     FROM weather_stats ws
--     LEFT JOIN airports a ON ws.airport_code = a.faa
--     LEFT JOIN route_stats rs ON ws.airport_code = rs.origin AND ws.date = rs.flight_date
-- )

-- -- Ergebnis: alles täglich pro Flughafen
-- SELECT *
-- FROM joined_airports_weather;


--Heikos Code
WITH flights_cleaned AS (
    SELECT * 
    FROM {{ ref('prep_flights') }}
),
weather_daily AS (
    SELECT * 
    FROM {{ ref('prep_weather_daily') }}
),
airports AS (
    SELECT faa, city, country, name
    FROM {{ ref('prep_airports') }}
),
flights_per_airport_day AS (
    SELECT
        flight_date,
        airport AS airport_code,
        COUNT(DISTINCT CASE WHEN direction = 'dep' THEN dest END) AS num_unique_departures,
        COUNT(DISTINCT CASE WHEN direction = 'arr' THEN origin END) AS num_unique_arrivals,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS canceled_flights,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_flights,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS actual_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines
    FROM (
        -- Departures
        SELECT 
            flight_date,
            origin AS airport,
            dest,
            NULL::TEXT AS origin, -- needed for symmetry in UNION
            cancelled,
            diverted,
            tail_number,
            airline,
            'dep' AS direction
        FROM flights_cleaned

        UNION ALL

        -- Arrivals
        SELECT 
            flight_date,
            dest AS airport,
            NULL::TEXT AS dest,
            origin,
            cancelled,
            diverted,
            tail_number,
            airline,
            'arr' AS direction
        FROM flights_cleaned
    ) AS unioned_flights
    GROUP BY flight_date, airport
),
final AS (
    SELECT
        weather_daily.date AS flight_date,
        weather_daily.airport_code,
        flights_per_airport_day.num_unique_departures,
        flights_per_airport_day.num_unique_arrivals,
        flights_per_airport_day.total_flights,
        flights_per_airport_day.canceled_flights,
        flights_per_airport_day.diverted_flights,
        flights_per_airport_day.actual_flights,
        flights_per_airport_day.unique_airplanes,
        flights_per_airport_day.unique_airlines,
        weather_daily.min_temp_c,
        weather_daily.max_temp_c,
        weather_daily.precipitation_mm,
        weather_daily.max_snow_mm,
        weather_daily.avg_wind_direction,
        weather_daily.avg_wind_speed_kmh,
        weather_daily.wind_peakgust_kmh,
        airports.city,
        airports.country,
        airports.name AS airport_name
    FROM weather_daily
    LEFT JOIN flights_per_airport_day 
        ON weather_daily.airport_code = flights_per_airport_day.airport_code AND weather_daily.date = flights_per_airport_day.flight_date
    LEFT JOIN airports 
        ON weather_daily.airport_code = airports.faa
)

SELECT *
FROM final
ORDER BY flight_date, airport_code