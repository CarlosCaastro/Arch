-- Qual a média de passageiros (passenger_count) por hora e por dia considerando todos os yellow taxis da frota.
--MÉDIA POR DIA

WITH COUNT_PASSENGER AS (
    SELECT 
        date_trunc('day', T."TPEP_PICKUP_DATETIME") AS PICKUP_DAY,
        SUM(T."PASSENGER_COUNT") AS TT_PASSENGER
    FROM gold.f_yellow_taxi T
    GROUP BY 1
)
SELECT
    AVG(TT_PASSENGER)
FROM COUNT_PASSENGER;

--MÉDIA POR HORA

WITH COUNT_PASSENGER AS (
    SELECT 
        CAST(date_trunc('day', T."TPEP_PICKUP_DATETIME") AS DATE) AS PICKUP_DAY,
        date_trunc('hour', T."TPEP_PICKUP_DATETIME") AS PICKUP_HOUR,
        SUM(T."PASSENGER_COUNT") AS TT_PASSENGER
    FROM gold.f_yellow_taxi T
    GROUP BY 1, 2
)
SELECT
    AVG(TT_PASSENGER)
FROM COUNT_PASSENGER;