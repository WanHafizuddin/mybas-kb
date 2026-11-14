-- Delay and on-time rate when it is raining at the stop vs dry, per route.
-- Arrivals with no weather value yet (archive lag) are left out.
SELECT
  r.route_short_name                         AS route,
  IF(f.is_raining, 'Raining', 'Dry')         AS weather,
  COUNT(*)                                   AS n_arrivals,
  AVG(f.delay_seconds) / 60                  AS avg_delay_minutes,
  AVG(IF(f.is_on_time, 1, 0))                AS on_time_rate
FROM `mybas-tracker-504819.basmy_real.fact_arrivals` AS f
JOIN `mybas-tracker-504819.basmy_real.dim_route` AS r USING (route_id)
WHERE f.service_date BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
  AND f.is_raining IS NOT NULL
GROUP BY route, weather
HAVING n_arrivals >= 20
ORDER BY route, weather
