-- On-time rate by route (Kuala Lumpur service dates in the report range).
SELECT
  CONCAT(r.route_short_name, ' ', r.route_long_name) AS route,
  COUNT(*)                                          AS n_arrivals,
  AVG(IF(f.is_on_time, 1, 0))                       AS on_time_rate,
  AVG(f.delay_seconds) / 60                         AS avg_delay_minutes
FROM `mybas-tracker-504819.basmy_real.fact_arrivals` AS f
JOIN `mybas-tracker-504819.basmy_real.dim_route` AS r USING (route_id)
WHERE f.service_date BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
GROUP BY route
HAVING n_arrivals >= 20
ORDER BY on_time_rate
