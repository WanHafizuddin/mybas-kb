-- Average and 90th-percentile delay by local hour of day.
SELECT
  f.local_hour,
  COUNT(*)                                                   AS n_arrivals,
  AVG(f.delay_seconds) / 60                                  AS avg_delay_minutes,
  APPROX_QUANTILES(f.delay_seconds, 10)[OFFSET(9)] / 60      AS p90_delay_minutes
FROM `mybas-tracker-504819.basmy_real.fact_arrivals` AS f
WHERE f.service_date BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
GROUP BY f.local_hour
HAVING n_arrivals >= 20
ORDER BY f.local_hour
