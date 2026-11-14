-- On-time rate by day of week x local hour (heatmap). Weighted by arrivals,
-- so it is computed from fact_arrivals rather than averaging the mart's rates.
SELECT
  f.day_of_week,
  CASE f.day_of_week WHEN 1 THEN '1 Mon' WHEN 2 THEN '2 Tue' WHEN 3 THEN '3 Wed'
       WHEN 4 THEN '4 Thu' WHEN 5 THEN '5 Fri' WHEN 6 THEN '6 Sat' ELSE '7 Sun' END AS day_name,
  f.local_hour,
  COUNT(*)                    AS n_arrivals,
  AVG(IF(f.is_on_time, 1, 0)) AS on_time_rate
FROM `mybas-tracker-504819.basmy_real.fact_arrivals` AS f
WHERE f.service_date BETWEEN PARSE_DATE('%Y%m%d', @DS_START_DATE) AND PARSE_DATE('%Y%m%d', @DS_END_DATE)
GROUP BY f.day_of_week, day_name, f.local_hour
HAVING n_arrivals >= 20
ORDER BY f.day_of_week, f.local_hour
