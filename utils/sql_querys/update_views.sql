--------------------Views--------------------
--------------City rank view--------------
CREATE OR REPLACE VIEW city_rank_view AS
SELECT * FROM (
SELECT city_id, year, CAST(SUM(price) / SUM(size) AS INT) AS rank , COUNT(*) AS total
FROM nadlan_raw
GROUP BY city_id ,year
)
WHERE total > 10
ORDER BY total;

--------------City rank view--------------
--------------City rank view--------------
--------------City rank view--------------
--------------City rank view--------------
