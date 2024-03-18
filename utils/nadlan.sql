----------------------------------
CREATE OR REPLACE VIEW city_rank_view AS
SELECT  city_id, year, CAST(SUM(price) / SUM(size) AS INT) AS rank
FROM nadlan_raw GROUP BY city_id, year
ORDER BY city_id, year;
----------------------------------
CREATE OR REPLACE VIEW street_rank_view AS
SELECT city_id, street_id, year, CAST(SUM(price) / SUM(size) AS INT) AS street_rank
FROM nadlan_clean
GROUP BY city_id, street_id,year
ORDER BY city_id, street_id,year;
----------------------------------
UPDATE nadlan_clean
SET street_rank = v.street_rank
FROM street_rank_view AS v
WHERE nadlan_clean.street_id = v.street_id
AND nadlan_clean.city_id = v.city_id
AND nadlan_clean.year = v.year;
----------------------------------
CREATE OR REPLACE VIEW gush_rank_view AS
SELECT gush, year, CAST(SUM(price) / SUM(size) AS INT) AS gush_rank
FROM nadlan_clean
GROUP BY gush, year
ORDER BY gush, year;
----------------------------------
UPDATE nadlan_clean
SET gush_rank = v.gush_rank
FROM gush_rank_view AS v
WHERE nadlan_clean.gush = v.gush
AND nadlan_clean.year = v.year;
----------------------------------
