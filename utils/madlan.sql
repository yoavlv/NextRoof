--------------Handle Missing Values-----------------
----------------------------------- Create floors_and_build_year_view VIEW
CREATE OR REPLACE VIEW floors_and_build_year_view AS
SELECT city_id, street_id,
    CAST(FLOOR(CAST(home_number AS NUMERIC)) AS INTEGER) AS home_number,
    MAX(floors) AS floors,
    MAX(build_year) AS build_year
FROM nadlan_clean
GROUP BY city_id, street_id,home_number;

----------------------------------- Create floors_and_build_year_avg_view VIEW
CREATE OR REPLACE VIEW floors_and_build_year_avg_view AS
SELECT city_id, street_id,
    FLOOR(AVG(floors)) AS avg_floors,
    FLOOR(AVG(build_year)) AS avg_build_year
FROM nadlan_clean
GROUP BY city_id, street_id;

----------------------------------- update floors
UPDATE madlan_clean
SET floors = v.floors
FROM floors_and_build_year_view AS v
WHERE madlan_clean.city_id = v.city_id
AND madlan_clean.street_id = v.street_id
AND madlan_clean.home_number = v.home_number
AND madlan_clean.floors IS NULL;

----------------------------------- update floors AVG
UPDATE madlan_clean
SET floors = v.avg_floors
FROM floors_and_build_year_avg_view AS v
WHERE madlan_clean.city_id = v.city_id
AND madlan_clean.street_id = v.street_id
AND madlan_clean.floors IS NULL;


----------------------------------- update build_year
UPDATE madlan_clean
SET build_year = v.build_year
FROM floors_and_build_year_view AS v
WHERE madlan_clean.city_id = v.city_id
AND madlan_clean.street_id = v.street_id
AND madlan_clean.home_number = v.home_number
AND madlan_clean.build_year IS NULL;

----------------------------------- update build_year AVG
UPDATE madlan_clean
SET build_year = v.avg_build_year
FROM floors_and_build_year_avg_view AS v
WHERE madlan_clean.city_id = v.city_id
AND madlan_clean.street_id = v.street_id
AND madlan_clean.build_year IS NULL;

--------------RANK-----------------
----------------------------------- Create street_rank
UPDATE madlan_clean
SET street_rank = v.street_rank
FROM street_rank AS v
WHERE madlan_clean.street_id = v.street_id
AND madlan_clean.city_id = v.city_id;

----------------------------------- Create helka_rank
UPDATE madlan_clean
SET helka_rank = v.helka_rank
FROM helka_rank AS v
WHERE madlan_clean.gush = v.gush
AND madlan_clean.helka = v.helka;

----------------------------------- Create gush_rank
UPDATE madlan_clean
SET gush_rank = v.gush_rank
FROM gush_rank AS v
WHERE madlan_clean.gush = v.gush;

------ Drop null ------