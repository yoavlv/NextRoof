--------------------triggers--------------------
--------------Clean irrelevant types--------------
DELETE FROM nadlan_raw
WHERE type IN (
    SELECT type
    FROM nadlan_raw
    GROUP BY type
    HAVING COUNT(type) < 1000
);