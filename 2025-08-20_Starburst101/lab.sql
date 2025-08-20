

--- VERIFY SCHEMA DISCOVERY WORKED

SELECT * FROM "nyc_uber_rides"."discovered_schema"."year_month" LIMIT 10;

SHOW CREATE TABLE discovered_schema.year_month; 
-- show it in exploder




--- VALIDATE BRONZE LAYER


SELECT DISTINCT(year_month) FROM discovered_schema.year_month ORDER BY year_month;




--- BUILD SCHEMA FOR SILVER/GOLD LAYERS

create schema nyc_uber_rides.demo;
use nyc_uber_rides.demo;
-- show it in the exploder




--- BUILD SILVER LAYER

-- iceberg, col datatypes, don't need extra year_month field thx to hidden partitioning
CREATE TABLE ride_pickups (
  dispatching_base varchar,
  pickup_date timestamp (6),
  affiliated_base_num varchar,
  location_id integer
 ) WITH (
  type = 'iceberg',
  format = 'parquet',
  partitioning = array['month(pickup_date)']
 );
-- GO AHEAD AND DO INSERT SINCE USING FREE SERVER!!

-- leaving off year_month from select and rest is just casting, 
--  but could do additional validation/cleaning/enrichmment as needed
--  plus automate with your fav tool like dagster, prefect, airflow, dbt, etc
INSERT INTO
 ride_pickups
SELECT
 dispatching_base,
 cast(pickup_date as timestamp (6)),
 affiliated_base_num,
 cast(location_id as INT)
FROM
 discovered_schema.year_month;

-- take a peek at data and structure
SELECT * FROM ride_pickups LIMIT 10;
DESCRIBE ride_pickups;

-- check out metadata tables
select * from "ride_pickups$snapshots";
select * from "ride_pickups$partitions";
-- partion col vals look weird, but check out metadata

-- what about the snowflake lookup data??  bronze? silver??
select * from taxi_zone_lookup.taxi_zones.zone_lookup;




--- FEDERATE DATA!!

SELECT
  p.dispatching_base,
  p.pickup_date,
  p.location_id,
  l.borough,
  l.zone
FROM
  ride_pickups p
INNER JOIN taxi_zone_lookup.taxi_zones.zone_lookup l ON p.location_id = l.location_id;



--- CREATE GOLD LAYER

CREATE TABLE
 rides_by_zone
WITH
 (type = 'iceberg', format = 'parquet') AS
SELECT
 p.dispatching_base,
 p.pickup_date,
 p.location_id,
 date_format(p.pickup_date, '%W') weekday,
 date_format(p.pickup_date, '%M') month,
 l.borough,
 l.zone
FROM
 ride_pickups p
 INNER JOIN taxi_zone_lookup.taxi_zones.zone_lookup l ON p.location_id = l.location_id;

-- peek into the new gold zone table
SELECT * FROM rides_by_zone LIMIT 10;



--- INTERACTIVE ANALYTICS

-- what is the most popular weekday by borough?
WITH
 weekly AS (
   SELECT
     borough,
     weekday,
     COUNT(*) AS total_rides,
     RANK() OVER (PARTITION BY borough ORDER BY COUNT(weekday) DESC
     ) AS rank_column
   FROM
     rides_by_zone
   GROUP BY
     borough,
     weekday
   ORDER BY
     borough,
     COUNT(*) DESC
 )
SELECT
 borough,
 weekday,
 total_rides
FROM
 weekly
WHERE
 rank_column = 1;



--- CREATE VIEWS FOR MARKETING (YEP, GOLD ZONE)

CREATE OR REPLACE VIEW borough_most_pop_weekday_vw AS
WITH
 weekly AS (
   SELECT
     borough,
     weekday,
     COUNT(*) AS total_rides,
     RANK() OVER (PARTITION BY borough ORDER BY COUNT(weekday) DESC
     ) AS rank_column
   FROM
     rides_by_zone
   GROUP BY
     borough,
     weekday
   ORDER BY
     borough,
     COUNT(*) DESC
 )
SELECT
 borough,
 weekday,
 total_rides
FROM
 weekly
WHERE
 rank_column = 1;

SELECT * FROM borough_most_pop_weekday_vw;

CREATE OR REPLACE VIEW borough_most_pop_month_vw AS
WITH
 monthly AS (
   SELECT
     borough,
     month,
     COUNT(*) AS total_rides,
     rank() OVER (PARTITION BY borough ORDER BY COUNT(month) DESC
     ) AS rank_column
   FROM
     rides_by_zone
   GROUP BY
     borough,
     month
   ORDER BY
     borough,
     COUNT(*) DESC
 )
SELECT
 borough,
 month,
 total_rides
FROM
 monthly
WHERE
 rank_column = 1;

SELECT * FROM borough_most_pop_month_vw;
-- ^^^ month version



--- SETUP RBAC FOR MARKETING ROLE TO 2 NEW VIEWS (ONLY)
-- notice how looks in UI
select * from borough_most_pop_weekday_vw;
select * from borough_most_pop_month_vw;
select * from discovered_schema.year_month;



--- DATA PRODUCTS (IN UI; don't forget to swap roles)



