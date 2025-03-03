-- Demo notes & activities for
--  March 4, 2025, webinar focused on
--  migrating from Hive to Iceberg


-- Modify mycatalog.myschema as appropriate
USE mycatalog.myschema;

------------------------------------
-- DEMO: show benefits of versioning
------------------------------------

-- create an iceberg table
CREATE TABLE dune_characters (
   id integer,
   name varchar,
   description varchar
) WITH (
   TYPE = 'iceberg', FORMAT = 'parquet'
);

-- add/select a few records
INSERT INTO dune_characters
 (id, name, description)
VALUES
 (101, 'Leto', 'Ruler of House Atreides'),
 (102, 'Jessica', 'Consort of the Duke'),
 (103, 'Paul', 'Son of Leto (aka Dale Cooper)');
SELECT * FROM dune_characters;

-- show 2 versions (aka snapshots)
SELECT made_current_at,
       snapshot_id, parent_id
  FROM "dune_characters$history"
 ORDER BY made_current_at;

-- add more records & show them all
INSERT INTO dune_characters
 (id, name, description)
VALUES
 (104, 'Thufir', 'Mentat'),
 (201, 'Vladimir', 'Ruler of House Harkonnen'),
 (202, 'Rabban', 'Ruthless nephew of Vladimir'),
 (203, 'Feyd-Rautha', 'Savvy nephew of Vladimir (played by Sting)'),
 (301, 'Reverend Mother Gaius Helen Mohiam', null);
SELECT * FROM dune_characters;

-- verify there is a 3rd snapshot now
SELECT made_current_at,
       snapshot_id, parent_id
  FROM "dune_characters$history"
 ORDER BY made_current_at;

 -- query the 2nd snapshot_id (replace 99999999 as appropriate)
SELECT * FROM dune_characters
FOR VERSION AS OF 99999999;

-- rollback to that version & verify only 3 rows are present
--  (modify mycatalog, myschema, and 99999999 as appropriate)
CALL mycatalog.system.rollback_to_snapshot(
   'myschema', 'dune_characters',
    99999999);
SELECT * FROM dune_characters;


------------------------------------
-- DEMO: Incompatible file format
------------------------------------

-- how many records and how is the table setup
SELECT format_number(COUNT()) FROM cust_hive_json;
SHOW CREATE TABLE cust_hive_json;

-- attempt in-place migration
ALTER TABLE cust_hive_json
SET PROPERTIES type='iceberg';

-- perform a shadow migration (abbr order for demo)
--  create new table
CREATE TABLE ice_from_cust_hive_json
WITH(type='iceberg')
AS SELECT * FROM cust_hive_json;
--  drop the old one
DROP TABLE cust_hive_json;
--  rename new to original name
ALTER TABLE ice_from_cust_hive_json RENAME TO cust_hive_json;
--  verify all is fine
SELECT format_number(COUNT()) FROM cust_hive_json;
SHOW CREATE TABLE cust_hive_json;


------------------------------------
-- DEMO: Incompatible data type
------------------------------------

-- how is the table setup
SHOW CREATE TABLE web_page_hive_parquet;

-- attempt in-place migration
ALTER TABLE web_page_hive_parquet
SET PROPERTIES type='iceberg';

-- attempt shadow migration w/CTAS
CREATE TABLE ice_from_web_page_hive_parquet
WITH(type='iceberg')
AS SELECT * FROM web_page_hive_parquet;

-- IT WORKED because the connector took care
--  of the appropriate casting of datatypes
DESC ice_from_web_page_hive_parquet;

--  drop the old one
DROP TABLE web_page_hive_parquet;
--  rename new to original name
ALTER TABLE ice_from_web_page_hive_parquet RENAME TO web_page_hive_parquet;
-- verify the datatypes are different
DESC web_page_hive_parquet;


------------------------------------
-- DEMO: bucketing is ignored
------------------------------------

-- gets some details about the table
SELECT * FROM orders_hive_orc_bucketed LIMIT 20;
DESC orders_hive_orc_bucketed;
SELECT DISTINCT(orderpriority) FROM orders_hive_orc_bucketed;
SHOW CREATE TABLE orders_hive_orc_bucketed;

-- verify partition FOLDERS and bucketing FILES are present
SELECT DISTINCT("$path") as distinct_file_name
  FROM orders_hive_orc_bucketed
 ORDER BY distinct_file_name;

-- attempt in-place migration
ALTER TABLE orders_hive_orc_bucketed
SET PROPERTIES type='iceberg';

-- see that bucketing info NOT in table definition
SHOW CREATE TABLE orders_hive_orc_bucketed;

-- LOOKS LIKE it is still there, 
--  because data files were not rewritten
SELECT DISTINCT("$path") as distinct_file_name
  FROM orders_hive_orc_bucketed
 ORDER BY distinct_file_name;

-- add another partition of data to show no bucketing in use
INSERT INTO orders_hive_orc_bucketed
SELECT orderkey, custkey, orderstatus, totalprice,
       orderdate, clerk, shippriority, comment,
       'BOGUS' as orderpriority
  FROM tpch.sf10.orders;

-- verify that new BOGUS partition data files are not bucketed
SELECT DISTINCT("$path") as distinct_file_name
  FROM orders_hive_orc_bucketed
 ORDER BY distinct_file_name;

-- recreate the table to try again with a shadow migration
DROP TABLE orders_hive_orc_bucketed;
CREATE TABLE orders_hive_orc_bucketed
WITH (type='hive', format='orc', 
      partitioned_by=ARRAY['orderpriority'],
      bucketed_by=ARRAY['custkey'], bucket_count=5)
AS SELECT orderkey, custkey, orderstatus, totalprice,
          orderdate, clerk, shippriority, comment,
          orderpriority
     FROM tpch.sf10.orders;

-- stand up new table to load and notice that
--  BUCKETING BECOMES PARTITIONING with similar benefits
CREATE TABLE ice_from_orders_hive_orc_bucketed
WITH (type='iceberg', format='orc',
      partitioning=ARRAY['orderpriority', 'bucket(custkey, 5)'])
AS SELECT * FROM tpch.sf10.orders;
--
SHOW CREATE TABLE ice_from_orders_hive_orc_bucketed;
--
SELECT DISTINCT("$path") as distinct_file_name
  FROM ice_from_orders_hive_orc_bucketed
 ORDER BY distinct_file_name;
--
SELECT * FROM "ice_from_orders_hive_orc_bucketed$partitions" order by partition;

-- "swap" the tables
DROP TABLE orders_hive_orc_bucketed;
ALTER TABLE ice_from_orders_hive_orc_bucketed RENAME TO orders_hive_orc_bucketed;


------------------------------------
-- DEMO: in-place CAN BE EFFECTIVE!!
------------------------------------

-- what does it looks like
SHOW CREATE TABLE cust_hive_orc_part_bfltr;
-- in-place upgrade works
ALTER TABLE cust_hive_orc_part_bfltr
SET PROPERTIES type='iceberg';
-- verify the properties all made it
SHOW CREATE TABLE cust_hive_orc_part_bfltr;


------------------------------------
-- DEMO: migrate a partition at a time
------------------------------------

-- what does it looks like & partition list
SHOW CREATE TABLE cust_hive_avro_part;
SELECT * FROM "cust_hive_avro_part$partitions";
SELECT format_number(COUNT()) FROM cust_hive_avro_part;

-- 1. CREATE A NEW TABLE
CREATE TABLE cust_hive_avro_part_NEW (
   custkey bigint,
   name varchar(25),
   address varchar(40),
   nationkey bigint,
   phone varchar(15),
   acctbal double,
   comment varchar(117),
   mktsegment varchar(10)
)
WITH (type='iceberg', format='avro', 
      partitioning=ARRAY['mktsegment']);

-- 2. RENAME OLD TABLE
ALTER TABLE cust_hive_avro_part 
RENAME TO cust_hive_avro_part_OLD;

-- 3. CREATE A VIEW
CREATE VIEW cust_hive_avro_part AS
SELECT * FROM cust_hive_avro_part_NEW
UNION 
SELECT * FROM cust_hive_avro_part_OLD;
--  verify it works
SELECT format_number(COUNT()) FROM cust_hive_avro_part;

-- remind ourselves of the partition names
SELECT * FROM "cust_hive_avro_part_old$partitions";

-- 4. (1st pass -- AUTOMOBILE)
-- 4A insert partition into new
INSERT INTO cust_hive_avro_part_NEW 
SELECT * FROM cust_hive_avro_part_OLD
 WHERE mktsegment = 'AUTOMOBILE';
-- 4B drop partition from old
DELETE FROM cust_hive_avro_part_OLD
 WHERE mktsegment = 'AUTOMOBILE';
-- 4C should also run metadata cleanup like sync_partitions_metadata
--  and ANALYZE table, but demonstrative example (short/n/sweet)

-- 4. (2nd pass -- BUILDING)
-- 4A insert partition into new
INSERT INTO cust_hive_avro_part_NEW 
SELECT * FROM cust_hive_avro_part_OLD
 WHERE mktsegment = 'BUILDING';
-- 4B drop partition from old
DELETE FROM cust_hive_avro_part_OLD
 WHERE mktsegment = 'BUILDING';
-- 4C should also run metadata cleanup like sync_partitions_metadata
--  and ANALYZE table, but demonstrative example (short/n/sweet)

-- 4. (3rd pass -- the rest! -- HOUSEHOLD, FURNITURE & MACHINERY)
-- 4A insert partition into new
INSERT INTO cust_hive_avro_part_NEW 
SELECT * FROM cust_hive_avro_part_OLD
 WHERE mktsegment IN ('HOUSEHOLD', 'FURNITURE', 'MACHINERY');
-- 4B drop partition from old
DELETE FROM cust_hive_avro_part_OLD
 WHERE mktsegment IN ('HOUSEHOLD', 'FURNITURE', 'MACHINERY');
-- 4C should also run metadata cleanup like sync_partitions_metadata
--  and ANALYZE table, but demonstrative example (short/n/sweet)

-- 5. DROP THE VIEW
DROP VIEW cust_hive_avro_part;
-- 6. DROP THE OLD TABLE
DROP TABLE cust_hive_avro_part_OLD;
-- 7. RENAME NEW TO ORIGINAL NAME
ALTER TABLE cust_hive_avro_part_NEW 
RENAME TO cust_hive_avro_part;

-- verify all still looks good
SHOW CREATE TABLE cust_hive_avro_part;
SELECT * FROM "cust_hive_avro_part$partitions";
SELECT format_number(COUNT()) FROM cust_hive_avro_part;
