-- Setup notes & activities for
--  May 8, 2024, webinar focused on
--  migrating from Hive to Iceberg


-- Modify mycatalog.myschema as appropriate
DROP SCHEMA mycatalog.myschema CASCADE;
CREATE SCHEMA mycatalog.myschema;
USE mycatalog.myschema;

------------------------------------
-- DEMO: show benefits of versioning
------------------------------------
--  NO SETUP REQ'D


------------------------------------
-- DEMO: Incompatible file format
------------------------------------
DROP TABLE IF EXISTS cust_hive_json;
CREATE TABLE cust_hive_json 
WITH(type='hive',format='json')
AS SELECT * FROM tpch.sf10.customer;


------------------------------------
-- DEMO: Incompatible data type
------------------------------------
DROP TABLE IF EXISTS web_page_hive_parquet;
CREATE TABLE web_page_hive_parquet 
WITH(type='hive',format='parquet')
AS SELECT * FROM tpcds.sf10.web_page;


------------------------------------
-- DEMO: bucketing is ignored
------------------------------------
DROP TABLE IF EXISTS orders_hive_orc_bucketed;
CREATE TABLE orders_hive_orc_bucketed
WITH (type='hive', format='orc', 
      partitioned_by=ARRAY['orderpriority'],
      bucketed_by=ARRAY['custkey'], bucket_count=5)
AS SELECT orderkey, custkey, orderstatus, totalprice,
          orderdate, clerk, shippriority, comment,
          orderpriority
     FROM tpch.sf10.orders;


------------------------------------
-- DEMO: in-place CAN BE EFFECTIVE!!
------------------------------------
DROP TABLE IF EXISTS cust_hive_orc_part_bfltr;
CREATE TABLE cust_hive_orc_part_bfltr
WITH (type='hive', format='orc', 
      -- have NOT tested >>  transactional = true,
      partitioned_by=ARRAY['mktsegment', 'nationkey'],
      orc_bloom_filter_columns=ARRAY['phone'],
      orc_bloom_filter_fpp=0.10)
AS SELECT custkey, name, address, phone, acctbal, comment,
          mktsegment, nationkey
     FROM tpch.sf10.customer;


------------------------------------
-- DEMO: migrate a partition at a time
------------------------------------
DROP TABLE IF EXISTS cust_hive_avro_part;
CREATE TABLE cust_hive_avro_part
WITH (type='hive', format='avro', 
      partitioned_by=ARRAY['mktsegment'])
AS SELECT custkey, name, address, nationkey, phone, acctbal, comment,
          mktsegment
     FROM tpch.sf10.customer;
