create schema mycloud.nifi_ingest;
use mycloud.nifi_ingest;

drop table pokemon_spawns;

CREATE TABLE pokemon_spawns (
    s2_id bigint, s2_token varchar,
    num int, name varchar,
    lat double, lng double,
    encounter_ms bigint, disppear_ms bigint
)
WITH (type = 'iceberg', format = 'avro');

INSERT INTO pokemon_spawns VALUES 
  (-9185794522947256000,'8085808cc6d',13,'Weedle',37.7935915752623,-122.408720633183,1469520187732,1469519919988),
  (-9185794529389707000,'8085808b51d',16,'Pidgey',37.7947455405929,-122.406419649564,1469520297172,1469519919992),
  (-9185794529389707000,'8085808b271',41,'Zubat',37.794999066064,-122.404384122075,1469520709924,1469519919991);

INSERT INTO pokemon_spawns VALUES 
  (-9185794082713108000,'808580f3587',16,'Pidgey',37.7956444102582,-122.407127649888,-1,1469519920134),
  (-9185794076270658000,'808580f4b1d',60,'Poliwag',37.7955915257874,-122.406331149188,1469520741876,1469519920153),
  (-9182922218470900000,'808fb4e54b3',50,'Diglett',37.3011286952679,-122.048453380601,1469520163692,1469520120130);


SELECT * FROM pokemon_spawns;

SELECT count(*) FROM pokemon_spawns;

select * from "pokemon_spawns$snapshots"
