/*
rm -rf metastore_db/
schematool -dbType derby -initSchema
*/


CREATE EXTERNAL TABLE  yagodataset_base(
subject STRING,
predicate STRING,
object STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/Users/keshav/Projects/ass_3/tables';
LOAD DATA LOCAL INPATH '/Users/keshav/Projects/nosql_proj/data.txt' INTO TABLE yagodataset_base;



CREATE TABLE  yagodataset(subject STRING, predicate STRING, object STRING) clustered by (object) into 5 buckets STORED AS ORC TBLPROPERTIES ("transactional"="true");
INSERT INTO TABLE yagodataset SELECT subject, predicate, object FROM yagodataset_base;
ALTER TABLE yagodataset ADD COLUMNS(timestp bigint);
update yagodataset set timestp = unix_timestamp(CURRENT_TIMESTAMP());


CREATE EXTERNAL TABLE  log_table(
subject STRING,
predicate STRING,
object STRING,
timestp BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LOCATION '/Users/keshav/Projects/ass_3/log_tables';


CREATE TABLE  merge_table(server STRING, timestp BIGINT) clustered by (timestp) into 2 buckets STORED AS ORC TBLPROPERTIES ("transactional"="true");
INSERT INTO TABLE merge_table VALUES("<psql>", 0);