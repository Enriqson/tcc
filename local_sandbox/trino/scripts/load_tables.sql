--------------------------------------------- HIVE TABLES ---------------------------------------------
INSERT INTO hive.hive_schema.customer 
SELECT * from hive.external_data.customer;

INSERT INTO hive.hive_schema.lineorder 
SELECT *, EXTRACT(YEAR FROM LO_ORDERDATE) AS LO_ORDERDATE_YEAR from hive.external_data.lineorder;

INSERT INTO hive.hive_schema.part 
SELECT * from hive.external_data.part;

INSERT INTO hive.hive_schema.supplier 
SELECT * from hive.external_data.supplier;

INSERT INTO hive.hive_schema.date 
SELECT * from hive.external_data.date;

--------------------------------------------- ICEBERG TABLES ---------------------------------------------
INSERT INTO demo.iceberg_schema.customer 
SELECT * from hive.external_data.customer;

INSERT INTO demo.iceberg_schema.lineorder 
SELECT *, EXTRACT(YEAR FROM LO_ORDERDATE) AS LO_ORDERDATE_YEAR from hive.external_data.lineorder;

INSERT INTO demo.iceberg_schema.part 
SELECT * from hive.external_data.part;

INSERT INTO demo.iceberg_schema.supplier 
SELECT * from hive.external_data.supplier;

INSERT INTO demo.iceberg_schema.date 
SELECT * from hive.external_data.date;