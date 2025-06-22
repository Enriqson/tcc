
--------------------------------------------- SCHEMAS ---------------------------------------------

CREATE SCHEMA demo.iceberg_schema WITH (location = 's3://warehouse/iceberg_schema');

CREATE SCHEMA hive.external_data WITH (location = 's3a://warehouse/parsed_data/');

CREATE SCHEMA hive.hive_schema WITH (location = 's3a://warehouse/hive_schema/');

CREATE TABLE demo.iceberg_schema.employees_test
(
  name varchar,
  salary decimal(10,2)
)
WITH (
  format = 'PARQUET'
);

INSERT INTO demo.iceberg_schema.employees_test (name, salary) VALUES ('Sam Evans', 55000);

CREATE TABLE hive.hive_schema.employees_test
(
  name varchar,
  salary decimal(10,2)
)
WITH (
  format = 'PARQUET', external_location = 's3a://warehouse/hive_schema/test'
);

INSERT INTO hive.hive_schema.employees_test (name, salary) VALUES ('Sam Evans', 55000);

--------------------------------------------- SAMPLE PARQUET DATA ---------------------------------------------
USE hive.external_data;

CREATE TABLE customer
(
        C_CUSTKEY       BigInt,
        C_NAME          varchar(25),
        C_ADDRESS       varchar(25),
        C_CITY          varchar(10), 
        C_NATION        varchar(25), 
        C_REGION        varchar(12), 
        C_PHONE         varchar(15),
        C_MKTSEGMENT    varchar(10) 
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/parsed_data/s_1/customer' 
);


CREATE TABLE lineorder
(
    LO_ORDERKEY             BigInt, 
    LO_LINENUMBER           Int, 
    LO_CUSTKEY              BigInt,
    LO_PARTKEY              BigInt, 
    LO_SUPPKEY              BigInt,
    LO_ORDERDATE            Date,
    LO_ORDERPRIORITY        varchar(15), 
    LO_SHIPPRIORITY         Int, 
    LO_QUANTITY             Int, 
    LO_EXTENDEDPRICE        Int, 
    LO_ORDTOTALPRICE        Int, 
    LO_DISCOUNT             Int, 
    LO_REVENUE              Int,
    LO_SUPPLYCOST           BigInt,
    LO_TAX                  Int,
    LO_COMMITDATE           Date,
    LO_SHIPMODE             varchar(10)
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/parsed_data/s_1/lineorder' 
);


CREATE TABLE part
(
        P_PARTKEY       BigInt,
        P_NAME          varchar(22),
        P_MFGR          varchar(6), 
        P_CATEGORY      varchar(7), 
        P_BRAND         varchar(9), 
        P_COLOR         varchar(11), 
        P_TYPE          varchar(25),
        P_SIZE          Int,
        P_CONTAINER     varchar(10) 
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/parsed_data/s_1/part' 
);


CREATE TABLE supplier
(
        S_SUPPKEY       BigInt,
        S_NAME          varchar(25),
        S_ADDRESS       varchar(25),
        S_CITY          varchar(10),
        S_NATION        varchar(15),
        S_REGION        varchar(12),
        S_PHONE         varchar(15)
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/parsed_data/s_1/supplier' 
);


CREATE TABLE date
(
        D_DATEKEY            Date,
        D_DATE               char(18),
        D_DAYOFWEEK          char(8),
        D_MONTH              char(9),
        D_YEAR               smallint,
        D_YEARMONTHNUM       bigint,
        D_YEARMONTH          char(7),
        D_DAYNUMINWEEK       Int,
        D_DAYNUMINMONTH      Int,
        D_DAYNUMINYEAR       Int,
        D_MONTHNUMINYEAR     smallint,
        D_WEEKNUMINYEAR      Int,
        D_SELLINGSEASON      varchar(12),
        D_LASTDAYINWEEKFL    boolean,
        D_LASTDAYINMONTHFL   boolean,
        D_HOLIDAYFL          boolean,
        D_WEEKDAYFL          boolean
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/parsed_data/s_1/date' 
);


--------------------------------------------- HIVE TABLES ---------------------------------------------
USE hive.hive_schema;


CREATE TABLE customer
(
        C_CUSTKEY       BigInt,
        C_NAME          varchar(25),
        C_ADDRESS       varchar(25),
        C_CITY          varchar(10), 
        C_NATION        varchar(25), 
        C_REGION        varchar(12), 
        C_PHONE         varchar(15),
        C_MKTSEGMENT    varchar(10) 
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/hive_schema/customer' 
);


CREATE TABLE lineorder
(
    LO_ORDERKEY             BigInt, 
    LO_LINENUMBER           Int, 
    LO_CUSTKEY              BigInt,
    LO_PARTKEY              BigInt, 
    LO_SUPPKEY              BigInt,
    LO_ORDERDATE            Date,
    LO_ORDERDATE_YEAR       Int,
    LO_ORDERPRIORITY        varchar(15), 
    LO_SHIPPRIORITY         Int, 
    LO_QUANTITY             Int, 
    LO_EXTENDEDPRICE        Int, 
    LO_ORDTOTALPRICE        Int, 
    LO_DISCOUNT             Int, 
    LO_REVENUE              int,
    LO_SUPPLYCOST           BigInt,
    LO_TAX                  Int,
    LO_COMMITDATE           Date,
    LO_SHIPMODE             varchar(10)
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/hive_schema/lineorder',
  partitioned_by = [LO_ORDERDATE_YEAR]
);


CREATE TABLE part
(
        P_PARTKEY       BigInt,
        P_NAME          varchar(22),
        P_MFGR          varchar(6), 
        P_CATEGORY      varchar(7), 
        P_BRAND         varchar(9), 
        P_COLOR         varchar(11), 
        P_TYPE          varchar(25),
        P_SIZE          Int,
        P_CONTAINER     varchar(10) 
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/hive_schema/part' 
);


CREATE TABLE supplier
(
        S_SUPPKEY       BigInt,
        S_NAME          varchar(25),
        S_ADDRESS       varchar(25),
        S_CITY          varchar(10),
        S_NATION        varchar(15),
        S_REGION        varchar(12),
        S_PHONE         varchar(15)
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/hive_schema/part' 
);


CREATE TABLE date
(
        D_DATEKEY            Date,
        D_DATE               char(18),
        D_DAYOFWEEK          char(8),
        D_MONTH              char(9),
        D_YEAR               smallint,
        D_YEARMONTHNUM       bigint,
        D_YEARMONTH          char(7),
        D_DAYNUMINWEEK       Int,
        D_DAYNUMINMONTH      Int,
        D_DAYNUMINYEAR       Int,
        D_MONTHNUMINYEAR     smallint,
        D_WEEKNUMINYEAR      Int,
        D_SELLINGSEASON      varchar(12),
        D_LASTDAYINWEEKFL    boolean,
        D_LASTDAYINMONTHFL   boolean,
        D_HOLIDAYFL          boolean,
        D_WEEKDAYFL          boolean
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://warehouse/hive_schema/date' 
);

--------------------------------------------- ICEBERG TABLES ---------------------------------------------
USE demo.iceberg_schema;

CREATE TABLE customer
(
        C_CUSTKEY       BigInt,
        C_NAME          varchar(25),
        C_ADDRESS       varchar(25),
        C_CITY          varchar(10), 
        C_NATION        varchar(25), 
        C_REGION        varchar(12), 
        C_PHONE         varchar(15),
        C_MKTSEGMENT    varchar(10) 
);


CREATE TABLE lineorder
(
    LO_ORDERKEY             BigInt, 
    LO_LINENUMBER           Int, 
    LO_CUSTKEY              BigInt,
    LO_PARTKEY              BigInt, 
    LO_SUPPKEY              BigInt,
    LO_ORDERDATE            Date,
    LO_ORDERDATE_YEAR       Int,
    LO_ORDERPRIORITY        varchar(15), 
    LO_SHIPPRIORITY         Int, 
    LO_QUANTITY             Int, 
    LO_EXTENDEDPRICE        Int, 
    LO_ORDTOTALPRICE        Int, 
    LO_DISCOUNT             Int, 
    LO_REVENUE              int,
    LO_SUPPLYCOST           BigInt,
    LO_TAX                  Int,
    LO_COMMITDATE           Date,
    LO_SHIPMODE             varchar(10)
)
WITH (
  partitioned_by = [LO_ORDERDATE_YEAR]
);

CREATE TABLE part
(
        P_PARTKEY       BigInt,
        P_NAME          varchar(22),
        P_MFGR          varchar(6), 
        P_CATEGORY      varchar(7), 
        P_BRAND         varchar(9), 
        P_COLOR         varchar(11), 
        P_TYPE          varchar(25),
        P_SIZE          Int,
        P_CONTAINER     varchar(10) 
);


CREATE TABLE supplier
(
        S_SUPPKEY       BigInt,
        S_NAME          varchar(25),
        S_ADDRESS       varchar(25),
        S_CITY          varchar(10),
        S_NATION        varchar(15),
        S_REGION        varchar(12),
        S_PHONE         varchar(15)
);


CREATE TABLE date
(
        D_DATEKEY            Date,
        D_DATE               char(18),
        D_DAYOFWEEK          char(8),
        D_MONTH              char(9),
        D_YEAR               smallint,
        D_YEARMONTHNUM       bigint,
        D_YEARMONTH          char(7),
        D_DAYNUMINWEEK       Int,
        D_DAYNUMINMONTH      Int,
        D_DAYNUMINYEAR       Int,
        D_MONTHNUMINYEAR     smallint,
        D_WEEKNUMINYEAR      Int,
        D_SELLINGSEASON      varchar(12),
        D_LASTDAYINWEEKFL    boolean,
        D_LASTDAYINMONTHFL   boolean,
        D_HOLIDAYFL          boolean,
        D_WEEKDAYFL          boolean
);
