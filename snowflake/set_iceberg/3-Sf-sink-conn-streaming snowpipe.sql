/*****************************************************

    STREAMING SNOWPIPE CLASSIC

*****************************************************/


-- set the content
use database TEST_DB;
use schema PUBLIC;


/*******************************************************************
  EXPERIMENT 1 -- ALL SET UP AUTOMATICALLY BY THE KAFKA CONNECTOR;
 
     (ONLY THE LANDING TABLE CAN BE CUSTOMIZED -- experiment 2)
     The channel objects are automatically created!!!
*******************************************************************/

-- a) landing table automatically created TEST_DB.PUBLIC.LEXISNEXIS_STREAMING

select * from TEST_DB.PUBLIC.LEXISNEXIS_STREAMING;

--- if snowflake.enable.schematization = true, and the table does not exist,
---- the table will have automatically enabled ENABLE_SCHEMA_EVOLUTION i

-- default vaules are not allowed

--drop table TEST_DB.PUBLIC.LEXISNEXIS_STREAMING;

SHOW CHANNELS LIKE 'SF_%'

ALTER TABLE TEST_DB.PUBLIC.LEXISNEXIS_STREAMING
    ADD  INGESTION_TIME TIMESTAMP_LTZ
 
ALTER TABLE TEST_DB.PUBLIC.LEXISNEXIS_STREAMING
  SET
  ALTER COLUMN  INGESTION_TIME
        DEFAULT CURRENT_TIMESTAMP;


/*******************************************************************
  EXPERIMENT 2 -- ALL SET UP AUTOMATICALLY BY THE KAFKA CONNECTOR;
 
     (create your own table)
     -- if snowflake.enable.schematization = true
     
 ALTER  TABLE TEST_DB.PUBLIC.LEXISNEXIS_STREAMING
    SET ENABLE_SCHEMA_EVOLUTION  = true;

*******************************************************************/
--drop table 
create or replace TABLE TEST_DB.PUBLIC.LEXISNEXIS_STREAMING(
	RECORD_METADATA VARIANT,
	RECORD_CONTENT VARIANT,
    INGESTION_TIME TIMESTAMP_LTZ,
    MY_RANDOM_COLUMN STRING  --DEFAULT CURRENT_TIMESTAMP  --custom column ( we can add heaps of columns but will not be populated by kafka connector)
);

--select SYSTEM$SNOWPIPE_STREAMING_MIGRATE_CHANNEL_OFFSET_TOKEN('TEST_DB', 'PUBLIC','LEXISNEXIS_STREAMING' );








-- b) landing table automatically created TEST_DB.PUBLIC.LEXISNEXIS_STREAMING

-- Try to create my own table











--check table:
select * from TEST_DB.PUBLIC.LEXISNEXIS;

DELETE FROM TEST_DB.PUBLIC.LEXISNEXIS

select get_ddl('TABLE','TEST_DB.PUBLIC.LEXISNEXIS')


DESCRIBE STAGE TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS;


show channels







--(8)--Create an Apache Iceberg table in Snowflake
/*****************************************************/
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH; --X-SMALL

CREATE OR REPLACE DATABASE TEST_DB;
USE SCHEMA TEST_DB.PUBLIC;

CREATE OR REPLACE ICEBERG TABLE TEST_DB.PUBLIC.LEXISNEXIS (
  Name STRING,
  Surname STRING
)
CATALOG = 'SNOWFLAKE'  -- default for Snowflake managed iceberg catalogs
EXTERNAL_VOLUME = 'iceberg_external_volume'
BASE_LOCATION = 'LEXISNEXIS';  -- The system will create an unique name like LEXISNEXIS.mMooh7rm/
/*****************************************************/


ALTER ICEBERG TABLE TEST_DB.PUBLIC.LEXISNEXIS 

For an existing table, modify the table using the ALTER TABLE command and set the ENABLE_SCHEMA_EVOLUTION parameter to TRUE.

DESCRIBE TABLE LEXISNEXIS;
select get_ddl


--CHECK INSERT
insert into TEST_DB.PUBLIC.LEXISNEXIS
  select *
  from (values (9944,2,3,4,5,'String1','String3')  ) as val
  
select * from TEST_DB.PUBLIC.LEXISNEXIS;
SELECT * FROM @SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS
    

DROP TABLE TEST_DB.PUBLIC.LEXISNEXIS;
DROP STAGE TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS
DROP PIPE TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0

/**************************************

DESCRIBE STAGE TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS


describe pipe TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0

select get_ddl('PIPE','TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0');
*/
