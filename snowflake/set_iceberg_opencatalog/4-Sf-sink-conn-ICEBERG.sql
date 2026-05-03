/*****************************************************

    STREAMING SNOWPIPE ICEBERG

Prerequisites TEST_DB, 
CREATE OR REPLACE DATABASE TEST_DB;
USE SCHEMA TEST_DB.PUBLIC;

*****************************************************/


-- set the content
use database TEST_DB;
use schema PUBLIC;

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH; --X-SMALL

/**************************************************
PREREQUISITES: THE ICEBERG TABLE MUST EXISTS BEFORE
STREAMING STARTS !!
***************************************************/


/*******************************************************************
  EXPERIMENT 1 -- CREATE AN ICEBERG TABLE (SKELETON)
  Kafka Connect cannot create iceberg table from scratch because it 
  cannot know the iceberg file location, integration volume etc.
- We must create at least skeleton + set schema evaluation to true

*******************************************************************/

--(8)--Create an Apache Iceberg table in Snowflake
/*****************************************************/

CREATE OR REPLACE ICEBERG TABLE TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG (
	record_metadata OBJECT()
)
CATALOG = 'SNOWFLAKE'  -- default for Snowflake managed iceberg catalogs
EXTERNAL_VOLUME = 'iceberg_external_volume'
BASE_LOCATION = 'LEXISNEXIS';  -- The system will create an unique name like LEXISNEXIS.mMooh7rm/

/*
ref: https://docs.snowflake.com/en/user-guide/data-load-schema-evolution
Snowflake enables seamless handling of evolving semi-structured data. 
As data sources add new columns, Snowflake automatically updates table structures to reflect these changes, 
including the addition of new columns. This eliminates the need for manual schema adjustments. 
*/
ALTER ICEBERG TABLE TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG 
    SET ENABLE_SCHEMA_EVOLUTION  = true;

/*
1st run, on config upload, system craetes a full record_metadata object
2nd run - 1st payload, system creates separate nodes from the avro. schema

--select get_ddl('TABLE','TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG')
CREATE OR REPLACE ICEBERG TABLE LEXISNEXIS_ICEBERG (
	RECORD_METADATA OBJECT(
        offset INT, 
        topic STRING, 
        partition INT, 
        key STRING, 
        schema_id INT, 
        key_schema_id INT, 
        CreateTime LONG, 
        LogAppendTime LONG, 
        SnowflakeConnectorPushTime LONG, 
        headers MAP(STRING, STRING)),
	SURNAME STRING 
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector',
	NAME STRING 
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector'
)
 EXTERNAL_VOLUME = 'ICEBERG_EXTERNAL_VOLUME'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'LEXISNEXIS/';
*/
select * from TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG;


select get_ddl('TABLE','TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG')
/*
CREATE OR REPLACE ICEBERG TABLE LEXISNEXIS_ICEBERG (
	RECORD_METADATA OBJECT(
        offset INT, 
        topic STRING, 
        partition INT, 
        key STRING, 
        schema_id INT, 
        key_schema_id INT, 
        CreateTime LONG, 
        LogAppendTime LONG, 
        SnowflakeConnectorPushTime LONG, 
        headers MAP(STRING, STRING)),
	SURNAME STRING 
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector',
	NAME STRING 
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector'
)
 EXTERNAL_VOLUME = 'ICEBERG_EXTERNAL_VOLUME'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'LEXISNEXIS/';
*/

/*******************************************************************
  EXPERIMENT 2 
  -- change avro schema, add a new node
  -- upload schema
  -- send a new message... observe schema evaluation

*******************************************************************/
select * from TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG
ORDER BY RECORD_METADATA.SnowflakeConnectorPushTime desc

SELECT *
FROM TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG
ORDER BY RECORD_METADATA:"SnowflakeConnectorPushTime" desc

select * from TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG

select get_ddl('TABLE','TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG')
/*
create or replace ICEBERG TABLE LEXISNEXIS_ICEBERG (
	RECORD_METADATA OBJECT(
            offset INT, 
            topic STRING, 
            partition INT, 
            key STRING, 
            schema_id INT, 
            key_schema_id INT, 
            CreateTime LONG, 
            LogAppendTime LONG, 
            SnowflakeConnectorPushTime LONG, 
            headers MAP(STRING, STRING)),
	SURNAME STRING 
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector',
	NAME STRING 
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector',
	AGE1 LONG   <-- NEW COLUMN !!!!!!!!!!!!!!!!!
        COMMENT 'column created by schema evolution from Snowflake Kafka Connector'
)
 EXTERNAL_VOLUME = 'ICEBERG_EXTERNAL_VOLUME'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'LEXISNEXIS/';
*/

