/*
SETUP SNOWFLAKE STREAMING CONNECTOR
*/


--create test db/schema
CREATE DATABASE IF NOT EXISTS KAFKA_TESTING;
CREATE OR REPLACE SCHEMA KAFKA_TESTING.PUBLIC;

--set db context;
USE SCHEMA KAFKA_TESTING.PUBLIC;

/***********************************************
Snowlake sink connector requirement
Add public key in the RSA format to the SF user
The key is requred for Kafka Sink Connector to authenticate with Snowflake.
The connector uses the private key to sign the messages before sending them to Snowflake.
Snowflake uses the public key to verify the signature of the messages.
***********************************************/
select current_user();
ALTER USER DEANM 
SET RSA_PUBLIC_KEY=' key here ... no cr no new lines';
-- check the user
DESC USER deanm;

--check channel object
show channels
  in table TEST_KAFKA_STREAMING;

 
SHOW CHANNELS IN TABLE TEST_KAFKA_STREAMING;
SELECT SYSTEM$SNOWPIPE_STREAMING_UPDATE_CHANNEL_OFFSET_TOKEN('KAFKA_TESTING.PUBLIC.TEST_KAFKA_STREAMING', 'SF-TEST-TOPIC_0', '-1');
SELECT SYSTEM$SNOWPIPE_STREAMING_UPDATE_CHANNEL_OFFSET_TOKEN('KAFKA_TESTING.PUBLIC.TEST_KAFKA_STREAMING', 'SF-TEST-TOPIC_1', '-1');
/*Check the target table*/
SELECT *
FROM TEST_KAFKA_STREAMING;


-- DROP TABLE IF EXISTS TEST_KAFKA_STREAMING;

-- flatten kafka metadata
SELECT 
  RECORD_METADATA:topic::string as topic,
  RECORD_METADATA:partition::integer as partition,
  JSON_EXTRACT_PATH_TEXT(RECORD_METADATA, 'offset') as offset,
  TO_TIMESTAMP(JSON_EXTRACT_PATH_TEXT(RECORD_METADATA, 'CreateTime')) as CreateTime,
  RECORD_METADATA:key::string as record_key,
  RECORD_METADATA,
FROM KAFKA_TESTING.PUBLIC.TEST_KAFKA_STREAMING;

--check the table
DESCRIBE TABLE TEST_KAFKA_STREAMING;

--check schema evolution property value
SHOW TABLES LIKE 'TEST_KAFKA_STREAMING';
SELECT "name",
       "enable_schema_evolution" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
