/*****************************************************

    SNOWPIPE CLASSIC

*****************************************************/


-- set the content
use database TEST_DB;
use schema PUBLIC;



/****************************************
This is an assumption that needs to be checked:

Snoflake-sink-connector plugin for the Kafka-connect works as follows

1. It is designed for the Streaming Snowpipe. It relies on that technology
2. It creates snowpipe objects automatically (if instructed in the config file)
3. When inserting in the table ,the sink-connector (JDBC 3.20.0)
   runs this: THis is a list command!!! shows what is in the stage object
           
           ls  @SNOWFLAKE_KAFKA_CONNECTOR_snowflake_lexisnexis_sink_704588899_STAGE_LEXISNEXIS/

This stage object is of type = INTERNAL; the 
show stages like 'SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS';

Check the PIPE status
show pipes like '%LEXISNEXIS%';
select system$pipe_status('TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0');



****************************************/

--AUTO CREATED OBJECTS:

-- ############ PIPE #############
-- SELECT GET_DDL('PIPE','TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0')
/*
create or replace pipe SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0 
    auto_ingest=false 
as 
    copy into LEXISNEXIS(RECORD_METADATA, RECORD_CONTENT) 
    from (
            select $1:meta, $1:content 
            from @SNOWFLAKE_KAFKA_CONNECTOR_snowflake_lexisnexis_sink_704588899_STAGE_LEXISNEXIS t
    ) 
file_format = (type = 'json');


-- ############ LANDING TABLE #############
-- SELECT GET_DDL('TABLE','TEST_DB.PUBLIC.LEXISNEXIS');

create or replace TABLE LEXISNEXIS (
	RECORD_METADATA VARIANT,
	RECORD_CONTENT VARIANT,
     INGESTION_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

*/

--check table:
select * from TEST_DB.PUBLIC.LEXISNEXIS;

--check stage content
SELECT $1 FROM @SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS

--describe stage object
DESCRIBE STAGE TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_STAGE_LEXISNEXIS;

--get pipe object definition
describe pipe TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0;
select get_ddl('PIPE','TEST_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_LEXISNEXIS_SINK_704588899_PIPE_LEXISNEXIS_0');



















    


