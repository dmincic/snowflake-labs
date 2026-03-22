
-- SET SESSION CONTEXT;
USE SCHEMA TEST_DB.PUBLIC;
USE WAREHOUSE COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

ALTER SESSION 
    SET QUERY_TAG='{"local_testing":"snowpipe_streaming"}';

/*
-----------------------------------
Test 1: Landing table does not exist
------------------------------------

(1) Kafka Connect Config: Config
"snowflake.topic2table.map": "sf_iceberg_topic:LEXISNEXIS_STREAMING_NEW",
OMITTED:  "snowflake.enable.schematization":"true"
""

(2) Create kafka connect config - first run (config header included!):
curl -X POST -H "Content-Type: application/json" --data @snowflake-sink-update_streaming_snowpipe.json http://127.0.0.1:8083/connectors


***********
--Results
***********
(BASE LINE)  A new, empty landing table created - Schema:  RECORD_METADATA ,RECORD_CONTENT , SHEMA EVOLUTION 'N'

create or replace TABLE LEXISNEXIS_STREAMING_NEW (
	RECORD_METADATA VARIANT,
	RECORD_CONTENT VARIANT
);

*/
select * from TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW;
--drop table TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW;

--check schema evloution paramter value
SHOW TABLES LIKE 'LEXISNEXIS_STREAMING_NEW';
   SELECT "enable_schema_evolution"     
   FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

DESCRIBE TABLE  TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW;
SELECT GET_DDL('TABLE','TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW');

/******************************************************************************
TEST 1 Would kafka connector alter an existing, EMPTY landing table 
       to support schema evlolution

(1) Update the existing Kafka connect config, add "snowflake.enable.schematization":"true"
(2) run the command below:
curl -X PUT -H "Content-Type: application/json" --data @snowflake-sink-update_streaming_snowpipe.json http://localhost:8083/connectors/snowflake-lexisnexis-sink/config

***********
--Results
***********
- No change
- Kafka Connector (JDBC 3.20.0) does not execute ALTER ddl on the table,
  The table's schema stays the same
****************************************************************************/

/*****************************************************************************
TEST 2 If we delete landing table, would kafka connector create a new table if we initiate 
       Kafka connect by uploading the same config file - we now expect the brand new table with schema evolution ON

(1) drop table TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW;
(2) Update the existing Kafka connect config, add 
curl -X PUT -H "Content-Type: application/json" --data @snowflake-sink-update_streaming_snowpipe.json http://localhost:8083/connectors/snowflake-lexisnexis-sink/config

***********
--Results
***********
- No change
- Kafka Connector (JDBC 3.20.0) does not create a brand new table

********************************************************************************/


/*****************************************************************************
TEST 3 The landing table is dropped. The config update does not create a new table
       What if we send a message

(1) Produce a message
***********
--Results
***********
- The new table has been created

create table if not exists identifier('LEXISNEXIS_STREAMING_NEW') (
    record_metadata variant 
        comment 'created by automatic table creation from Snowflake Kafka Connector'
);
alter table identifier('LEXISNEXIS_STREAMING_NEW') 
    set ENABLE_SCHEMA_EVOLUTION = true;

********************************************************************************/


/*****************************************************************************
TEST 4 Try to convert the exisitng, not empty table to schema evolution = false.
       Would the new config change the table. What if we send a message afterwards?

(1) Update Kafka connect config, exclude schematization
(2) Check the update (http://localhost:8083/connectors/test-streaming-snowpipe/)

***********
--Results
***********
- No changes

(3) Send a message
- message newer reached the table

(4) Check Kafka connect logs (docker level, last 100 messages, ongoing)
docker logs --tail 100 -f kafka-connect
ERROR (after successfully uploading a new config version)

com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException: [SF_KAFKA_CONNECTOR] Exception: Incompatible table
Error Code: 5003
Detail: Table doesn't have a compatible schema
Message: table name: LEXISNEXIS_STREAMING_NEW

NOTE: The message rejections are not visible in the log , why
Kafka Connect does not log every dropped or failed message once a persistent error like a schema incompatibility is detected.

The connector enters a failing state internally, often holding the messages in memory or retrying silently — but not pushing them.

Because the table exists and is incompatible, the connector assumes the config is intentionally fixed and refuses to reinitialize the table or schema.

(5) Just in case , check if the messages are produced in the Kafka topic
docker exec -it kafka /bin/bash
kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic test_spipe_streaming --from-beginning --max-messages 5

ALL MESSAGES THERE
(fix the state of the system)
- upload the original config (shcema evolution on)
- see if the messagess arrive into the table

***********
--Results
***********
- all messages in place, system stable

********************************************************************************/


/*****************************************************************************
TEST 5 Reverse TEST 4 - Would a non-schema evoultion enabled table with records 
                        be automatically converted to schema enalbed , and with the records
                        if we change the config and send a new message

Current landing table is schema enalbed.
- delete all records
    DELETE FROM TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW;
- update config file with no schema evoulution

(1) Update config file (error)
com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException: [SF_KAFKA_CONNECTOR] Exception: Incompatible table
Error Code: 5003
Detail: Table doesn't have a compatible schema
Message: table name: LEXISNEXIS_STREAMING_NEW

(2) drop the landing table
      drop table LEXISNEXIS_STREAMING_NEW

(3) Check table
     select * from  LEXISNEXIS_STREAMING_NEW
-------
result
-------
 - Table (no schema evolution) created correctly, no messages

(4) send a new message
-------
result
-------
  - the new message arrived,
  - the old messages are not in the table, and are still in the kafka topic

Why Kafka Connect didn’t re-ingest older messages:
Kafka Connect tracks offsets per topic partition per connector.

When messages are consumed, their offsets are committed.

Even if the messages weren’t successfully written to Snowflake (e.g. due to schema errors), the connector still may have committed the offsets — especially if errors.tolerance isn’t set to all.

When you deleted and re-created the Snowflake table, Kafka Connect didn’t reset its offsets — it just picked up from the last committed one.

(5) Check the current offsets
kafka-consumer-groups.sh --bootstrap-server localhost:9092  --describe --group connect-test-streaming-snowpipe

- The lag  = 0 , --> Kafka Connect thinks it already processed those messages.

OPTIONS:

(6.1)
      stop kafka connect docker
      docker stop kafka-connect

(6.3) (kafka docker) Reset the offset

   kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group connect-test-streaming-snowpipe \
  --topic test_spipe_streaming \
  --reset-offsets --to-earliest --execute

-Parition 0 , New-offset 0
  
(6.4) Start kafka connect
      docker start  kafka-connect

THE OFFSET IS CHANGED IN THE KAFKA , BUT SNOWFLAKE DON'T CARE ABOUT IT.
docker log kafka-conect shows 
[2025-06-01 05:29:23,804] INFO [SF_KAFKA_CONNECTOR] Fetched offsetToken for channelName:TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW.TEST_SPIPE_STREAMING_0, offset:4 (com.snowflake.kafka.connector.internal.streaming.DirectTopicPartitionChannel)

This means that the offset is managed by SNOWLAKE , not kafka.

Option 1: Rename or delete the target Snowflake table
          Recreate it (optionally empty),
          Restart or re-post the connector config,
            The  Snowpipe Streaming will reset its internal tracking and reprocess from the beginning of the Kafka topic.

Option 2: Use a new topic name
        Kafka Connect + Snowflake associates the combination of topic + table name as a channel. If you:
        Send the same messages to a new topic (e.g., test_spipe_streaming_v2),  
        Update your connector config to map that topic to a new table,
        Repost the config,
            you'll get a fresh ingestion stream starting from offset 0.

Option 3 (Advanced): Manually reset the offset token in Snowflake
        This is not recommended or officially supported, but if you're deep into experimentation, it involves:

        Dropping or modifying the internal offset channel metadata,
        Which lives in Snowflake's metadata tables and is not documented for public manipulation.   
        

Kafka offset reset ≠ Snowpipe Streaming offset reset.
Snowpipe Streaming maintains its own internal offset tracking.

To fully reset:
    1_ Drop or rename the Snowflake table, OR
    Use a new Kafka topic and table mapping.

    2_ docker stop kafka-connect

    3_ Check kafka offset 
       I have no name!@kafka:/$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-test-streaming-snowpipe
       lag =0 , offset =0
    4_ Move offset to the earliest
       I have no name!@kafka:/$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group connect-test-streaming-snowpipe --topic test_spipe_streaming --reset-offsets --to-earliest --execute

    5_ docker start kafka-connect

    6_all messages reloaded

    
   drop table TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW
   select * from TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW
   
----------------------------------------------------------------------
-- How to convert the existing non schema evoution into a schema evolution with 
-- all rows.

1. Update new config (nothing happens, schema incopatible error )
2. Stop kafka connect 
3. Drop table
4. Change offset (kafka) -reset to earliest
5  Start kafka connect




Can you configure Kafka Connect + Snowflake Sink to reset offsets to earliest on restart or config update?
✅ Short Answer: No — not with Snowpipe Streaming.
🔍 Here's Why:
When using Snowflake Sink Connector with Snowpipe Streaming ingestion, the connector does not use Kafka consumer group offsets for tracking ingestion.

Instead, it:

Maintains internal ingestion offset tokens in Snowflake-managed metadata for each topic-table mapping.

Uses these tokens to resume ingestion even across connector restarts — ignoring the Kafka offsets unless it's a brand-new topic or table.

That means:

Setting auto.offset.reset=earliest (even if you add it to your config) has no effect on Snowpipe Streaming.

Restarting the connector or updating the config will not rewind ingestion to the start of the topic — unless the table or topic is new (as explained earlier).

🛠️ What About Other Modes?
If you're using Snowflake Sink Connector in regular Snowpipe (via COPY INTO) mode (i.e., not snowflake.ingestion.method=SNOWPIPE_STREAMING), the connector does use Kafka Connect offsets and the Kafka consumer group, so:

In that mode, auto.offset.reset can be relevant if the consumer group is new or offsets are missing.

But still, once offsets are committed, the connector will resume from there — and not reset on config change unless the group is re-created or the offset data is cleared.

🧩 Summary
Setting / Situation	Effect on Ingestion Offset?
auto.offset.reset=earliest	❌ No effect in Snowpipe Streaming
Restarting Kafka Connect	❌ Continues from stored Snowflake offset
Updating connector config	❌ No reset, continues from stored offset
Deleting + recreating table	✅ Resets Snowpipe Streaming ingestion
Using a new Kafka topic	✅ Starts fresh ingestion from offset 0
Manually resetting internal Snowflake offset	⚠️ Not supported; risky








    
Best Practice for Controlled Reprocessing:
When using Snowflake Sink Connector and schema evolution:

Always use new table names or reset offsets explicitly when reprocessing is needed.

Configure Kafka Connect with:

json
Copy
Edit
"consumer.auto.offset.reset": "earliest"
...only for new connector instances. This won’t help after the connector has started and committed offsets.



********************************************************************************/










------------------------
-- (3) Produce message
--------------------------

/*
1) RESTART SERVICE
create table if not exists identifier('LEXISNEXIS_STREAMING_NEW') (
    record_metadata variant 
    comment 'created by automatic table creation from Snowflake Kafka Connector'
);
alter table identifier('LEXISNEXIS_STREAMING_NEW') 
    set ENABLE_SCHEMA_EVOLUTION = true;

show tables like 'LEXISNEXIS_STREAMING_NEW' 
limit 1;

select SYSTEM$SNOWPIPE_STREAMING_MIGRATE_CHANNEL_OFFSET_TOKEN((?), (?), (?));



2) ADD NEW FIELDS TO THE SCHEMA

alter table identifier('LEXISNEXIS_STREAMING_NEW') 
    add column 
        if not exists "SURNAME" VARCHAR 
            comment 'column created by schema evolution from Snowflake Kafka Connector' , 
        if not exists "NAME" VARCHAR 
            comment 'column created by schema evolution from Snowflake Kafka Connector' 

3) REMOVE EXISTING FIELD FROM THE SCHEMA (no changes to the schema evolution!!)

4) RENAME THE EXISITNG COLUMN (ADD NEW COLUMN WITH THE NEW NAME)

*/

------------------------------------------
-- Testing
-----------------------------------------

-- no schema evolution
select * from TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW


drop table TEST_DB.PUBLIC.LEXISNEXIS_STREAMING_NEW






