/*
SET UP SNOWPIPE

*/

--create a medium warehouse
CREATE WAREHOUSE IF NOT EXISTS DEAN_TEST
    WAREHOUSE_SIZE = MEDIUM;

USE WAREHOUSE DEAN_TEST;

--create snowpipe db
CREATE DATABASE IF NOT EXISTS SNOWPIPE_RAW;

--create schema
CREATE SCHEMA IF NOT EXISTS SNOWPIPE_RAW.MDM_RELTIO;

--set up db context
USE SCHEMA SNOWPIPE_RAW.MDM_RELTIO;

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE STORAGE INFORMATION OBJECT
1)
    AWS: Create a policy
    AWS: Create Role
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

-- CREATE AWS POLICY to access Snowflake
-- CREATE AWS ROLE based on the created policy 
-- ref: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE STORAGE INTEGRATION OBJECT

storage integrations allow SF to read data from and write data to an Amazon S3 bucket.
The bucket is referenced in an external (i.e. S3) stage. 

Integrations are named, first-class SF objects that store an AWS identity and access management (IAM) user ID. 
An administrator in your organization grants the integration IAM user permissions in the AWS account.

An integration can also list buckets (and optional paths) that limit the locations users can specify 
when creating external stages that use the integration.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/    
CREATE OR REPLACE STORAGE INTEGRATION  AWS_STORAGE_INTEGRATION
  TYPE = EXTERNAL_STAGE
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://dean-test-snowpipe/reltio/') -- THIS IS THE LIST 
  COMMENT = 'Dean, S3 storage integration'
  -- specific to AWS S3
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '. . .role/DeansSnowflakeRole'
  STORAGE_AWS_EXTERNAL_ID = ' . . . '

-- Describe the integration object

DESCRIBE STORAGE INTEGRATION AWS_STORAGE_INTEGRATION;
-- THE STORAGE_AWS_IAM_USER_ARN IS AUTOMATICALLY CREATED 

--validate integration object
-- SYSTEM$VALIDATE_STORAGE_INTEGRATION( '<storage_integration_name>', '<storage_path>', '<test_file_name>', '<validate_action>' )
select SYSTEM$VALIDATE_STORAGE_INTEGRATION(
        'AWS_STORAGE_INTEGRATION',
        's3://dean-test-snowpipe/reltio/',
        'sample_users_with_id_1.gz',
        'read'        
)

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE STAGE OBJECT 
2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
/*  drop stage, landing table and pipe for a clean test
    DROP STAGE IF EXISTS SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE;
    DROP TABLE IF EXISTS SNOWPIPE_RAW.MDM_RELTIO.USERS_STAGEDATA;
    DROP PIPE IF EXISTS  SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS;
*/


CREATE OR REPLACE STAGE SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE
    URL = 's3://dean-test-snowpipe/reltio/'
    STORAGE_INTEGRATION = AWS_STORAGE_INTEGRATION  
    FILE_FORMAT = ( TYPE=JSON
                    COMPRESSION = gzip
                    STRIP_OUTER_ARRAY = true
    )
    COMMENT ='Deans comment , Hello there';


    
show stages like 'SNOWPIPE__USERS__EXT_STAGE';
DESCRIBE STAGE SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE ;
LIST @SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE --LIST MAPPED FILES


/*  OPTIONAL NOT IN USE
------- version #2 use different credentials 
CREATE OR REPLACE STAGE SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__MDM_RELTIO__BASE__EXT_STAGE
    URL = 's3://dean-test-snowpipe/reltio/'
    CREDENTIALS=(AWS_KEY_ID='. .  .'
    AWS_SECRET_KEY='. . . ')
    FILE_FORMAT = ( TYPE=JSON
                    COMPRESSION = gzip
                    STRIP_OUTER_ARRAY = false
    );
*/    
------------------------------
--- test , query stage object:
------------------------------
select  $1 AS json_data,
        TO_VARCHAR(typeof(to_variant($1))) AS type,
        metadata$filename AS file_name,
        METADATA$FILE_LAST_MODIFIED AS stg_creation_timestamp,
        METADATA$START_SCAN_TIME AS insert_timestamp
from @SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE ;


/*
Create raw layer landing table (contains json data)
*/
CREATE OR REPLACE TABLE SNOWPIPE_RAW.MDM_RELTIO.USERS_STAGEDATA(
    json_data variant,
    json_data_type varchar,
    file_name varchar,
    stg_creation_timestamp timestamp,
       -- DEFAULT CURRENT_TIMESTAMP(),
    insert_timestamp timestamp
       -- DEFAULT CURRENT_TIMESTAMP()
);
    
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE PIPE OBJECT
2)  (the pipe object will create a target table automatically (DBT)
    . THe name of the table will match the pipe object)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

CREATE OR REPLACE PIPE SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS
  AUTO_INGEST=TRUE --parameter specifies to read event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.
  --AUTO_INGEST_FALSE
AS
  COPY INTO SNOWPIPE_RAW.MDM_RELTIO.USERS_STAGEDATA
    FROM (
            SELECT  $1 AS json_data,
                    TO_VARCHAR(typeof(to_variant($1))) AS type,
                    metadata$filename AS file_name,
                    METADATA$FILE_LAST_MODIFIED AS stg_creation_timestamp,
                    METADATA$START_SCAN_TIME AS insert_timestamp
            FROM @SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE 
    )
    FILE_FORMAT = (TYPE = JSON
                   COMPRESSION = gzip
                   STRIP_OUTER_ARRAY = true
    );  
    --PATTERN = '.*users.*.json';

----------------------------------
-- check the landing table
--------------------------------
SELECT * 
FROM SNOWPIPE_RAW.MDM_RELTIO.USERS_STAGEDATA; --5rows

LIST @SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__USERS__EXT_STAGE

-------------------------------
/*
    PIPE RUNS (AUTO_INGEST=TRUE)
    the pipe expects SQS messaging from S3.
    The initial files (the files that existed in  the S3 bucket before we created the PIPE will not be automatically ingested)
    We need to run ALTER PIPE/REFRESH 

    PIPE RUNS (AUTO_INGEST=TRUE)
    The pipe cannot run manually. We need to execute the COPY INTO command from the PIPE definition to ingest data from the stage.

*/
 -------------------------
 -- Load historical data (data added before we set up the utomatic snowpipe)
 -- 1 REFRESH PIPE (loads the last 7 days)
 -- 2. If more than 7 days , execute COPY INTO command manually 
 -- 3. REFRESH PIPE (to load data that may have arrived in between 1 and 2)


 
 ALTER PIPE SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS
   REFRESH; --takes a few seconds

--check status: (running if manual, no data import)
 SELECT SYSTEM$PIPE_STATUS('SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS');

--CHECK LOAD STATUS
select *
from table(information_schema.copy_history(
                TABLE_NAME=>'SNOWPIPE_RAW.MDM_RELTIO.USERS_STAGEDATA', 
                START_TIME=> DATEADD(days, -5, CURRENT_TIMESTAMP())
           )
);
 




 
















 



--WHERE load_status = 'LOAD_FAILED'
--AND last_load_time > dateadd(minute, -30, current_timestamp);


 

/* Create an EXTERNAL stage table including the format and integration object information
   Do not create  format/integration objects separately
*/

CREATE OR REPLACE STAGE SNOWPIPE_RAW.MDM_RELTIO.SNOWPIPE__MDM_RELTIO__BASE__EXT_STAGE
    URL= 's3://dean-test-snowpipe/reltio/'
COMMENT ='Deans comment , Hello there'
WITH TAG (Dean = 'this is reltio stage test')

---external params
externalStageParams (for Amazon S3) ::=
  URL = { 's3://<bucket>[/<path>/]' | 's3gov://<bucket>[/<path>/]' }

  [ { STORAGE_INTEGRATION = <integration_name> } | { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' [ AWS_TOKEN = '<string>' ] } | AWS_ROLE = '<string>'  } ) } ]
  [ ENCRYPTION = ( [ TYPE = 'AWS_CSE' ] [ MASTER_KEY = '<string>' ] |
                   [ TYPE = 'AWS_SSE_S3' ] |
                   [ TYPE = 'AWS_SSE_KMS' [ KMS_KEY_ID = '<string>' ] ] |
                   [ TYPE = 'NONE' ] ) ]


-----------------



/*~~~~~~~~~~~~~~~ 1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~
CREATE INGESTION TABLE (snowpipe's endpoint)
Note: The ingestion tables and other objects
are usually not in the same database as SNOWPIPE objects.
      This is due to security reasons.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
CREATE OR REPLACE TABLE SNOWPIPE_RAW.MDM_RELTIO.RELTIO (
        Id NUMBER(38, 0)
          CONSTRAINT PK_Customers_Id PRIMARY KEY,
        Customer_Id VARCHAR,
        First_Name VARCHAR,
        Last_Name VARCHAR,
        Company VARCHAR,
        City VARCHAR,
        Country VARCHAR,
        Phone_1 VARCHAR,
        Phone_2 VARCHAR,
        Email VARCHAR,
        Subscription_Date DATE,
        Website VARCHAR
);

/*~~~~~~~~~~~~~~ 2 ~~~~~~~~~~~~~~~~~~~~~
 CREATE FILE FORMAT OBJECT
 Note: this object can be automatically
       created through  STAGE object ddl
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
CREATE OR REPLACE FILE FORMAT SNOWPIPE_RAW.SNOWPIPE.CUSTOMERS_CSV_FILEFORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF =('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE;




