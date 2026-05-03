/*
Set up test databases
*/
USE ROLE ACCOUNTADMIN;

--create test database
CREATE DATABASE IF NOT EXISTS THIRD_PARTY_DATA;
USE DATABASE THIRD_PARTY_DATA;

--this schema is not synced with the open catalog
CREATE SCHEMA IF NOT EXISTS THIRD_PARTY_DATA.VENDOR_SCHEMA;

/*********************************************
STEPS TO SET UP OPEN CATALOG (EXTERNAL CATALOG)
To be able to:
 1- Sink Snowflake iceberg tables
 Note: Kafka Connect does not have any specific 
       access requirement. It writes directly to the 
       existing (and mapped) Iceberg table
*********************************************/

/*
1. Create a Connection as a highest-level access object
   "Polaris is a central gatekeeper for Iceberg metadata. The service connection object is the 
    key to getting in. It defines Client ID/ Client Secret.
    Once your application/Client is in i.e. Snowflake, it will be assigned a set of permissions via
    another high-level object - The Principal Role
    "
2. Capture Client ID and secret!!! This is the only opportunity

    Client ID: 
    bQczA2JVphkRSOJP0lw4SBwtYLI=
    
    Client Secret:
    PgBkWUg4BWfCQdWpQuLuULldpXR6REKbCYhiuj0wrhk=
    
    ClientId:ClientSecret
    bQczA2JVphkRSOJP0lw4SBwtYLI=:PgBkWUg4BWfCQdWpQuLuULldpXR6REKbCYhiuj0wrhk=

 3. Create a new catalog: This will be space(or I think of it as a container) for the Interactions with Snowflake
    - The type of Catalog will be EXTERNAL

    Note: Credential vending is OFF (default). Turn it ON for the third party apps to be able to read.
          In that case Polaris provides a temp access to S3 to them

 4. Crate an IAM role /policy (or use the exisitng policy)
    - The role should have a temp Principal. The real principal will be the IAM User that will be provisioned after we finished 
      Polaris catalog setup - then we'll replace the temporary principal with the IAM User ARN
      step. Then we'll replace the real ARN with the temporary
 5. Create Polaris External Catalog
 
 6.Create a Catalog Role and assign Priviledges. Then Assign the Catalog role to the Principal role
   Note: Minimum access requirements: TABLE_CREATE, TABLE_WRITE_PROPERTIES, TABLE_DROP, NAMESPACE_CREATE, and NAMESPACE_DROP.
    
 7. Create CATALOG INTEGRATION OBJECT
*/
CREATE OR REPLACE CATALOG INTEGRATION sf_managed_catalog_integration
CATALOG_SOURCE = POLARIS
TABLE_FORMAT = ICEBERG
--CATALOG_NAMESPACE = 'snowflake_managed_namespace' -- THIS IS OPTIONAL FOR SNOWLAKE MANAGED 
--see https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-open-catalog#optional-parameters

--specifies information about your Open Catalog account and catalog name.
REST_CONFIG = (
    --CATALOG_URI = 'https://fdnffob-deanpolariscatalog.snowflakecomputing.com/polaris/api/catalog' -- Your Polaris Account URL + /polaris/api/catalog
    CATALOG_URI = 'https://ricxavh-dmincic_opencatalog.snowflakecomputing.com/polaris/api/catalog'
    CATALOG_NAME ='snowflake_managed_catalog' -- this is the name of the catalog we created in Polaris
    /*
        Think of it this way: Polaris acts as a control plane that manages your S3 buckets. You create a logical "warehouse" 
        in Polaris and map it to a physical S3 bucket location. When you interact with the Polaris REST API, you refer to this 
        warehouse by its logical name, and Polaris handles the mapping to the underlying S3 path.
    */
)
REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = 'bQczA2JVphkRSOJP0lw4SBwtYLI='
    OAUTH_CLIENT_SECRET = 'PgBkWUg4BWfCQdWpQuLuULldpXR6REKbCYhiuj0wrhk='
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
)
  ENABLED = TRUE;

  describe catalog integration  sf_managed_catalog_integration;
/*
  8. Create a shell Snowflake Iceberg table

  Note: We need to add at least one column. Without columns we get an error like:
  091361 (0A000): SQL Compilation error: Cannot create an Iceberg table with a Snowflake managed catalog integration 
  without specifying column schemas for table LEXISNEXIS_SBFE_RW.
*/
CREATE OR REPLACE ICEBERG TABLE THIRD_PARTY_DATA.VENDOR_SCHEMA.LEXISNEXIS_SBFE_RW (
	record_metadata OBJECT()
)
CATALOG = 'SNOWFLAKE'  -- default for Snowflake managed iceberg catalogs
EXTERNAL_VOLUME = 'iceberg_external_volume' -- volume integration
BASE_LOCATION = 'LEXISNEXIS_SBFE_RW';  -- The system will create an unique name like LEXISNEXIS_SBFE_RW.mMooh7rm/

/*
ref: https://docs.snowflake.com/en/user-guide/data-load-schema-evolution
Snowflake enables seamless handling of evolving semi-structured data. 
As data sources add new columns, Snowflake automatically updates table structures to reflect these changes, 
including the addition of new columns. This eliminates the need for manual schema adjustments. 
*/

--add column comment
ALTER ICEBERG TABLE THIRD_PARTY_DATA.VENDOR_SCHEMA.LEXISNEXIS_SBFE_RW
  ALTER COLUMN record_metadata
    SET COMMENT = 'Kafka topic metadata';


ALTER ICEBERG TABLE THIRD_PARTY_DATA.VENDOR_SCHEMA.LEXISNEXIS_SBFE_RW
    SET ENABLE_SCHEMA_EVOLUTION  = true;

ALTER ICEBERG TABLE THIRD_PARTY_DATA.VENDOR_SCHEMA.LEXISNEXIS_SBFE_RW
    SET CATALOG_SYNC = 'sf_managed_catalog_integration'; --Ingegration object

--CHECK THE RESULTS

SELECT * FROM THIRD_PARTY_DATA.VENDOR_SCHEMA.LEXISNEXIS_SBFE_RW;

/**********************
Internal settings
SHOW CHANNELS;
--channels exists - a channel per partition
***********************/

USE DATABASE THIRD_PARTY_DATA;
USE SCHEMA THIRD_PARTY_DATA.VENDOR_SCHEMA;
SHOW PIPES
    