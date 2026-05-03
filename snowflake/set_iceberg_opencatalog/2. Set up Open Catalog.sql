--------- (1) ---------
/****************************************************

Create policy (add the same S3 bucket into the existing policy, add location)
arn:aws:iam::187866040890:policy/snowflake_access_iceberg
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::sf-iceberg-dean-test/iceberg/*",
                "arn:aws:s3:::sf-iceberg-dean-test/polaris_iceberg/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::sf-iceberg-dean-test",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "iceberg/*",
                        "polaris_iceberg/*"
                    ]
                }
            }
        }
    ]
}


**************************************************/



--------- (2) ---------
/***********************
Create IAM role
arn:aws:iam::187866040890:role/SnowflakeIcebergPolarisS3Role
************************/



-----3--------------
/*********************
Craete external volume
**********************/
USE ROLE ACCOUNTADMIN; -- Or a role with CREATE EXTERNAL VOLUME privilege

CREATE OR REPLACE EXTERNAL VOLUME my_polaris_s3_ev
STORAGE_LOCATIONS = (
    (
        NAME = 'polaris_iceberg_data_location',
        STORAGE_PROVIDER = 'S3',
        STORAGE_BASE_URL = 's3://sf-iceberg-dean-test/', --  S3 bucket name
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::187866040890:role/SnowflakeIcebergPolarisS3Role' --  IAM Role ARN
    )
)
COMMENT = 'External Volume for Snowflake Open Catalog (Polaris) Iceberg tables';

describe external volume my_polaris_s3_ev;

-- Verify the external volume (this will output connection details)
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('my_polaris_s3_ev');
-- get external_id and sf user and replace it in the Role Trust relationship
-- user:         arn:aws:iam::184862803517:user/k0yz0000-s
-- external_id : WA00654_SFCRole=5_z9DVJ4u5MUq66X5wK/C646D4hXY=
-- NOTE: if we recreate external volume, the external id will be differnt

	"Condition": {
				"StringLike": {
					"s3:prefix": [
						"*",
						"iceberg/*",
						"polaris_iceberg/*"
					]
				}
			}

-----3--------------
/*******************************
Create Catalog Integration Object

a) Create Service Connection
 Client ID: yipu2g7kqcpI9obUP+woi2g7/rU=
 Client Secret:t2igRBaIfb1aeDw/AZ8Ndj54ygRfQz6SSuTqWTWWQIs=
 As <client id>:<secret> yipu2g7kqcpI9obUP+woi2g7/rU=:t2igRBaIfb1aeDw/AZ8Ndj54ygRfQz6SSuTqWTWWQIs=

*********************************/
USE ROLE ACCOUNTADMIN; -- Or a role with CREATE CATALOG INTEGRATION privilege

-- this is the link between snowflake and the open catalog. Even if its developed by snowflake, it is treated as a separate service

CREATE OR REPLACE CATALOG INTEGRATION my_polaris_catalog_int
CATALOG_SOURCE = POLARIS
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = 'test_polaris_namespace' -- Choose a meaningful namespace for your tables in Polaris
                                             -- this defines a logical grouping for your tables within the Polaris catalog. It's like a database or schema name within Polaris
REST_CONFIG = (
    CATALOG_URI = 'https://fdnffob-deanpolariscatalog.snowflakecomputing.com/polaris/api/catalog' -- Your Polaris Account URL + /polaris/api/catalog
    WAREHOUSE = 'test_polaris_catalog' -- this is the name of the catalog we created in Polaris
    /*
        Think of it this way: Polaris acts as a control plane that manages your S3 buckets. You create a logical "warehouse" 
        in Polaris and map it to a physical S3 bucket location. When you interact with the Polaris REST API, you refer to this 
        warehouse by its logical name, and Polaris handles the mapping to the underlying S3 path.
    */
)
REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = 'NcP8kLTkW78IgR4oASdVTdEYJrE='
    OAUTH_CLIENT_SECRET = 'Xjb9kDeSX8CHQVaxCShYd/rO2q8F31OdbyZQNfbS8/I='
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
)
  ENABLED = TRUE;


  describe catalog integration  my_polaris_catalog_int

 ------------4A--------------
/**************************************************************
Create an internally managed table - Iceberg table
/**************************************************************
Before sync a Snowflake-managed Iceberg table to Open Catalog,
specify the external catalog in Open Catalog that Snowflake should sync the table to.

To set up catalog sync, use the ALTER DATABASE command with the CATALOG_SYNC parameter. 
For the value of this parameter, specify the name of the catalog integration object

After running this code, Snowflake syncs all Snowflake-managed Iceberg tables in the MY_ICEBERG database 
to the "test_polaris_catalog"(defined in the "my_polaris_catalog_int" catalog integration object)

*********************************/ 
ALTER DATABASE MY_ICEBERG
    SET CATALOG_SYNC = 'my_polaris_catalog_int';

-- create snowflake managed table(s) under the MY_ICEBERG,SYNC_TO_POLARIS database/schema
/*
IMPORTANT NOTE:
If we defined Snowflake database as catalog sync, the schema will be automatically created 
as the catalog's namespace. A catalog can have 0,1 or more namespaces. A namespace can have 0,1,more namespaces AND/OR 0,1 or more tables

What happens if we create a regular database and under that database a schema that is synced with the catalog.
What if we have normal db and schema and only a table is mapped to a catalog table. How we navigate to get the right position of the table.
*/

 ------------4A--------------
CREATE OR REPLACE SCHEMA MY_ICEBERG.SYNC_TO_POLARIS;

USE DATABASE MY_ICEBERG;
USE SCHEMA SYNC_TO_POLARIS;

CREATE OR REPLACE ICEBERG TABLE my_sf_managed_table (col1 int)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'my_polaris_s3_ev'
  BASE_LOCATION = 'polaris_iceberg/sf_managed_iceberg/';


---TEST THE TABLE
INSERT INTO my_sf_managed_table
  SELECT 1
  UNION ALL
  SELECT 2

SELECT * FROM my_sf_managed_table

-----test 4.1 -----------------------
/*
Create a normal database and 
a schema that is mapped to the catalog? Can we map

*/
CREATE OR REPLACE DATABASE MY_REGULAR_DB;

CREATE SCHEMA MY_REGULAR_DB.MY_ICEBERG_MANAGED_SCHEMA;

ALTER SCHEMA MY_REGULAR_DB.MY_ICEBERG_MANAGED_SCHEMA
    SET CATALOG_SYNC = 'my_polaris_catalog_int'; 
--at this stage , no changes in Polaris. No new namespace have been created.

--crate snowflake managed iceberg table
CREATE OR REPLACE ICEBERG TABLE MY_REGULAR_DB.MY_ICEBERG_MANAGED_SCHEMA.my_sf_iceberg_managed_table1 (col11 int)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'my_polaris_s3_ev'
  BASE_LOCATION = 'polaris_iceberg/sf_managed_iceberg/';

--- test 4.2 ---
/*
What happens if I create a new schema? Would it be visible in Polaris
What happens if I create a regular table within the schema that is visible in Polaris . 
*/


CREATE OR REPLACE SCHEMA MY_REGULAR_DB.MY_REGULAR_SCHEMA
--NOT VISIBLE,, ... GOOD RESULT

--test 4.3. Lets create catalog synced iceberg table


CREATE OR REPLACE ICEBERG TABLE MY_REGULAR_DB.MY_REGULAR_SCHEMA.my_sf_iceberg_managed_table_synced (col12 int)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'my_polaris_s3_ev'
  BASE_LOCATION = 'polaris_iceberg/sf_managed_iceberg/';
 --creates only s3 metadata entry, nothigh else

 ALTER ICEBERG TABLE MY_REGULAR_DB.MY_REGULAR_SCHEMA.my_sf_iceberg_managed_table_synced
    SET CATALOG_SYNC = 'my_polaris_catalog_int'; 

---add some rows
insert into MY_REGULAR_DB.MY_REGULAR_SCHEMA.my_sf_iceberg_managed_table_synced
  select 111
  union all
  select 222;

    

--create only snowflake managed iceberg table, do not sync with Polaris
CREATE OR REPLACE ICEBERG TABLE MY_REGULAR_DB.MY_REGULAR_SCHEMA.my_sf_iceberg_managed_table_not_synced (col13 int)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'my_polaris_s3_ev'
  BASE_LOCATION = 'polaris_iceberg/sf_managed_iceberg/';
 --creates only s3 metadata entry, nothing else. It is not synced!

---test 4.4. what if I want to create a regular table under a synced schema

CREATE OR REPLACE TABLE MY_REGULAR_DB.MY_ICEBERG_MANAGED_SCHEMA.MY_NORMAL_TABLE (col55 int);

insert into MY_REGULAR_DB.MY_ICEBERG_MANAGED_SCHEMA.MY_NORMAL_TABLE
 select 33 
 union all
 select 44

 select * from MY_REGULAR_DB.MY_ICEBERG_MANAGED_SCHEMA.MY_NORMAL_TABLE
 -- nothing unusual happend! the normal table acts as a normal table... :)

 
 /*
 CONCLUSION !!!!

 1) CREATE ICEBERG TABLE  in Snowflake ALWAYS creates an internal catalog that manages iceberg files in S3. The internal catalog is not exposed
 2) When we set property CATALOG_SYNC to the catalog integration the following happens

     a) if the sync is on DB level
            - all iceberg tables under any schema that is created under that db will be mapped in Polaris. The schema = namespace under the db. the db is namespace under the 
              catalog
     b) if the sync is on the Schema level
             - all iceberg tables under that specific schema will be synced and visible in Polaris
             - The very first table created under that schema will initiate system to 
             - create a new namespace = db name, and a nested namespace = schema name
     c) if the sync is on the table levl
            - the very first iceberg table created under a regular db and schema, and after  alter table set catalog_sync
              will create a namespace = db, sub namespace = schema and the table
 PRETTY COOL STUFF !!!!!
 
 */

-- try to create a bi-directional system spark/snowflake r/w
-- snowflake does not rely on the internal catalog, no sync


--craete schema bound to a catalog / no sync
CREATE SCHEMA IF NOT EXISTS MY_REGULAR_DB.shared_iceberg_schema
  CATALOG = my_polaris_catalog_int;

--crate
CREATE ICEBERG TABLE MY_REGULAR_DB.shared_iceberg_schema.bidirectional_table
  EXTERNAL_VOLUME = 'my_polaris_s3_ev'
  CATALOG = my_polaris_catalog_int
AS
SELECT 
  'O1' AS order_id,
  'C1' AS customer_id,
  125.50 AS order_total,
  TO_DATE('2025-06-21') AS order_date;




-----------investingation
                   arn:aws:iam::184862803517:user/spb01000-s
AWS_IAM_USER_ARN":"arn:aws:iam::184862803517:user/k0yz0000-s











 
/*******************************
Create an externally managed table - Iceberg table
*********************************/ 
CREATE DATABASE IF NOT EXISTS my_data_lakehouse;
CREATE SCHEMA IF NOT EXISTS my_data_lakehouse.iceberg_schema;

USE DATABASE my_data_lakehouse;
USE SCHEMA iceberg_schema;


CREATE OR REPLACE ICEBERG TABLE my_first_polaris_table --(
-- record_metadata OBJECT()
--)
EXTERNAL_VOLUME = my_polaris_s3_ev -- the EXTERNAL VOLUME
CATALOG = my_polaris_catalog_int -- the CATALOG INTEGRATION y
--CATALOG_TABLE_NAME = 'my_remote_table'
AUTO_REFRESH = TRUE;

BASE_LOCATION = 'polaris_iceberg/my_first_polaris_table/'; -- This is a *relative* path within S3 bucket's external volume root.
                                                           -- s3://sf-iceberg-dean-test/polaris_iceberg/my_first_polaris_table/
COMMENT = 'An Iceberg table managed by Snowflake Open Catalog (Polaris)';


ALTER ICEBERG TABLE TEST_DB.PUBLIC.LEXISNEXIS_ICEBERG 
    SET ENABLE_SCHEMA_EVOLUTION  = true;






















CREATE ICEBERG TABLE open_catalog_iceberg_table
  CATALOG = 'my_polaris_catalog_int'
  EXTERNAL_VOLUME = 'my_polaris_s3_ev'
  CATALOG_TABLE_NAME = 'my_iceberg_table'
  AUTO_REFRESH = TRUE;








            

/*******************************************
Configure open catalog schema
********************************************/
CREATE OR REPLACE DATABASE MY_ICEBERG;
USE DATABASE MY_ICEBERG;

--1-- Create an open catalog schema
/*
That schema acts as a namespace in Open Catalog.
    -   It's the entry point for Snowflake to register and find tables in the open catalog.
    -   Each table created under it will be:
        -   Visible via the Iceberg REST API
        -   Queryable from Snowflake SQL
        -Modifiable from Spark

Without this schema, Snowflake wouldn’t know that it should use the Open Catalog 
it would default to its internal (closed) catalog.

The only difference is that by saying CATALOG = 'OPEN_CATALOG', you’re telling Snowflake:
“When creating or managing tables in this schema, use the Open Catalog system, not the internal one.”
*/
CREATE SCHEMA MY_ICEBERG.DEAN_ICEBERG_SHARED_SCHEMA
  CATALOG = 'OPEN_CATALOG';

  