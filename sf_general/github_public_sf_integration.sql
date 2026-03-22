/*
To integrate a GitHub public repository with Snowflake, we need to create two Snowflake objects
1. API Integration object
2. Git Repository clone object

The API Integration object specifies the allowed URL prefixes for the Git server.
Since it's a public repository, no credentials are included in the integration.

The Git Repository clone object references the API integration and the specific public repository URL.

Once the repository clone is created, files from the remote public repository are synchronized to Snowflake, 
where you can then reference them in notebooks, stored procedures, and other Snowflake objects. 
You will need to periodically ALTER GIT REPOSITORY ... FETCH to pull the latest changes from the remote to the Snowflake clone.
*/

/*
Create an API Integration that specifies the allowed URL prefixes for the Git server. 
Since it's a public repository, no credentials are included in the integration.
*/
CREATE OR REPLACE API INTEGRATION dean_public_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/dmincic/snowflake-labs') -- Replace with the base URL of your public repo
  ENABLED = TRUE;


/*
Create DB:
*/
CREATE OR REPLACE DATABASE DEAN_SF_LABS;
USE DATABASE DEAN_SF_LABS;
/*
Create a Git Repository clone object in Snowflake, 
referencing the API integration and the specific public repository URL.

--This action will create a GitRepository object in the DEAN_SF_LABS.PUBLIC  schema
(we can specify any schema, PUBLIC schema is DEFAULT)
*/
CREATE OR REPLACE GIT REPOSITORY dean_snowflake_labs_repo_clone
  ORIGIN = 'https://github.com/dmincic/snowflake-labs' -- Replace with the full URL of your public repo
  API_INTEGRATION = dean_public_git_api_integration;

SHOW GIT REPOSITORIES IN DATABASE DEAN_SF_LABS;

/*
Once the repository clone is created, files from the remote public repository are synchronized to Snowflake, 
where you can then reference them in notebooks, stored procedures, and other Snowflake objects. 
You will need to periodically ALTER GIT REPOSITORY ... FETCH to pull the latest changes from the remote to the Snowflake clone.

*/


ALTER GIT REPOSITORY DEAN_SF_LABS.PUBLIC.DEAN_SNOWFLAKE_LABS_REPO_CLONE
    FETCH ;