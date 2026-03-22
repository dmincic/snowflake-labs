/*
We can use Snoflake's first-level object called ERROR INTEGRATION 
to route errors that occurred across different SF objects like Dynamic tables, PIPES* etc.

The flow of data (in this case error payload) is as follows:

PIPE --> ERROR INTEGRATION (SF object) --> NOTIFICATION_INTEGRATION parameter --> SNS Topic (AWS) --> AWS CloudWatch --> xMatters

!! Many pipes can use the same NOTIFICATION INTEGRATION OBJECT
!! In case of PIPE error integration process, we don't need to create a separeate ERROR INTEGRATION object. Instead we can just 
alter the existing pipe and add the NOTIFICATION_INTEGRATION parameter with the value of the previously created NOTIFICATION INTEGRATION OBJECT name

example:
CREATE PIPE <name>
  [ AUTO_INGEST = TRUE | FALSE  ]
  ERROR_INTEGRATION = <integration_name>
  AS <copy_statement>

---------------------------------------------------------------------------
-- an example of an explicit error integration object (not used with Pipes)
---------------------------------------------------------------------------
The errors are routed to a pre-defined SNS TOPIC e.g.
CREATE ERROR INTEGRATION my_error_integration
   TYPE = SNS
   AWS_SNS_TOPIC_ARN = '<SNS topic ARN>'
   ENABLED = TRUE;
*/

/*
ALTER THE EXISTING PIPE and add the ERROR_INTEGRATION parameter
*/
DESCRIBE NOTIFICATION INTEGRATION PIPE_ERRORS_NOTIFICATION_INTEGREATION; 

ALTER PIPE SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS
SET ERROR_INTEGRATION = PIPE_ERRORS_NOTIFICATION_INTEGREATION;

--check pipe definition
DESCRIBE PIPE SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS;

/*
- The body of error messages identifies the pipe and the errors encountered during a load.
- The following is a sample message payload describing a Snowpipe error. The payload can include one or more error messages.
- Note that you must parse the string into a JSON object to process values in the payload.

{
   "version":"1.0",
   "messageId":"a62e34bc-6141-4e95-92d8-f04fe43b43f5",
   "messageType":"INGEST_FAILED_FILE",
   "timestamp":"2021-10-22T19:15:29.471Z",
   "accountName":"MYACCOUNT",
   "pipeName":"MYDB.MYSCHEMA.MYPIPE",
   "tableName":"MYDB.MYSCHEMA.MYTABLE",
   "stageLocation":"s3://mybucket/mypath",
   "messages":[
      {
         "fileName":"/file1.csv_0_0_0.csv.gz",
         "firstError":"Numeric value 'abc' is not recognized"
      }
   ]
}


 
*/


/****************************************
              TESTING (SNS TOPIC - error capturing)

*****************************************/
/*
1) Create an email subscription to the SNS Topic
2) Verify subscription
3) Add a file to  the s3 bucket that is not formatted as defined in the PIPE format section
   - this will result in the error
4) Check if you received email with the error.

*/









/*
The topic will collect errors across different SF objects we attached the ERROR INTEGRATION object to.
This means that we need to create a mechanism to filter only the errors we are interested in e.g. dynamic tables etc.
e.g a lambda function can be added to filter for e.g. dynamic tables errors only

Key Considerations:
Error Visibility:

Snowflake currently doesn't differentiate between error types in the ERROR_INTEGRATION. Both dynamic table and other operational errors 
(e.g., Snowpipe or task failures) can be sent to the same SNS topic. If needed, add logic (e.g., a Lambda function) to filter for dynamic table-specific errors.
Testing and Validation:

Validate that the error payloads from dynamic tables meet your requirements and are routed correctly to your monitoring tools.
Automated Resolution:

While notifications handle alerting, use additional tools like Lambda to attempt automated recovery (e.g., retrying a refresh).

------------------------
-- PIPE OBJECT HAS A PRE-DEFINED ERROR_INTEGRATION PARAMETER that filters only
-- the PIPE object-related errors, so we don't need Lambda logic to distinguish 
------------------------
*/



/******************************
CREATE ERROR INTEGRATION OBJECT
*******************************/





