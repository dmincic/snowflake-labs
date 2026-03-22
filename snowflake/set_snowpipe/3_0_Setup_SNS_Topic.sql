/*
We need to create SNS topics to capture specific errors coming from SF

a) create aws sns topic in the same region as SF account
b) this sns is not the one we created for Auto Pipe run
   1.) The S3 SNS Property set to send msgs to SF's SQL service. 
c) do not use FIFO for the error notification
*/

/*****************
1) CREATE SNS TOPIC
*****************/
-- Follow the instructions and create an SNS Topic. This is a straight forward process.
-- Capture the new resource's ARN
-- arn:aws:sns: sp... sns topic arn ...


/*******************
2) CREATE IAM POLICY
*******************/

/*
Create an AWS Identity and Access Management (IAM) policy that grants permissions to publish to the SNS topic. The policy defines the following actions:

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sns:Publish"
            ],
            "Resource": "arn: ... errors sns topic arn ..."
        }
    ]
}
*/

/*******************
2) CREATE ROLE
*******************/
/*
1) Trusted entity type: AWS Account
2) In the Account ID field, enter your own AWS account ID temporarily.
3) Select the Require external ID option. 
   This option enables you to grant permissions on your Amazon account resources (i.e. SNS) to a third party (i.e. Snowflake).
   
    This is simmilar to STORAGE_AWS_EXTERNAL_ID
   
    For now, enter a dummy ID such as 0000. Later, you will modify the trust relationship and replace the dummy ID with the external ID
    for the Snowflake IAM   user generated for your account. A condition in the trust policy for your IAM role allows your Snowflake users 
    to assume the role using the notification integration object you will create later.
    
4) Attach IAM policy
5) Assign name to the Role
   arn:aws:iam:: ... role/deansSnoflakeSNSrole
---------
Snoflake 
---------
6) Create NOTIFICATION INTEGRATION object
*/
--https://docs.snowflake.com/sql-reference/sql/create-notification-integration-queue-outbound-aws
CREATE OR REPLACE NOTIFICATION INTEGRATION PIPE_ERRORS_NOTIFICATION_INTEGREATION
    ENABLED = TRUE
    TYPE = QUEUE
    DIRECTION = OUTBOUND -- this indicates the direction of the cloud messaging with respect to Snowflake. 
    NOTIFICATION_PROVIDER = AWS_SNS
    AWS_SNS_TOPIC_ARN = 'arn:aws:sns:ap-southeast-2:. . . :Reltio_pipe_errors'
    AWS_SNS_ROLE_ARN = 'arn:aws:iam::. . . :role/deansSnoflakeSNSrole'
    COMMENT = 'Interacts with a SNS topic to send snowpipe errors'
-- NOTE: SF_AWS_EXTERNAL_ID cannot be explicitly assigned as with the STORAGE INTEGRATION object
/* 7 Get integration parameters*/
   DESCRIBE NOTIFICATION INTEGRATION PIPE_ERRORS_NOTIFICATION_INTEGREATION; 
/*
   SF_AWS_IAM_USER_ARN  sf user arn ...
   SF_AWS_EXTERNAL_ID   external id ...

Use the parameters above to update IAM ROLE

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "AWS": "sf user arn ..."
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": " external id ..."
        }
      }
    }
  ]
}
*/
