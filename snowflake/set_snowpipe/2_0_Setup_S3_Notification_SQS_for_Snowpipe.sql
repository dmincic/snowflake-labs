/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This section sets up the following AWS resources

1.1. S3 bucket event notification propety
     https://aws.amazon.com/blogs/aws/s3-event-notification/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

--get ARN of the AWS SQS resource -> This resource is created by Snowflake

-- set session context
USE WAREHOUSE DEAN_TEST;
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWPIPE_RAW;
USE SCHEMA SNOWPIPE_RAW.MDM_RELTIO;

DESCRIBE PIPE SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS;
-- notification_channel : arn:aws:sqs:ap-southeast-2:010928196459:sf-snowpipe-AIDAQFC27BNVWOZNYKIDX-ag_lsFVkvkLP1VqnQqDV5Q

------------------------------------------------
-- 1.1 Configure s3 bucket notification channel
------------------------------------------------
/*
    Name: Name of the event notification.
    Events: Select the ObjectCreate (All) option.
    Destination: Select SQS Queue from the radio buttons.
    SQS: Select Add SQS queue ARN from the radio buttons.
    SQS queue ARN: Paste the SQS queue ARN (notification channel) from the SHOW PIPES output.

    - Snowpipe SQS queues are created and managed by Snowflake
    - Following AWS guidelines, Snowflake designates no more than one SQS queue per S3 bucket. 
    - An SQS queue may be shared among multiple buckets in the same region from the same AWS account. 
    - The SQS queue coordinates notifications for all pipes connecting the external stages for the S3 buckets to the target tables. 
      When a data file is uploaded into the bucket, all pipes that match the stage directory path perform a one-time load of the file into their corresponding target tables.

      In general, we create SNS Topics. Amazon SNS is a highly available, durable, secure, fully managed pub/sub messaging service (Publisher - Subscriber methodology)
      that enables you to decouple distributed systems. Amazon SNS provides topics for high-throughput, push-based, many-to-many messaging.
      Once we create a SNS Topic we can define Subscribers.

      In the case of S3 SNS, we have limited options and the terminology is different, e.g. If we want messages to be handled by SQS , the SQS is not called subscriber, 
      but the SNS PUBLISH(SEND) the "event/MSG" to SQS queue to be read by a server(in our case Snowflake).


*/




