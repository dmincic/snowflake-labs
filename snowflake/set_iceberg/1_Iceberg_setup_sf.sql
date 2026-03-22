/***************************************
Configure an external volume for Amazon S3
****************************************/

--(1)-- Create S3 bucket: arn:aws:s3:::sf-iceberg-dean-test

--(2)-- Create IAM Policy for Snowflake access to the S3 bucket
/*******************************************************************
-- Name: arn:aws:iam::187866040890:policy/snowflake_access_iceberg

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
            "Resource": "arn:aws:s3:::sf-iceberg-dean-test/iceberg/*"
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
                        "iceberg/*"
                    ]
                }
            }
        }
    ]
}
*******************************************************************/

--(3)-- Create IAM Role and Attach the Policy to the Role
/*********************************************************
-- Name: arn:aws:iam::187866040890:role/sf_access_iceberg
-- ExternalId: "iceberg_table_external_id"  <-- used to distinguish between multiple Sf. objects. 
-- Snoflake has only one IAM User, so we need one more information to uniquely identify Principal.

-- Principal: TEMPORARY ... will be replaced with Snoflake's IAM User
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::187866040890:root"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "iceberg_table_external_id"
                }
            }
        }
    ]
}
********************************************************************/

--(4)-- Create an external volume in Snowflake
-- Use IAM Role information & S3 bucket name
/******************************************************************/
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'sf_managed_iceberg'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://sf-iceberg-dean-test/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::187866040890:role/sf_access_iceberg'
            STORAGE_AWS_EXTERNAL_ID = 'iceberg_table_external_id'
         )
      )
      ALLOW_WRITES = TRUE;
      
-- Check the external storage: Note: The scope of this object is SF account
SHOW EXTERNAL VOLUMES
   LIKE 'ICEBERG%';
/******************************************************************/

--(5)-- Locate Snowflake's IAM user: Snowflake provisions a single IAM user for the entire Snowflake account.
-- This information can be retrieved by DESCRIBING objects like INTEGRATION, STAGE, EXTERNAL VOLUME ..etc
/***********************************************************************************/
DESC EXTERNAL VOLUME iceberg_external_volume;



--result
{
    "NAME": "my-s3-us-west-2",
    "STORAGE_PROVIDER": "S3",
    "STORAGE_BASE_URL": "s3://sf-iceberg-dean-test/iceberg/",
    "STORAGE_ALLOWED_LOCATIONS": [
        "s3://sf-iceberg-dean-test/iceberg/*"
    ],
    "STORAGE_AWS_ROLE_ARN": "arn:aws:iam::187866040890:role/sf_access_iceberg",
    "STORAGE_AWS_IAM_USER_ARN": "arn:aws:iam::940482416158:user/nuyx0000-s",  <-- snoflake IAM User
    "STORAGE_AWS_EXTERNAL_ID": "iceberg_table_external_id",
    "ENCRYPTION_TYPE": "NONE",
    "ENCRYPTION_KMS_KEY_ID": ""
}
/************************************************************************************/
--(6)-- Update IAM Role (Trust Relationships) and replace the default Principal "arn:aws:iam::187866040890:root"
    --  with Snowflake's IAM User - "arn:aws:iam::940482416158:user/nuyx0000-s"

/*********************************************************
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::940482416158:user/nuyx0000-s"
			},
			"Action": "sts:AssumeRole",
			"Condition": {
				"StringEquals": {
					"sts:ExternalId": "iceberg_table_external_id"
				}
			}
		}
	]
}
********************************************************************/
--(7)-- Verify storage access (Can Snoflake access storage provider - S3)

-- Use system function below to verify storage access
-- WARNING! The test checks the external storage STORAGE_BASE_URL = 's3://sf-iceberg-dean-test/iceberg/'
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('iceberg_external_volume');

-- result
/*
{
    "success": true,
    "storageLocationSelectionResult": "PASSED",
    "storageLocationName": "my-s3-us-west-2",
    "servicePrincipalProperties": "STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::940482416158:user/nuyx0000-s; STORAGE_AWS_EXTERNAL_ID: iceberg_table_external_id",
    "location": "s3://sf-iceberg-dean-test/iceberg/",
    "storageAccount": null,
    "region": "ap-southeast-2",
    "writeResult": "PASSED",
    "readResult": "PASSED",
    "listResult": "PASSED",
    "deleteResult": "PASSED",
    "awsRoleArnValidationResult": "PASSED",
    "azureGetUserDelegationKeyResult": "SKIPPED"
}
*/

/****************************************************
Snowflake-sink-connector requires 

--(8)-- Create sf private key and passphrase (This is required by SF Kafka Connector!))

- Create private key pair (wsl2)
  - `openssl genrsa -out snowflake_key.pem 2048`
  - `openssl pkcs8 -topk8 -inform PEM -outform DER -in snowflake_key.pem -out snowflake_key.p8 -nocrypt`
    - To encrypt with a passphrase (openssl will ask to enter a passphrase on the screen)
      - `openssl pkcs8 -topk8 -inform PEM -outform DER -in snowflake_key.pem -out snowflake_key.p8` 
      - In this case , add `"snowflake.private.key.passphrase": "your_passphrase"` to ![snowflake-sink.json](../../snowflake-sink.json)
- Base64 encode the DER-format key:
  - `base64 snowflake_key.p8 > snowflake_key.b64`
- Paste the one-line base64 string from snowflake_key.b64 into ![snowflake-sink.json](../../snowflake-sink.json)
- Extract the public key(RSA) from the private key(we need that for SFlake)
  - `openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub`
  - `openssl rsa -in snowflake_key.p8 -inform DER -pubout -out snowflake_key.pub`
- SNOWFLAKE:
- ```ALTER USER dmincic SET RSA_PUBLIC_KEY='<public key here , no cr no new lines>'```

*/

ALTER USER dmincic SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmiPCBOUwpGCn79rxHsEq8Lci90RKOqVb3GsvZp1F55nx2upAVSNPDg9JKmCY7mL8XZU6njKHcVHKn0YHu7LNTK3aCRUPeALmm2+m/scYSbjQZV7YsFRC8lEUF0wWyln3Mq48bQloZ6E9B/3F0/MscdH+nUaBwI5oB7XIMdbF6cXkTpFrKeRSYisyPxvdo3o6gLuSNJeyFb/lyYf89eC8Jpf9ih9lJG1sd33AuY+VhFIBSQ/PmVEyzI/K/zBXgmfD45Qksxx06XUzgBsN4HYkK6MrhDmxxr0DAE4gUkTnh5GTp+rqFYzoAWRJSbs8BrPfBiub+PMDWB/IA08BnG0tlQIDAQAB';


