/*
1) Cretae a new, external catalog
2) Add a new catalog's external_id to the existing policy that allows access to the s3 baucket
  S3 BUCKET: s3://sf-iceberg-dean-test/polaris_external_iceberg/
        ROLE:arn:aws:iam::187866040890:role/polaris_s3_role   (allows pyiceberg to access s3 bucket, also allows polaris to access s3 bucket)
        PRINCIPAL: "AWS": "arn:aws:iam::184862803517:user/spb01000-s"   (POLARIS CONNECTION)
        POLICY - snowflake_access_iceberg --> allows access to the S3 bucket

*/