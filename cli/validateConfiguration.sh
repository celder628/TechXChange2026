#
# Copyright IBM Corp. 2026 - 2026
# SPDX-License-Identifier: Apache-2.0
#
if response=$(curl -s -w "%{http_code}" --request POST \
    --url https://${BASE_URL}/lakehouse/api/v3/spark_engines/${ENGINE_ID}/applications \
    --header 'LhInstanceId: '"${INSTANCE_ID}"'' \
    --header 'authorization: Bearer '"${bearer_token}"'' \
    --header 'content-type: application/json' \
    --data '{
    "application_details": {
        "application": "s3a://'"${S3_SOURCE_BUCKET}"'/applications/validate_config.py",
        "conf": {
        "spark.submit.pyFiles": "s3a://'"${S3_SOURCE_BUCKET}"'/applications/config.py,s3a://'"${S3_SOURCE_BUCKET}"'/applications/medallion.py",
        "ae.spark.executor.count": "2",
        "spark.driver.cores": "2",
        "spark.driver.memory": "8G",
        "spark.executor.cores": "4",
        "spark.executor.memory": "8G",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.endpoint": "'"${S3_ENDPOINT}"'",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.access.key": "'"${S3_ACCESS_KEY}"'",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.secret.key": "'"${S3_SECRET_KEY}"'"
        }
    }
    }' 2>&1); then

    http_code="${response: -3}"

    body="${response:0:${#response}-3}"

    if [[ "$http_code" == "202" ]]; then
      echo "Validate Configuration Job created successfully"
      export job_id=$(jq -r '.id' <<< "${body}")
    elif [[ "$http_code" == "401" ]]; then
      echo "Validate Configuration Job failed"
      echo "Login Required - use getToken.sh"
    else
      echo "Validate Configuration Job failed"
      echo "$body"
    fi
else
    echo "Validate Configuration Job failed"
fi
