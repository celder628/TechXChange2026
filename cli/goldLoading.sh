if response=$(curl -k -s -w "%{http_code}" --request POST \
    --url https://${BASE_URL}/lakehouse/api/v3/spark_engines/${ENGINE_ID}/applications \
    --header 'LhInstanceId: '"${INSTANCE_ID}"'' \
    --header 'authorization: Bearer '"${bearer_token}"'' \
    --header 'content-type: application/json' \
    --data '{
    "application_details": {
        "application": "/applications/gold_initial_loading.py",
        "conf": {
        "ae.spark.executor.count": "1",
        "spark.driver.cores": "1",
        "spark.driver.memory": "4G",
        "spark.executor.cores": "1",
        "spark.executor.memory": "4G",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.endpoint": "'"${S3_ENDPOINT}"'",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.access.key": "'"${S3_ACCESS_KEY}"'",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.secret.key": "'"${S3_SECRET_KEY}"'"
        },
        "env": {
          "BASE_URL": "'"${BASE_URL}"'",
          "APP_ENV": "'"${APP_ENV}"'",
          "WXD_USERNAME": "'"${WXD_USERNAME}"'",
          "WXD_API_KEY": "'"${WXD_API_KEY}"'",
          "S3_ACCESS_KEY": "'"${S3_ACCESS_KEY}"'",
          "S3_SECRET_KEY": "'"${S3_SECRET_KEY}"'",
          "S3_BUCKET_NAME": "'"${S3_BUCKET_NAME}"'",
          "S3_SOURCE_BUCKET": "'"${S3_SOURCE_BUCKET}"'",
          "S3_SOURCE_KEY": "'"${S3_SOURCE_KEY}"'",
          "S3_ENDPOINT": "'"${S3_ENDPOINT}"'",
          "MEDALLION_DEFAULT_CATALOG": "'"${MEDALLION_DEFAULT_CATALOG}"'",
          "MEDALLION_BRONZE_SCHEMA": "'"${MEDALLION_BRONZE_SCHEMA}"'",
          "MEDALLION_SILVER_SCHEMA": "'"${MEDALLION_SILVER_SCHEMA}"'",
          "MEDALLION_GOLD_SCHEMA": "'"${MEDALLION_GOLD_SCHEMA}"'"
        }        
    },
    "volumes": [
      {
        "name": "cpd::applications",
        "mount_path": "/applications"
      }
    ]
    }' 2>&1); then

    http_code="${response: -3}"

    body="${response:0:${#response}-3}"

    if [[ "$http_code" == "202" ]]; then
      echo "Gold Loading Job created successfully"
      export job_id=$(jq -r '.id' <<< "${body}")
    elif [[ "$http_code" == "401" ]]; then
      echo "Gold Loading Job failed"
      echo "Login Required - use getToken.sh"

    else
      echo "Gold Loading Job failed"
      echo $body
    fi
else
    echo "Gold Loading Job failed"
    echo $body
fi

