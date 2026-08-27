if response=$(curl -k -s -w "%{http_code}" --request POST \
    --url https://${lakehouse_url}/lakehouse/api/v3/spark_engines/${engine_id}/applications \
    --header 'authinstanceid: '"${CRN}"'' \
    --header 'authorization: Bearer '"${bearer_token}"'' \
    --header 'content-type: application/json' \
    --data '{
    "application_details": {
        "application": "s3a://'"${source_bucket}"'/applications/initial_loading_all_steps.py",
        "conf": {
        "spark.submit.pyFiles": "s3a://'"${source_bucket}"'/applications/config.py,s3a://'"${source_bucket}"'/applications/medallion.py",
        "ae.spark.executor.count": "2",
        "spark.driver.cores": "2",
        "spark.driver.memory": "16G",
        "spark.executor.cores": "4",
        "spark.executor.memory": "16G",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.endpoint": "'"${source_bucket_url}"'",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.access.key": "'"${source_bucket_access_key}"'",
        "spark.hadoop.fs.s3a.bucket.wx-data-medallion-src.secret.key": "'"${source_bucket_secret_key}"'"
        }
    }
    }' 2>&1); then

    http_code="${response: -3}"

    body="${response:0:${#response}-3}"

    if [[ "$http_code" == "202" ]]; then
      echo "Initial Loading All Steps Job created successfully"
      export job_id=$(jq -r '.id' <<< "${body}")
    elif [[ "$http_code" == "401" ]]; then
      echo "Initial Loading All Steps Job failed"
      echo "Login Required - use getToken.sh"

    else
      echo "Initial Loading All Steps Job failed"
      echo $body
    fi
else
    echo "Initial Loading All Steps Job failed"
    echo $body
fi

