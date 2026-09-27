#
# Copyright IBM Corp. 2026 - 2026
# SPDX-License-Identifier: Apache-2.0
#
# submitJob.sh — shared helper called by all loading scripts.
#
# Usage (source this file, then call submit_job):
#   LAYER=bronze STAGE=initial source submitJob.sh
#

submit_job() {
  local layer="${LAYER}"
  local stage="${STAGE}"
  local label="$(tr '[:lower:]' '[:upper:]' <<< "${layer:0:1}")${layer:1} $(tr '[:lower:]' '[:upper:]' <<< "${stage:0:1}")${stage:1} Loading"   # e.g. "Bronze Initial Loading"

  # Optionally inject spark.jars.packages when the env var is set
  local jars_packages_entry=""
  if [[ -n "${SPARK_JARS_PACKAGES:-}" ]]; then
    jars_packages_entry='"spark.jars.packages": "'"${SPARK_JARS_PACKAGES}"'",'
  fi

  # Optionally inject spark.archives when the env var is set
  local archives_entry=""
  if [[ -n "${SPARK_ARCHIVES:-}" ]]; then
    archives_entry='"spark.archives": "'"${SPARK_ARCHIVES}"'",'
  fi

  # Inject Delta Lake catalog extensions when table format is delta
  local delta_conf_entry=""
  if [[ "${MEDALLION_TABLE_FORMAT:-}" == "delta" ]]; then
    delta_conf_entry='"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",'
  fi

  if response=$(curl -k -s -w "%{http_code}" --request POST \
      --url https://${BASE_URL}/lakehouse/api/v3/spark_engines/${ENGINE_ID}/applications \
      --header 'LhInstanceId: '"${INSTANCE_ID}"'' \
      --header 'authorization: Bearer '"${bearer_token}"'' \
      --header 'content-type: application/json' \
      --data '{
      "application_details": {
          "application": "/applications/'"${layer}"'_'"${stage}"'_loading.py",
          "conf": {
            '"${jars_packages_entry}"'
            '"${archives_entry}"'
            '"${delta_conf_entry}"'
            "ae.spark.executor.count":"'"${AE_SPARK_EXECUTOR_COUNT:-1}"'",
            "spark.driver.cores": "'"${SPARK_DRIVER_CORES:-1}"'",
            "spark.driver.memory": "'"${SPARK_DRIVER_MEMORY:-4G}"'",
            "spark.executor.cores": "'"${SPARK_EXECUTOR_CORES:-1}"'",
            "spark.executor.memory": "'"${SPARK_EXECUTOR_MEMORY:-4G}"'"
          },
          "env": {
            "BASE_URL": "'"${BASE_URL}"'",
            "APP_ENV": "'"${APP_ENV:-onprem}"'",
            "WXD_USERNAME": "'"${WXD_USERNAME}"'",
            "WXD_API_KEY": "'"${WXD_API_KEY}"'",
            "STORAGE_TYPE": "'"${STORAGE_TYPE:-s3}"'",
            "SOURCE_CSV": "'"${SOURCE_CSV:-cfpb/complaints.csv}"'",
            "S3_ACCESS_KEY": "'"${S3_ACCESS_KEY}"'",
            "S3_SECRET_KEY": "'"${S3_SECRET_KEY}"'",
            "S3_BUCKET_NAME": "'"${S3_BUCKET_NAME:-wx-medallion-backup-test}"'",
            "S3_SOURCE_BUCKET": "'"${S3_SOURCE_BUCKET:-wx-medallion-src}"'",
            "S3_ENDPOINT": "'"${S3_ENDPOINT:-https://s3.us-east-1.amazonaws.com}"'",
            "ADLS_STORAGE_ACCOUNT": "'"${ADLS_STORAGE_ACCOUNT}"'",
            "ADLS_ACCOUNT_KEY": "'"${ADLS_ACCOUNT_KEY}"'",
            "ADLS_CONTAINER": "'"${ADLS_CONTAINER:-wx-medallion}"'",
            "ADLS_SOURCE_CONTAINER": "'"${ADLS_SOURCE_CONTAINER:-wx-medallion-src}"'",
            "MEDALLION_TABLE_FORMAT": "'"${MEDALLION_TABLE_FORMAT:-iceberg}"'",
            "MEDALLION_DEFAULT_CATALOG": "'"${MEDALLION_DEFAULT_CATALOG:-wx_medallion}"'",
            "MEDALLION_BRONZE_SCHEMA": "'"${MEDALLION_BRONZE_SCHEMA:-cfpb_bronze}"'",
            "MEDALLION_SILVER_SCHEMA": "'"${MEDALLION_SILVER_SCHEMA:-cfpb_silver}"'",
            "MEDALLION_GOLD_SCHEMA": "'"${MEDALLION_GOLD_SCHEMA:-cfpb_gold}"'",
            "KAFKA_BOOTSTRAP_SERVERS":"'"${KAFKA_BOOTSTRAP_SERVERS}"'",
            "KAFKA_TOPIC": "'"${KAFKA_TOPIC}"'",
            "KAFKA_SECURITY_PROTOCOL": "'"${KAFKA_SECURITY_PROTOCOL}"'",
            "KAFKA_SASL_USERNAME": "'"${KAFKA_SASL_USERNAME}"'",
            "KAFKA_SASL_PASSWORD": "'"${KAFKA_SASL_PASSWORD}"'",
            "KAFKA_SSL_CA_CERT": "'"${KAFKA_SSL_CA_CERT}"'"
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
        echo "${label} Job created successfully"
        set -a
        export job_id=$(jq -r '.id' <<< "${body}")
        set +a
      elif [[ "$http_code" == "401" ]]; then
        echo "${label} Job failed"
        echo "Login Required - use getToken.sh"
      else
        echo "${label} Job failed"
        echo "$body"
      fi
  else
      echo "${label} Job failed"
      echo "$body"
  fi
}

submit_job