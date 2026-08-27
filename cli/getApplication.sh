if response=$(curl -k -s -w "%{http_code}" --request GET \
    --url https://${BASE_URL}/lakehouse/api/v3/spark_engines/${ENGINE_ID}/applications/${job_id} \
    --header 'LhInstanceId: '"${INSTANCE_ID}"'' \
    --header 'authorization: Bearer '"${bearer_token}"'' \
    --header 'content-type: application/json }' 2>&1); then

    http_code="${response: -3}"

    body="${response:0:${#response}-3}"

    if [[ "$http_code" == "200" ]]; then
      echo $body | jq .
    elif [[ "$http_code" == "401" ]]; then
      echo "Get Application failed"
      echo "Login Required - use getToken.sh"
    else
      echo "Get Application failed"
      echo $body
    fi
else
    echo "Get Application failed"
    echo $body
fi

