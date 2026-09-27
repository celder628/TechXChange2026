#
# Copyright IBM Corp. 2026 - 2026
# SPDX-License-Identifier: Apache-2.0
#
set -a
source env.sh
set +a

if response=$(curl -k -s -w "%{http_code}" --request POST \
      --url https://${BASE_URL}/icp4d-api/v1/authorize \
      --header "Content-Type: application/json" \
      --data '{
        "username": "'"${WXD_USERNAME}"'",
        "api_key": "'"${WXD_API_KEY}"'"
      }' 2>&1); then

    http_code="${response: -3}"

    body="${response:0:${#response}-3}"

    if [[ "$http_code" == "200" ]]; then
      echo "Bearer Token created successfully"
      set -a
      export bearer_token=$(jq -r '.token' <<< "${body}")
      set +a
    else
      echo "Bearer Token creation failed"
    fi
else
    echo "Bearer Token creation failed"
fi
