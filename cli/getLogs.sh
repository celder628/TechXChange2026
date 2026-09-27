#
# Copyright IBM Corp. 2026 - 2026
# SPDX-License-Identifier: Apache-2.0
#

if response=$(curl -k -w "%{http_code}" --request GET \
  --url https://${BASE_URL}/zen-volumes/${VOLUME_NAME}/v1/volumes/files/spark%252F${ENGINE_ID}%252F${job_id}%252Flogs%252Fspark-driver-${job_id}-stdout \
  --header 'authorization: Bearer '"${bearer_token}"'' 2>&1); then

    http_code="${response: -3}"

    body="${response:0:${#response}-3}"

    if [[ "$http_code" == "200" ]]; then
      echo -e "$body"
    elif [[ "$http_code" == "403" ]]; then
      echo "Log retrieval failed"
      echo "Login Required - use getToken.sh"
    else
      echo "Log retrieval failed"
      echo "$body"
    fi
else
    echo "Log retrieval failed"
    echo "$body"
fi

