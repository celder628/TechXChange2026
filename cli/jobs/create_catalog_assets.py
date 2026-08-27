# (C) Copyright IBM Corporation 2026.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from medallion import (
    read_config
)

from catalog import (
    get_bearer_token,
    get_catalog_id,
    get_connection_id,
    create_catalog_asset,
    profile_catalog_assets
)

def main():

    try:

        config_data = read_config()

        bearer_token = get_bearer_token(config_data)

        # Retrieve the catalog ID for the medallion catalog
        catalog_id = get_catalog_id(bearer_token, config_data)

        print(catalog_id)

        # Retrieve the connection ID for the medallion catalog data connection
        connection_id = get_connection_id(bearer_token, config_data, catalog_id)

        print(connection_id)

        dataset_list = []

        # Create the bronze level assets
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "complaints-bronze", "Bronze level complaints", config_data["medallion"]["bronze_schema"], "complaints", "\"public\",\"bronze\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "company-bronze", "Bronze level company", config_data["medallion"]["bronze_schema"], "company", "\"public\",\"bronze\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "summary-by-state-bronze", "Bronze level summary_by_year", config_data["medallion"]["bronze_schema"], "summary_by_state", "\"public\",\"bronze\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "summary-by-year-bronze", "Bronze level summary_by_year", config_data["medallion"]["bronze_schema"], "summary_by_year", "\"public\",\"bronze\"")

        # Create the silver level assets
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "complaints-silver", "Silver level complaints", config_data["medallion"]["silver_schema"], "complaints", "\"public\",\"silver\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "company-silver", "Silver level company", config_data["medallion"]["silver_schema"], "company", "\"public\",\"silver\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "summary-by-state-silver", "Silver level summary_by_year", config_data["medallion"]["silver_schema"], "summary_by_state", "\"public\",\"silver\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "summary-by-year-silver", "Silver level summary_by_year", config_data["medallion"]["silver_schema"], "summary_by_year", "\"public\",\"silver\"")

        # Create the gold level assets
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "complaints-gold", "Gold level complaints", config_data["medallion"]["gold_schema"], "complaints", "\"public\",\"gold\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "company-gold", "Gold level company", config_data["medallion"]["gold_schema"], "company", "\"public\",\"gold\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "summary-by-state-gold", "Gold level summary_by_year", config_data["medallion"]["gold_schema"], "summary_by_state", "\"public\",\"gold\"")
        create_catalog_asset(bearer_token, config_data, catalog_id, connection_id, "summary-by-year-gold", "Gold level summary_by_year", config_data["medallion"]["gold_schema"], "summary_by_year", "\"public\",\"gold\"")

        # result_comma = ", ".join(dataset_list)
        # print(result_comma)

        # profile_catalog_assets(bearer_token, config_data, catalog_id, result_comma)

    finally:
        print("completed")

if __name__ == "__main__":
    main()

