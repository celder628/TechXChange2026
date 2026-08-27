import requests
import urllib3
import json

def get_bearer_token_cloud(config_data: dict) -> str:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    api_key = config_data["spark"]["api_key"]
    url = "https://iam.cloud.ibm.com/identity/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    payload = {
        "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
        "apikey": api_key,
    }
    response = requests.post(url, headers=headers, data=payload, verify=False)
    return response.json()["access_token"]

def get_bearer_token_onprem(config_data: dict) -> str:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = config_data["base_url"]
    username = config_data["spark"]["username"]
    api_key = config_data["spark"]["api_key"]
    url = f"https://{base_url}/icp4d-api/v1/authorize"

    headers = {"Content-Type": "application/json"}
    data = {"username": username, "api_key": api_key}
    response = requests.post(url, headers=headers, json=data, verify=False)
    response.raise_for_status()
    return response.json()["token"]

def get_bearer_token(config_data: dict) -> str:

    if config_data["spark"]["environment"] == "onprem":
        bearer_token = get_bearer_token_onprem(config_data)
        return bearer_token
    else:
        bearer_token = get_bearer_token_cloud(config_data)
        return bearer_token

def get_request(bearer_token: str, url: str) -> str:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    return response.json()


def post_request(bearer_token: str, url: str, data: dict) -> str:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    response = requests.post(url, headers=headers, data=data, verify=False)
    response.raise_for_status()
    return response.json()

def get_catalog_id(bearer_token: str, config_data: dict) -> str:

    # Retrieve the base URL
    base_url = config_data["base_url"]

    # Create search url for the catalog name
    catalog_name = config_data["catalog"]["catalog_name"]
    url = f"https://{base_url}/v2/catalogs?name={catalog_name}"

    # Retrieve the catalog
    catalogs = get_request(bearer_token, url)

    # check if the catalog exists
    if len(catalogs["catalogs"]) == 0:
        print(f"The catalog was not found.  Double check that a Knowledge Catalog with the following name has been created: {catalog_name}")
        return None
    else:
        catalog_id = catalogs["catalogs"][0]["metadata"]["guid"]
        #print(json.dumps(catalog, indent=4))
        print(f"catalog_id: {catalog_id}")
        return catalog_id

def get_connection_id(bearer_token: str, config_data: dict, catalog_id: str) -> str:

    # Retrieve the base URL
    base_url = config_data["base_url"]

    # Create search url for the catalog name
    connection_name = config_data["catalog"]["connection_name"]
    url = f"https://{base_url}/v2/connections?catalog_id={catalog_id}&entity.name={connection_name}"

    # Retrieve the catalog
    connections = get_request(bearer_token, url)


    # check if the connection exists
    if len(connections["resources"]) == 0:
        print(f"No connections detected.  Double check that a data connection exists in the Knowledge Catalog with the name: {connection_name}")
        return None
    else:
        connection_id = connections["resources"][0]["metadata"]["asset_id"]
        print(f"connection_id: {connection_id}")
        return connection_id


def create_catalog_asset(bearer_token: str, config_data: dict, catalog_id: str, connection_id: str, 
    data_asset_name: str, data_asset_description: str, schema_name: str, table_name: str, tags: str) -> str:

    # Retrieve the base URL
    base_url = config_data["base_url"]

    # Create the URL for the create data asset
    url = f"https://{base_url}/v2/data_assets?catalog_id={catalog_id}"

    DATA_ASSET_PAYLOAD = """{
    "metadata": {
        "name": "{data_asset_name}",
        "description": "{data_asset_description}",
        "tags": [
        {tags}
        ],
        "asset_type": "data_asset",
        "origin_country": "us",
        "asset_attributes": [
            "data_asset",
            "discovered_asset"
        ],    
        "rov": {
        "mode": 0
        }
    },
    "entity": {
        "data_asset": {
        "mime_type": "application/x-ibm-rel-table",
        "dataset": true,
        "properties": [
            {
            "name": "catalog_name",
            "value": "{wxd_catalog_name}"
            },
            {
            "name": "schema_name",
            "value": "{wxd_schema_name}"
            },
            {
            "name": "table_name",
            "value": "{wxd_table_name}"
            }
        ]
        },
        "discovered_asset": {
            "extended_metadata": [
            {
                "name": "table_type",
                "value": "TABLE"
            }
        ]
        }
    },
    "attachments": [
        {
        "asset_type": "data_asset",
        "name": "{data_asset_name}",
        "description": "{data_asset_description}",
        "mime": "application/x-ibm-rel-table",
        "connection_id": "{connection_id}",
        "connection_path": "/{wxd_catalog_name}/{wxd_schema_name}/{wxd_table_name}",
        "is_partitioned": false
        }
    ]
    }"""

    # Create the parameters for the data asset
    data_asset_payload = DATA_ASSET_PAYLOAD.replace("{data_asset_name}", data_asset_name) \
        .replace("{data_asset_description}", data_asset_description) \
        .replace("{wxd_catalog_name}", config_data["medallion"]["default_catalog"]) \
        .replace("{wxd_schema_name}", schema_name) \
        .replace("{wxd_table_name}", table_name) \
        .replace("{connection_id}", connection_id) \
        .replace("{tags}", tags)

    # Create the data asset
    data_asset = post_request(bearer_token, url, data_asset_payload)

    dataset_id = data_asset["asset_id"]
    #print(json.dumps(data_asset, indent=4))
    print(f"\n\ndataset_id: {dataset_id}")

    #return dataset_id

    # Create search url for the catalog name
    profile_url = f"https://{base_url}/v2/data_profiles?start=true&use_mde=true"

    DATA_PROFILE_PAYLOAD = """{
        "metadata": {
            "dataset_id": "{dataset_id}",
            "catalog_id": "{catalog_id}"
        }
    }"""

    # Set parameters for the data profile
    data_profile_payload = DATA_PROFILE_PAYLOAD.replace("{dataset_id}", dataset_id).replace("{catalog_id}", catalog_id)

    print(data_profile_payload)

    # Create the data profile
    data_profile = post_request(bearer_token, profile_url, data_profile_payload)
    #print(json.dumps(data_profile, indent=4))

def profile_catalog_assets(bearer_token: str, config_data: dict, catalog_id: str, dataset_ids: str) -> str:

    # Retrieve the base URL
    base_url = config_data["base_url"]

    # Create search url for the catalog name
    profile_url = f"https://{base_url}/v2/data_profiles?start=true&use_mde=true"

    DATA_PROFILE_PAYLOAD = """{
        "metadata": {
            "dataset_ids": ["{dataset_ids}"],
            "catalog_id": "{catalog_id}"
        }
    }"""

    # Set parameters for the data profile
    data_profile_payload = DATA_PROFILE_PAYLOAD.replace("{dataset_ids}", dataset_ids).replace("{catalog_id}", catalog_id)

    print(data_profile_payload)

    # Create the data profile
    data_profile = post_request(bearer_token, profile_url, data_profile_payload)
    #print(json.dumps(data_profile, indent=4))